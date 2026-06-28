"""Transport runtime interfaces for runtools environments.

A transport defines how live job instances are found, observed, and controlled.
Each transport module provides the concrete discovery, directory, proxies, and
layout needed for its communication mechanism.

Main interfaces:

- :class:`InstanceDiscovery` finds the currently active runs using whatever
  mechanism the transport chooses, such as socket broadcast, SQL, or presence keys.
- :class:`InstanceDirectory` exposes live job instances as stable proxy objects and
  publishes their events.
- :class:`InstanceDirectoryBase` implements the shared directory core — identity map,
  admission, and notifications. :class:`EventDrivenInstanceDirectoryBase` adds startup
  driven by an external event stream; concrete transports provide proxy construction,
  event receiving, and cleanup.
"""

import logging
from abc import ABC, abstractmethod
from collections import OrderedDict
from dataclasses import dataclass
from threading import Lock
from typing import Dict, List, NamedTuple, Optional, Protocol, runtime_checkable, Set

from runtools.runcore.job import (
    InstanceID, InstanceNotifications, JobInstance, JobRun,
    NotificationBinding,
)
from runtools.runcore.listening import InstanceEventRouter
from runtools.runcore.proxy import JobInstanceProxyBase

log = logging.getLogger(__name__)

_ENDED_TOMBSTONES_MAX = 1000


@dataclass(frozen=True)
class DiscoveredRuns:
    """Active runs of an environment, as enumerated by one discovery sweep.

    ``complete`` is a property of the enumeration: whether it covers every source
    that could currently have active runs. Instances missing from an incomplete
    enumeration must not be treated as stopped or unreachable; they may simply
    belong to a source that failed to answer.
    """
    runs: List[JobRun]
    complete: bool = True

    def __iter__(self):
        return iter(self.runs)

    def __bool__(self):
        return bool(self.runs)


@runtime_checkable
class InstanceDiscovery(Protocol):
    """Find active runs for an environment.

    Implementations may use different mechanisms: socket broadcast, a database
    query, Redis presence keys, or an in-memory registry. Results are snapshots and
    may already be stale by the time the caller receives them.
    """

    def discover_active_runs(self, run_match=None) -> DiscoveredRuns: ...


@runtime_checkable
class InstanceDirectory(Protocol):
    """Directory of live job instances in an environment.

    The directory discovers active instances, listens for instance events, and
    returns stable proxy objects for live instances. Repeated lookups for the same
    instance return the same proxy while the instance remains known. Closing the
    directory stops event delivery and releases the resources used by its proxies.
    """

    @property
    def notifications(self) -> InstanceNotifications:
        """Typed event stream for all instances observed by this directory."""

    def get_instances(self, run_match=None) -> List[JobInstance]: ...

    def get_instance(self, instance_id: InstanceID) -> Optional[JobInstance]: ...

    def open(self) -> None: ...

    def close(self) -> None: ...


class _AdmittedInstance(NamedTuple):
    """The directory's unit of membership: a proxy plus its event subscription."""
    proxy: JobInstanceProxyBase
    binding: NotificationBinding


class InstanceDirectoryBase(ABC):
    """Transport- and input-neutral core of an :class:`InstanceDirectory` implementation.

    Owns the live proxy registry: one proxy per live ``InstanceID`` plus tombstones for recently
    removed instances. Admission creates and wires a proxy; removal unbinds it and records a
    tombstone so stale snapshots cannot re-admit it.

    Reads are local and never touch the transport. A missing proxy means the instance is not live in
    this directory's current view.

    Subclasses provide the input strategy (event stream or polling), notification aggregate, proxy
    creation, proxy wiring, and resource cleanup.
    """

    def __init__(self):
        self._admitted: Dict[InstanceID, _AdmittedInstance] = {}
        self._ended_tombstones: OrderedDict[InstanceID, bool] = OrderedDict()  # Recently ended instance IDs; older snapshots for these IDs must not recreate proxies
        self._proxies_lock = Lock()

    @property
    @abstractmethod
    def notifications(self) -> InstanceNotifications:
        """The all-instance event stream consumers subscribe to (the directory's aggregate)."""

    @abstractmethod
    def open(self) -> None:
        """Seed the view and start keeping it current — subclass owns the input strategy."""

    def get_instances(self, run_match=None) -> List[JobInstance]:
        """The current live view, filtered by the proxies' state.

        Never queries the transport; eventually consistent per the transport doc.
        """
        with self._proxies_lock:
            proxies = [admitted.proxy for admitted in self._admitted.values()]
        if run_match:
            return [proxy for proxy in proxies if run_match(proxy.snap())]
        return proxies

    def get_instance(self, instance_id: InstanceID) -> Optional[JobInstance]:
        with self._proxies_lock:
            admitted = self._admitted.get(instance_id)
        return admitted.proxy if admitted else None

    def close(self) -> None:
        self._close_resources()

    def _admit(self, job_run: JobRun):
        """Ensure the instance is in the view — pure membership, get-or-create.

        Applying state is not this method's concern: a new proxy carries the admitting
        snapshot, and later updates arrive through the subclass's input (an event stream
        or polling).
        """
        instance_id = job_run.instance_id
        with self._proxies_lock:
            if job_run.lifecycle.is_ended or instance_id in self._ended_tombstones:
                self._ensure_removed(instance_id)
                return
            if instance_id in self._admitted:
                return

        new_proxy = self._create_proxy(job_run)
        with self._proxies_lock:
            if instance_id not in self._admitted and instance_id not in self._ended_tombstones:
                self._admitted[instance_id] = _AdmittedInstance(new_proxy, self._wire_proxy(new_proxy))

    def _ensure_removed(self, instance_id: InstanceID):
        """Drop the instance from the view, unbind its membership binding, and tombstone it
        against stale re-admission.

        Caller must hold ``_proxies_lock``; binding removal is lock-free, so
        unbinding under the lock is safe.
        """
        admitted = self._admitted.pop(instance_id, None)
        if admitted is not None:
            admitted.binding.unbind()
        self._ended_tombstones[instance_id] = True
        while len(self._ended_tombstones) > _ENDED_TOMBSTONES_MAX:
            self._ended_tombstones.popitem(last=False)

    @abstractmethod
    def _wire_proxy(self, proxy: JobInstanceProxyBase) -> NotificationBinding:
        """Connect a newly admitted ``proxy`` to the directory's notification topology.

        Returns the binding the directory owns and unbinds when the instance leaves the view. The
        direction is transport-specific: an event-driven directory subscribes the proxy to its event
        router (proxy <- router); a snapshot directory subscribes its aggregate to the proxy
        (aggregate <- proxy), since there the proxy is the event source.
        """

    @abstractmethod
    def _create_proxy(self, initial: JobRun) -> JobInstanceProxyBase:
        """Create the proxy object for a newly discovered instance.

        May be called redundantly under admission races; a proxy that is not
        admitted is simply discarded — it is inert until the directory binds it.
        """

    @abstractmethod
    def _close_resources(self) -> None:
        """Release resources owned by this directory."""


class EventDrivenInstanceDirectoryBase(InstanceDirectoryBase):
    """Directory whose view is seeded by discovery and kept current by an event stream.

    Startup follows a fixed order: start receiving into the buffering event router, discover
    active instances, create proxies, then flush the router — buffered events drain in receive
    order and dispatch switches live. This prevents startup events from being lost before their
    proxies exist. The flush is not a caller-visible barrier: proxies are available the moment
    discovery admits them and may briefly hold discovery-stale state — buffered and live events
    refresh them under the staleness guard.

    Liveness gap, deliberate: an instance whose node dies without emitting ``ENDED`` stays in
    the view. Dead-instance eviction arrives with the periodic reconciler (transport doc,
    design point 4) rather than being bolted onto the read path.

    Subclasses provide event receiving on top of the core's proxy creation and cleanup.
    """

    def __init__(self, discovery: InstanceDiscovery):
        super().__init__()
        self._discovery = discovery
        self._event_router = InstanceEventRouter(start_buffering=True)
        self._event_router.add_observer_lifecycle(self._on_state_event)
        self._event_router.add_observer_phase(self._on_state_event)
        self._event_router.add_observer_status(self._on_state_event)

    @property
    def notifications(self) -> InstanceNotifications:
        return self._event_router  # the router is this transport's input and aggregate stream

    def open(self) -> None:
        self._start_receiving(self._event_router)
        for job_run in self._discovery.discover_active_runs():
            self._admit(job_run)
        self._event_router.flush_buffer()

    def _wire_proxy(self, proxy: JobInstanceProxyBase) -> NotificationBinding:
        return proxy.bind_to(self._event_router)  # proxy <- router: the router feeds the proxy

    def _on_state_event(self, event):
        self._admit(event.job_run)

    @abstractmethod
    def _start_receiving(self, handler) -> None:
        """Start delivering raw events ``(event_type, instance_metadata, event_dict)`` to handler."""
