import bisect
import datetime
from abc import abstractmethod
from threading import Lock
from typing import Callable, List, Optional, Tuple, override

from runtools.runcore.job import (
    JobRun, JobInstance, InstanceNotifications, InstanceObservableNotifications,
    InstanceControlEvent, InstanceLifecycleEvent, InstanceOutputEvent, InstancePhaseEvent, InstanceStatusEvent,
    NotificationBinding, OutputTailReader, SignalSender, STOP_OP,
)
from runtools.runcore.matching import MetadataCriterion
from runtools.runcore.output import Output, OutputLine
from runtools.runcore.run import Stage, StopReason


class JobInstanceProxyBase(JobInstance):
    """Base class for job instance proxies.

    A proxy represents an instance whose state and commands are mediated by a
    transport implementation. It keeps a local :class:`JobRun` snapshot and
    updates that snapshot from state events. Snapshot reads are local; commands
    and output reads are delegated to the concrete transport.

    State updates are monotonic: older snapshots are ignored according to
    :meth:`JobRun.is_newer_than`.

    Subclasses implement command delivery and remote output fetches.
    """

    def __init__(self, initial: JobRun, output_buffer_depth: int = 100):
        self._job_run = initial  # Replacement-only, never mutated in place => lock-free reads
        self._output = _ProxyOutput(self._fetch_output_tail, output_buffer_depth)
        self._notifications = InstanceObservableNotifications(
            instance_filter=(MetadataCriterion.exact_match(initial.instance_id)))
        self._notifications.add_observer_lifecycle(self._on_lifecycle_update)
        self._notifications.add_observer_phase(self._on_phase_update)
        self._notifications.add_observer_status(self._on_status_update)
        self._notifications.add_observer_output(self._on_output_update)

    @property
    def metadata(self):
        return self._job_run.metadata

    def _update_state(self, job_run: JobRun):
        """Apply a possibly-newer snapshot of this instance.

        Guarded by :meth:`JobRun.is_newer_than` — older snapshots (stale discovery
        results, replayed events) never regress the cached state.
        """
        if job_run.is_newer_than(self._job_run):
            self._job_run = job_run

    def bind_to(self, source: InstanceNotifications) -> NotificationBinding:
        """Subscribe this proxy to an upstream instance event source.

        The proxy is inert until bound — construction has no external effects.
        The caller (the directory) owns the returned binding and unbinds it when
        the instance leaves the view.
        """
        return self._notifications.bind_to(source)

    def _on_lifecycle_update(self, event: InstanceLifecycleEvent):
        self._update_state(event.job_run)

    def _on_phase_update(self, event: InstancePhaseEvent):
        self._update_state(event.job_run)

    def _on_status_update(self, event: InstanceStatusEvent):
        self._update_state(event.job_run)

    def _on_output_update(self, event: InstanceOutputEvent):
        self._output.add_line(event.output_line)

    def snap(self) -> JobRun:
        return self._job_run

    def find_phase_control(self, phase_filter):
        phase = self._job_run.find_first_phase(phase_filter)
        if not phase:
            return None
        return PhaseControlProxy(self._exec_phase_op, phase.phase_id)

    @abstractmethod
    def _exec_phase_op(self, phase_id: str, op_name: str, *op_args):
        """Execute an operation on a phase of the remote instance."""

    @abstractmethod
    def _fetch_output_tail(self, max_lines: int) -> List[OutputLine]:
        """Fetch lines from the remote instance's tail buffer."""

    @property
    def output(self) -> Output:
        return self._output

    @abstractmethod
    def stop(self, stop_reason= StopReason.STOPPED):
        pass

    def run(self):
        raise NotImplementedError("Remote run is not supported")

    @property
    @override
    def notifications(self) -> InstanceNotifications:
        return self._notifications


class _ProxyOutput(Output):
    """Bounded buffer of output events received since proxy construction.

    Tail reads covered by the buffer are served locally. Reads the buffer cannot
    prove complete are served directly from the remote instance; fetched lines are
    intentionally not merged into the event-fed buffer.

    The buffer is kept sorted by line ordinal and drops duplicate deliveries, so
    transport delivery order is not load-bearing — the output analog of the
    :meth:`JobRun.is_newer_than` guard on state.
    """

    def __init__(self, fetch_tail: Callable[[int], List[OutputLine]], max_lines: int):
        self._fetch_tail = fetch_tail
        self._max_lines = max_lines
        self._lines: List[OutputLine] = []
        self._lock = Lock()

    @property
    def locations(self):
        return ()

    def add_line(self, line: OutputLine):
        with self._lock:
            if not self._lines or line.ordinal > self._lines[-1].ordinal:
                self._lines.append(line)  # Fast path: in-order delivery
            else:
                index = bisect.bisect_left(self._lines, line.ordinal, key=lambda l: l.ordinal)
                if index < len(self._lines) and self._lines[index].ordinal == line.ordinal:
                    return  # Duplicate delivery
                self._lines.insert(index, line)
            del self._lines[:-self._max_lines]

    def tail(self, max_lines: int = 0):
        with self._lock:
            lines = list(self._lines)
        if 0 < max_lines <= len(lines):
            return lines[-max_lines:]
        return self._fetch_tail(max_lines)


class PhaseControlProxy:
    """Proxy for controlling a phase in another process.

    Delegates operations to the owning instance proxy's transport-specific
    ``_exec_phase_op``.
    """

    def __init__(self, exec_phase_op: Callable, phase_id: str):
        self._exec_phase_op = exec_phase_op
        self._phase_id = phase_id

    def exec_op(self, op_name: str, *op_args):
        """Execute an operation on the phase.

        Args:
            op_name: Name of the operation to execute
            op_args: Arguments to pass to the operation

        Returns:
            Operation-specific return value

        Raises:
            PhaseNotFoundError: If the phase doesn't exist on the server
            TargetNotFoundError: If the instance or server is not found
            InstanceCallServerError: For server-side errors during execution
            InstanceCallClientError: For client-side errors during execution
        """
        return self._exec_phase_op(self._phase_id, op_name, *op_args)

    def __getattr__(self, name):
        """Dynamic method resolution to enable natural operation calling.

        This allows calling operations directly as methods on the phase control object.
        For example: phase_control.pause() instead of phase_control.exec_op('pause')

        Args:
            name: Name of the operation/attribute to access

        Returns:
            A callable function that delegates to exec_op if name doesn't exist as an attribute
        """
        # Only intercept methods that don't exist as actual attributes
        if name.startswith('_'):
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

        def method_proxy(*args):
            return self.exec_op(name, *args)

        return method_proxy


_InstanceStateEvent = InstanceLifecycleEvent | InstancePhaseEvent | InstanceStatusEvent | InstanceControlEvent
"""A state-carrying instance event — lifecycle, phase, or status (the events a directory routes
through ``_on_state_event``). Excludes output and control, which carry no run snapshot."""

_STAGE_PROGRESSION = (Stage.CREATED, Stage.RUNNING, Stage.ENDED)


def _synthesize_events(prev: Optional[JobRun], curr: JobRun) -> List[_InstanceStateEvent]:
    """Reconstruct the instance events that took ``prev`` to ``curr``, ordered by timestamp.

    A snapshot transport learns about state changes by re-reading run snapshots, not by receiving
    the events a producing node emits; this rebuilds those events from a snapshot pair so
    :class:`SnapshotJobInstanceProxy` can drive the same notifications an event-driven proxy would.
    The producer's rules are mirrored exactly (see ``runjob.instance``): a root-phase transition
    yields a lifecycle event, every RUNNING/ENDED phase transition yields a phase event, and a
    changed status yields a status event. CREATED is a phase's initial state, not a transition the
    producer emits — so a CREATED root yields only its lifecycle event (the instance emits that at
    birth) and a CREATED child yields nothing.

    Every per-stage timestamp is stored on the snapshot (:meth:`RunLifecycle.transition_at`), so
    transitions that a single poll interval would coalesce — e.g. ``CREATED -> RUNNING -> ENDED`` —
    are replayed individually with their real timestamps; only status is coalesced to its latest
    value.

    Args:
        prev: The last seen snapshot of the instance, or None for a first sighting — a first
            sighting reports each phase's net (current) stage rather than replaying its history.
        curr: The newer snapshot to diff against ``prev``.

    Returns:
        Lifecycle, phase, and status events carrying ``curr``, ordered by transition timestamp.
    """
    events: List[_InstanceStateEvent] = []
    root_id = curr.root_phase.phase_id
    prev_stages = {p.phase_id: p.lifecycle.stage for p in prev.search_phases()} if prev else {}

    for phase in curr.search_phases():
        is_root = phase.phase_id == root_id
        for stage in _stages_reached(prev_stages.get(phase.phase_id), phase.lifecycle.stage):
            timestamp = phase.lifecycle.transition_at(stage)
            if timestamp is None:
                continue  # stage skipped, e.g. stopped before start jumps CREATED -> ENDED, leaving RUNNING unset
            if is_root:
                events.append(InstanceLifecycleEvent(curr, stage, timestamp))
            if stage != Stage.CREATED:  # the producer emits no CREATED phase transition — it's the initial state
                events.append(InstancePhaseEvent(curr, is_root, phase.phase_id, stage, timestamp))

    if curr.status and (prev is None or prev.status != curr.status):
        timestamp = _status_timestamp(curr.status) or curr.lifecycle.last_transition_at
        events.append(InstanceStatusEvent(curr, timestamp))

    # control_requests is append-only (single writer), so the new entries are the suffix
    prev_request_count = len(prev.control_requests) if prev else 0
    for request in curr.control_requests[prev_request_count:]:
        events.append(InstanceControlEvent(curr, request, request.applied_at))

    events.sort(key=lambda event: event.timestamp)
    return events


def _stages_reached(prev_stage: Optional[Stage], curr_stage: Stage) -> Tuple[Stage, ...]:
    """Stages newly reached between ``prev_stage`` (exclusive) and ``curr_stage`` (inclusive).

    An unseen phase (``prev_stage`` is None) reports only its current stage — first sightings
    carry net state, not a replayed ``CREATED -> ...`` history. A phase whose stage did not
    advance yields nothing.

    Returns the linear-progression candidates; a phase may skip one (e.g. stopped before start
    jumps ``CREATED -> ENDED``), so the caller drops any stage whose ``transition_at`` is None.
    """
    if prev_stage is None:
        return (curr_stage,)
    lo = _STAGE_PROGRESSION.index(prev_stage)
    hi = _STAGE_PROGRESSION.index(curr_stage)
    return _STAGE_PROGRESSION[lo + 1:hi + 1]


def _status_timestamp(status) -> Optional[datetime.datetime]:
    """Latest timestamp carried by the status, mirroring how ``JobRun.last_updated`` reads it."""
    timestamps = []
    if status.last_event:
        timestamps.append(status.last_event.timestamp)
    if status.result:
        timestamps.append(status.result.timestamp)
    timestamps += [warning.timestamp for warning in status.warnings]
    timestamps += [operation.updated_at for operation in status.operations]
    return max(timestamps) if timestamps else None


class SnapshotJobInstanceProxy(JobInstanceProxyBase):
    """Job instance proxy updated by whole snapshots rather than an event stream.

    A snapshot transport (polling a database, presence keys, etc.) pushes fresh snapshots via
    :meth:`update_from_snapshot` instead of binding the proxy to an upstream event source. The proxy
    applies the newer-wins guard, replaces the cached snapshot, and emits the synthesized state
    events to its own notification hub (where a directory's aggregate fans in). Control is posted
    through the signal sender; output tail reads come from the shared bounded tail
    (design point 7) — the newest retained lines, not the complete output.

    The directory also pushes liveness (:meth:`update_liveness`) — a snapshot transport cannot tell
    a quiet run from a dead producer, so the heartbeat verdict is part of the proxy's view.
    """

    def __init__(self, initial: JobRun, signal_sender: SignalSender, output_tail: OutputTailReader,
                 output_buffer_depth: int = 100):
        super().__init__(initial, output_buffer_depth)
        self._signals = signal_sender
        self._output_tail = output_tail
        self._heartbeat_age: Optional[float] = None
        self._lost = False

    @property
    def heartbeat_age(self) -> Optional[float]:
        """Seconds since the producing node last attested this run's liveness; None when unknown."""
        return self._heartbeat_age

    @property
    def is_lost(self) -> bool:
        """True when the heartbeat is stale — the producing node likely died; the state may be frozen."""
        return self._lost

    def update_liveness(self, heartbeat_age: Optional[float], lost: bool) -> None:
        self._heartbeat_age = heartbeat_age
        self._lost = lost

    def update_from_snapshot(self, curr: JobRun) -> None:
        """Apply a freshly read snapshot and emit the state events it implies.

        Ignores a snapshot that is not newer than the cached one (which also stops a stale active
        snapshot from resurrecting an ended instance). The cache is replaced before emitting so an
        observer that reads :meth:`snap` within its callback sees the new state.
        """
        prev = self._job_run
        if not curr.is_newer_than(prev):
            return
        events = _synthesize_events(prev, curr)
        self._job_run = curr
        for event in events:
            self._emit(event)

    def _emit(self, event: _InstanceStateEvent) -> None:
        """Fan a synthesized event out through the hub to downstream and aggregate observers.

        The proxy's own ``_on_*_update`` also fires but is a no-op here — the cache already holds the
        snapshot the event carries.
        """
        if isinstance(event, InstanceLifecycleEvent):
            self._notifications.lifecycle_notification.observer_proxy.instance_lifecycle_update(event)
        elif isinstance(event, InstancePhaseEvent):
            self._notifications.phase_notification.observer_proxy.instance_phase_update(event)
        elif isinstance(event, InstanceControlEvent):
            self._notifications.control_notification.observer_proxy.instance_control_update(event)
        else:
            self._notifications.status_notification.observer_proxy.instance_status_update(event)

    def stop(self, stop_reason=StopReason.STOPPED):
        """Post a stop signal for the owning node to apply (design point 5) — fire-and-forget;
        the outcome is observed through the ordinary state lane."""
        self._signals.send_signal(self._job_run.instance_id, STOP_OP, args=(stop_reason.name,))

    def _exec_phase_op(self, phase_id: str, op_name: str, *op_args):
        self._signals.send_signal(self._job_run.instance_id, op_name, phase_id=phase_id, args=op_args)

    def _fetch_output_tail(self, max_lines: int) -> List[OutputLine]:
        return self._output_tail.read_output_tail(self._job_run.instance_id, max_lines)
