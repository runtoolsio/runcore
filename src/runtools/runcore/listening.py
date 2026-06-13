"""Domain notification dispatch — transport-neutral.

:class:`InstanceEventRouter` decodes raw wire event payloads (``event_type``, instance
metadata, event dict) and routes them to per-type observer channels. It is registered as
a handler on a transport-specific event receiver (e.g. ``UnixSocketEventReceiver`` in
``runtools.runcore.transport.unix_socket``); observers register on the router itself —
it is the env-wide typed event hub.
"""

import logging
from threading import Lock

from runtools.runcore.job import (
    InstanceControlEvent, InstanceLifecycleEvent, InstanceObservableNotifications,
    InstanceOutputEvent, InstancePhaseEvent, InstanceStatusEvent,
)

log = logging.getLogger(__name__)


class InstanceEventRouter(InstanceObservableNotifications):
    """Route raw instance event payloads to typed notification channels.

    Transports call this object with ``(event_type, instance_metadata, event_dict)``.
    The router deserializes ``event_dict`` into the matching event class and emits
    it through the lifecycle, phase, output, control, or status notification.

    Constructed with ``buffering=True``, the router holds incoming events until
    :meth:`flush` drains them in receive order and switches to live dispatch.
    This lets a consumer start receiving from the wire before its observers exist
    without losing or reordering startup events.
    """

    def __init__(self, *, start_buffering: bool = False, **kwargs):
        super().__init__(**kwargs)
        self._buffer = [] if start_buffering else None
        self._buffer_lock = Lock()

    def __call__(self, event_type, instance_metadata, event):
        with self._buffer_lock:
            if self._buffer is not None:
                self._buffer.append((event_type, instance_metadata, event))
                return
        self._route(event_type, instance_metadata, event)

    def flush_buffer(self):
        """Drain buffered events in receive order and switch to live dispatch.

        Unlike a stream flush this is one-way: after the first call the router
        dispatches live and never buffers again. Idempotent; a no-op for a router
        constructed without buffering. Dispatching under the buffer lock keeps
        drained and live events ordered: the receiving thread blocks until the
        backlog has been delivered.
        """
        with self._buffer_lock:
            buffered, self._buffer = self._buffer or [], None
            for event_args in buffered:
                self._route(*event_args)

    def _route(self, event_type, instance_metadata, event):
        if event_type == InstanceLifecycleEvent.EVENT_TYPE:
            self.lifecycle_notification.observer_proxy.instance_lifecycle_update(InstanceLifecycleEvent.deserialize(event))
        elif event_type == InstancePhaseEvent.EVENT_TYPE:
            self.phase_notification.observer_proxy.instance_phase_update(
                InstancePhaseEvent.deserialize(event))
        elif event_type == InstanceOutputEvent.EVENT_TYPE:
            self.output_notification.observer_proxy.instance_output_update(InstanceOutputEvent.deserialize(event))
        elif event_type == InstanceControlEvent.EVENT_TYPE:
            self.control_notification.observer_proxy.instance_control_update(InstanceControlEvent.deserialize(event))
        elif event_type == InstanceStatusEvent.EVENT_TYPE:
            self.status_notification.observer_proxy.instance_status_update(InstanceStatusEvent.deserialize(event))
        else:
            log.warning("Unknown event type=%s", event_type, extra={"type": event_type, "instance": str(instance_metadata)})
