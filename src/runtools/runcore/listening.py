"""Domain notification dispatch — transport-neutral.

:class:`InstanceEventReceiver` adapts raw event payloads (``event_type``, instance metadata,
event dict) into typed runtools notifications. It is registered as a handler on a
transport-specific event receiver (e.g. ``UnixSocketEventReceiver`` in
``runtools.runcore.transport.unix_socket``) and dispatches incoming events to per-type
observer notifications.
"""

import logging

from runtools.runcore.job import (
    InstanceControlEvent, InstanceLifecycleEvent, InstanceObservableNotifications,
    InstanceOutputEvent, InstancePhaseEvent, InstanceStatusEvent,
)

log = logging.getLogger(__name__)


class InstanceEventReceiver(InstanceObservableNotifications):

    def __call__(self, event_type, instance_metadata, event):
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
