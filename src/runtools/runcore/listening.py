#  Sender, Listening
import json
import logging
from json import JSONDecodeError

from runtools.runcore import util
from runtools.runcore.job import JobInstanceMetadata, \
    InstanceTransitionEvent, InstanceOutputEvent, InstanceObservableNotifications, InstanceLifecycleEvent
from runtools.runcore.util.socket import DatagramSocketServer

log = logging.getLogger(__name__)

TRANSITION_LISTENER_FILE_EXTENSION = '.tlistener'
OUTPUT_LISTENER_FILE_EXTENSION = '.olistener'


def _listener_socket_name(ext):
    return util.unique_timestamp_hex() + ext


def _missing_field_txt(obj, missing):
    return f"event=[invalid_event] object=[{obj}] reason=[missing field: {missing}]"


def _read_metadata(req_body_json):
    event_metadata = req_body_json.get('event_metadata')
    if not event_metadata:
        raise ValueError(_missing_field_txt('root', 'event_metadata'))

    event_type = event_metadata.get('event_type')
    if not event_type:
        raise ValueError(_missing_field_txt('event_metadata', 'event_type'))

    instance_metadata = event_metadata.get('instance')
    if not instance_metadata:
        raise ValueError(_missing_field_txt('event_metadata', 'instance'))

    return event_type, JobInstanceMetadata.deserialize(instance_metadata)


class EventReceiver(DatagramSocketServer):
    """
    Generic event receiver that uses composition to handle different event types.
    """

    def __init__(self, socket_path, instance_match=None):
        """
        Initialize the event receiver.

        Args:
            socket_path: Path to the socket file
            instance_match: Optional filter for instances
        """
        super().__init__(socket_path, allow_ping=True)
        self.instance_match = instance_match
        self._event_handlers = {}
        self._default_handlers = []

    def register_handler(self, handler, *event_types: str):
        """
        Register a handler for a specific event type.

        Args:
            event_types: Event type identifier
            handler: Handler function to process events of this type
        """
        if event_types:
            for event_type in event_types:
                self._event_handlers[event_type] = handler
        else:
            self._default_handlers.append(handler)

        return self

    def handle(self, req_body):
        """Process received event data."""
        try:
            req_body_json = json.loads(req_body)
        except JSONDecodeError:
            log.warning(f"event=[invalid_json_event_received] length=[{len(req_body)}]")
            return

        try:
            event_type, instance_meta = _read_metadata(req_body_json)
        except ValueError as e:
            log.warning(e)
            return

        # Check if this event type should be handled by this receiver
        if self.instance_match and not self.instance_match(instance_meta):
            return

        handlers = list(self._default_handlers)
        if event_type in self._event_handlers:
            handlers.append(self._event_handlers[event_type])
        for handler in handlers:
            try:
                handler(event_type, instance_meta, req_body_json.get("event"))
            except Exception:
                log.exception("event=[event_handler_failed] event_type=[%s] instance=[%s]", event_type, instance_meta)


class InstanceEventReceiver(InstanceObservableNotifications):

    def __call__(self, event_type, instance_metadata, event):
        if event_type == InstanceLifecycleEvent.EVENT_TYPE:
            self.lifecycle_notification.observer_proxy.instance_lifecycle_update(InstanceLifecycleEvent.deserialize(event))
        elif event_type == InstanceTransitionEvent.EVENT_TYPE:
            self.transition_notification.observer_proxy.instance_transition_update(
                InstanceTransitionEvent.deserialize(event))
        elif event_type == InstanceOutputEvent.EVENT_TYPE:
            self.output_notification.observer_proxy.instance_output_update(InstanceOutputEvent.deserialize(event))
        else:
            log.warning(f"[unknown_event_type] event_type=[{event_type}] instance=[{instance_metadata}]")
