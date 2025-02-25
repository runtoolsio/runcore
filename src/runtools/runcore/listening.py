#  Sender, Listening
import json
import logging
from abc import abstractmethod
from json import JSONDecodeError

from runtools.runcore import util, paths
from runtools.runcore.job import InstanceTransitionObserver, InstanceOutputObserver, JobInstanceMetadata, \
    InstanceTransitionEvent, InstanceOutputEvent
from runtools.runcore.util.observer import ObservableNotification
from runtools.runcore.util.socket import SocketServer

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

    instance_metadata = req_body_json.get('instance_metadata')
    if not instance_metadata:
        raise ValueError(_missing_field_txt('root', 'instance_metadata'))

    return event_type, JobInstanceMetadata.deserialize(instance_metadata)


class EventReceiver(SocketServer):

    def __init__(self, socket_name, instance_match=None, event_types=()):
        super().__init__(lambda: paths.socket_path(socket_name, create=True), allow_ping=True)
        self.instance_match = instance_match
        self.event_types = event_types

    def handle(self, req_body):
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

        if (self.event_types and event_type not in self.event_types) or \
                (self.instance_match and not self.instance_match(instance_meta)):
            return

        self.handle_event(event_type, instance_meta, req_body_json.get('event'))

    @abstractmethod
    def handle_event(self, event_type, instance_meta, event):
        pass


class InstanceTransitionReceiver(EventReceiver):

    def __init__(self, instance_match=None, phases=(), stages=()):
        super().__init__(_listener_socket_name(TRANSITION_LISTENER_FILE_EXTENSION), instance_match)
        self.phases = phases
        self.stages = stages
        self._notification = ObservableNotification[InstanceTransitionObserver]()

    def handle_event(self, _, instance_meta, event):
        e = InstanceTransitionEvent.deserialize(event)

        if self.phases and e.phase_id not in self.phases:
            return

        if self.stages and e.new_stage not in self.stages:
            return

        self._notification.observer_proxy.new_instance_transition(e)

    def add_observer_transition(self, observer):
        self._notification.add_observer(observer)

    def remove_observer_transition(self, observer):
        self._notification.remove_observer(observer)


class InstanceOutputReceiver(EventReceiver):

    def __init__(self, instance_match=None):
        super().__init__(_listener_socket_name(OUTPUT_LISTENER_FILE_EXTENSION), instance_match)
        self._notification = ObservableNotification[InstanceOutputObserver]()

    def handle_event(self, _, instance_meta, event):
        self._notification.observer_proxy.new_instance_output(InstanceOutputEvent.deserialize(event))

    def add_observer_output(self, observer):
        self._notification.add_observer(observer)

    def remove_observer_output(self, observer):
        self._notification.remove_observer(observer)
