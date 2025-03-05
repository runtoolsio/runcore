import logging
import shutil
from abc import ABC, abstractmethod
from threading import Event

from runtools.runcore import RemoteCallClient, InstanceTransitionReceiver, paths, util
from runtools.runcore.criteria import JobRunCriteria
from runtools.runcore.db import SortCriteria, sqlite
from runtools.runcore.job import JobInstanceObservable
from runtools.runcore.listening import InstanceOutputReceiver
from runtools.runcore.remote import JobInstanceRemote
from runtools.runcore.util.err import run_isolated_collect_exceptions
from runtools.runcore.util.observer import DEFAULT_OBSERVER_PRIORITY

log = logging.getLogger(__name__)

DEF_ENV_ID = 'default'


def wait_for_interrupt(env, *, reraise=True):
    try:
        Event().wait()
    except KeyboardInterrupt:
        env.close()
    finally:
        if reraise:
            raise KeyboardInterrupt


class LocalConnectorLayout(ABC):

    @property
    @abstractmethod
    def env_dir(self):
        pass

    @property
    @abstractmethod
    def socket_client_rpc(self):
        pass

    @property
    @abstractmethod
    def socket_listener_events(self):
        pass

    @property
    @abstractmethod
    def provider_sockets_server_rpc(self):
        pass

    @abstractmethod
    def cleanup(self):
        pass


class StandardLocalConnectorLayout(LocalConnectorLayout):
    NODE_DIR_PREFIX = "node_"
    CONNECTOR_DIR_PREFIX = "connector_"

    @classmethod
    def create(cls, env_id, root_dir=None):
        return cls(*create_layout_dirs(env_id, root_dir, cls.CONNECTOR_DIR_PREFIX))

    def __init__(self, env_dir, connector_dir):
        self._env_dir = env_dir
        self.component_dir = connector_dir

    @property
    def env_dir(self):
        return self._env_dir

    @property
    def socket_name_client_rpc(self):
        return 'client-rpc.sock'

    @property
    def socket_client_rpc(self):
        return self.component_dir / self.socket_name_client_rpc

    @property
    def socket_name_listener_events(self):
        return 'listener-events.sock'

    @property
    def socket_listener_events(self):
        return self.component_dir / self.socket_name_listener_events

    @property
    def socket_name_server_rpc(self):
        return 'server-rpc.sock'

    @property
    def provider_sockets_server_rpc(self):
        return paths.files_in_subdir_provider(self.env_dir, self.socket_name_server_rpc,
                                              pattern=f"^{self.NODE_DIR_PREFIX}")

    def cleanup(self):
        shutil.rmtree(self.component_dir)


def create_layout_dirs(env_id, root_dir, component_prefix):
    if root_dir:
        env_dir = root_dir / env_id
    else:
        env_dir = paths.ensure_dirs(paths.runtime_env_dir() / env_id)

    comp_id = component_prefix + util.unique_timestamp_hex()
    component_dir = paths.ensure_dirs(env_dir / comp_id)

    return env_dir, component_dir


class EnvironmentConnector(ABC):

    def __enter__(self):
        """
        Open the environment node.
        """
        self.open()
        return self

    @abstractmethod
    def open(self):
        pass

    @abstractmethod
    def get_active_runs(self, run_match=None):
        pass

    def get_instance(self, instance_id):
        """TODO Abstract"""
        inst = self.get_instances(JobRunCriteria.instance_match(instance_id))
        return inst[0] if inst else None

    @abstractmethod
    def get_instances(self, run_match=None):
        pass

    @abstractmethod
    def read_history_runs(self, run_match, sort=SortCriteria.ENDED, *, asc=True, limit=-1, offset=0, last=False):
        pass

    @abstractmethod
    def read_history_stats(self, run_match=None):
        pass

    @abstractmethod
    def add_observer_transition(self, observer, priority: int = DEFAULT_OBSERVER_PRIORITY):
        """
        Add an observer for job instance transitions in this environment.
        The observer will be notified of all transitions for all instances.

        Args:
            observer: The transition observer to add
            priority: Priority level for the observer (lower numbers = higher priority)
        """
        pass

    @abstractmethod
    def remove_observer_transition(self, observer):
        """Remove a previously registered transition observer."""
        pass

    @abstractmethod
    def add_observer_output(self, observer, priority: int = DEFAULT_OBSERVER_PRIORITY):
        """
        Add an observer for job instance outputs in this environment.
        The observer will be notified of all outputs from all instances.

        Args:
            observer: The output observer to add
            priority: Priority level for the observer (lower numbers = higher priority)
        """
        pass

    @abstractmethod
    def remove_observer_output(self, observer):
        """Remove a previously registered output observer."""
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @abstractmethod
    def close(self):
        pass


def local(env_id=DEF_ENV_ID, persistence=None, connector_layout=None) -> EnvironmentConnector:
    layout = connector_layout or StandardLocalConnectorLayout.create(env_id)
    persistence = persistence or sqlite.create(':memory:')  # TODO Load correct database
    client = RemoteCallClient(layout.provider_sockets_server_rpc, layout.socket_client_rpc)
    transition_receiver = InstanceTransitionReceiver(layout.socket_listener_events)
    output_receiver = InstanceOutputReceiver(layout.socket_listener_events)
    return LocalConnector(env_id, layout, persistence, client, transition_receiver, output_receiver)


class LocalConnector(JobInstanceObservable, EnvironmentConnector):

    def __init__(self, env_id, connector_layout, persistence, client, transition_receiver, output_receiver):
        JobInstanceObservable.__init__(self)
        self.env_id = env_id
        self._layout = connector_layout
        self._persistence = persistence
        self._client = client
        self._transition_receiver = transition_receiver
        self._output_receiver = output_receiver

    def open(self):
        self._persistence.open()

        self._transition_receiver.add_observer_transition(self._transition_notification.observer_proxy)
        self._transition_receiver.start()

        self._output_receiver.add_observer_output(self._output_notification.observer_proxy)
        self._output_receiver.start()

    def get_active_runs(self, run_match=None):
        """Retrieve active job runs from all available servers.

        Args:
            run_match: Optional criteria for filtering job runs
                      (default: None, which matches all runs)

        Returns:
            List of JobRun objects from all responding servers
        """
        run_results = self._client.collect_active_runs(run_match)
        active_runs = []

        for result in run_results:
            if result.error:
                log.warning(f"[remote_call_error] op=[collect_active_runs] server=[{result.server_address}]",
                            exc_info=result.error)
                continue
            active_runs.extend(result.retval)

        return active_runs

    def get_instances(self, run_match=None):
        run_results = self._client.collect_active_runs(run_match)
        instances = []

        for result in run_results:
            if result.error:
                # TODO Skip servers with errors
                continue

            server_address = result.server_address
            for job_run in result.retval:
                instances.append(JobInstanceRemote(self._client, server_address, job_run.instance_id))

        return instances

    def read_history_runs(self, run_match, sort=SortCriteria.ENDED, *, asc=True, limit=-1, offset=0, last=False):
        return self._persistence.read_history_runs(run_match, sort, asc=asc, limit=limit, offset=offset, last=last)

    def read_history_stats(self, run_match=None):
        return self._persistence.read_history_stats(run_match)

    def close(self):
        self._output_receiver.remove_observer_output(self._output_notification.observer_proxy)
        self._transition_receiver.remove_observer_transition(self._transition_notification.observer_proxy)

        run_isolated_collect_exceptions(
            "Errors during closing local environment",
            self._output_receiver.close,
            self._transition_receiver.close,
            self._client.close,
            self._persistence.close,
            self._layout.cleanup,
        )
