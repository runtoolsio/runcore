from abc import ABC, abstractmethod
from threading import Event

from runtools.runcore import RemoteCallClient, InstanceTransitionReceiver
from runtools.runcore.criteria import JobRunCriteria
from runtools.runcore.db import SortCriteria, sqlite
from runtools.runcore.job import JobInstanceObservable
from runtools.runcore.listening import InstanceOutputReceiver
from runtools.runcore.util.err import run_isolated_collect_exceptions
from runtools.runcore.util.observer import DEFAULT_OBSERVER_PRIORITY


class Environment(ABC):

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


class PersistingEnvironment(Environment, ABC):

    def __init__(self, persistence):
        self._persistence = persistence

    def open(self):
        self._persistence.open()

    def read_history_runs(self, run_match, sort=SortCriteria.ENDED, *, asc=True, limit=-1, offset=0, last=False):
        return self._persistence.read_history_runs(run_match, sort, asc=asc, limit=limit, offset=offset, last=last)

    def read_history_stats(self, run_match=None):
        return self._persistence.read_history_stats(run_match)

    def close(self):
        self._persistence.close()


def local(persistence=None):
    persistence = persistence or sqlite.create(':memory:')
    return LocalEnvironment(persistence)


class LocalEnvironment(JobInstanceObservable, PersistingEnvironment, Environment):

    def __init__(self, persistence):
        JobInstanceObservable.__init__(self)
        PersistingEnvironment.__init__(self, persistence)
        self._client = RemoteCallClient()
        self._transition_receiver = InstanceTransitionReceiver()
        self._output_receiver = InstanceOutputReceiver()

    def open(self):
        PersistingEnvironment.open(self)

        self._transition_receiver.add_observer_transition(self._transition_notification.observer_proxy)
        self._transition_receiver.start()

        self._output_receiver.add_observer_output(self._output_notification.observer_proxy)
        self._output_receiver.start()

    def get_active_runs(self, run_match=None):
        return [run for res in self._client.collect_active_runs(run_match) if not res.error for run in res.retval]

    def get_instances(self, run_match=None):
        pass

    def close(self):
        self._output_receiver.remove_observer_output(self._output_notification.observer_proxy)
        self._transition_receiver.remove_observer_transition(self._transition_notification.observer_proxy)

        run_isolated_collect_exceptions(
            "Errors during closing local environment",
            self._output_receiver.close,
            self._transition_receiver.close,
            self._client.close,
            lambda: PersistingEnvironment.close(self)
        )

def wait_for_interrupt(env, *, reraise=True):
    try:
        Event().wait()
    except KeyboardInterrupt:
        env.close()
    finally:
        if reraise:
            raise KeyboardInterrupt
