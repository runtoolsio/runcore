from abc import ABC, abstractmethod

from runtools.runcore import APIClient, InstanceTransitionReceiver
from runtools.runcore.db import SortCriteria
from runtools.runcore.job import InstanceTransitionObserver, InstanceOutputObserver
from runtools.runcore.listening import InstanceOutputReceiver
from runtools.runcore.util.observer import DEFAULT_OBSERVER_PRIORITY, ObservableNotification, MultipleExceptions


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
    def get_active_runs(self, run_match):
        pass

    @abstractmethod
    def get_instances(self, run_match):
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


class LocalEnvironment(Environment):

    def __init__(self, persistence):
        self._persistence = persistence
        self._client = APIClient()
        self._transition_notification = ObservableNotification[InstanceTransitionObserver]()
        self._output_notification = ObservableNotification[InstanceOutputObserver]()
        self._transition_receiver = InstanceTransitionReceiver()
        self._transition_receiver.add_observer_transition(self._transition_notification.observer_proxy)
        self._output_receiver = InstanceOutputReceiver()
        self._output_receiver.add_observer_output(self._output_notification.observer_proxy)

    def open(self):
        self._transition_receiver.start()
        self._output_receiver.start()

    def get_active_runs(self, run_match):
        return self._client.get_active_runs(run_match)

    def get_instances(self, run_match):
        pass

    def read_history_runs(self, run_match, sort=SortCriteria.ENDED, *, asc=True, limit=-1, offset=0, last=False):
        return self._persistence.read_history_runs(run_match, sort, asc=asc, limit=limit, offset=offset, last=last)

    def read_history_stats(self, run_match=None):
        return self._persistence.read_history_stats(run_match)

    def add_observer_transition(self, observer, priority: int = DEFAULT_OBSERVER_PRIORITY):
        self._transition_notification.add_observer(observer, priority)

    def remove_observer_transition(self, observer):
        self._transition_notification.remove_observer(observer)

    def add_observer_output(self, observer, priority: int = DEFAULT_OBSERVER_PRIORITY):
        self._output_notification.add_observer(observer, priority)

    def remove_observer_output(self, observer):
        self._output_notification.remove_observer(observer)

    def close(self):
        exceptions = []

        try:
            self._output_receiver.close()
        except Exception as e:
            exceptions.append(e)

        try:
            self._transition_receiver.close()
        except Exception as e:
            exceptions.append(e)

        try:
            self._client.close()
        except Exception as e:
            exceptions.append(e)

        try:
            self._persistence.close()
        except Exception as e:
            exceptions.append(e)

        if exceptions:
            if len(exceptions) > 1:
                raise MultipleExceptions(exceptions)
            raise exceptions[0]
