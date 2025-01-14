from abc import ABC, abstractmethod

from runtools.runcore.db import SortCriteria


class Environment(ABC):

    @abstractmethod
    def get_instances(self, run_match):
        pass

    @abstractmethod
    def read_history_runs(self, run_match, sort=SortCriteria.ENDED, *, asc=True, limit=-1, offset=0, last=False):
        pass

    @abstractmethod
    def read_history_stats(self, run_match=None):
        pass
