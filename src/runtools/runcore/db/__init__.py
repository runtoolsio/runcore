import importlib
import pkgutil
from abc import ABC
from enum import Enum

from runtools import runcore
from runtools.runcore.job import JobRun
from runtools.runcore.common import RuntoolsException
from runtools.runcore.job import InstanceTransitionObserver
from runtools.runcore.run import PhaseRun, RunState

_db_modules = {}


def load_database_module(db_type):
    """
    Loads the database module specified by the parameter.

    Args:
        db_type (str): Type of the database to be loaded
    """
    db_module = _db_modules.get(db_type)
    if db_module:
        return db_module

    for finder, name, is_pkg in pkgutil.iter_modules(runcore.db.__path__, runcore.db.__name__ + "."):
        if name == runcore.db.__name__ + "." + db_type:
            return importlib.import_module(name)

    raise DatabaseNotFoundError(runcore.db.__name__ + "." + db_type)


class DatabaseNotFoundError(RuntoolsException):

    def __init__(self, module_):
        super().__init__(f'Cannot find database module {module_}. Ensure this module is installed '
                         f'or check that the provided persistence type value is correct.')


class SortCriteria(Enum):
    """
    Enum representing the criteria by which job instance rows can be sorted.

    Attributes:
    - CREATED: Sort by the timestamp when the job instance was created.
    - ENDED: Sort by the timestamp when the job instance ended or was completed.
    - TIME: Sort by the execution time of the job instance.
    """
    CREATED = 1
    ENDED = 2
    TIME = 3


class Persistence(ABC):

    def read_history_runs(self, run_match=None, sort=SortCriteria.CREATED, *, asc, limit, offset, last=False):
        pass

    def read_history_stats(self, run_match=None):
        pass

    def store_job_runs(self, *job_runs):
        pass

    def remove_job_runs(self, run_match):
        pass

    def clean_up(self, max_records, max_age):
        pass

    def close(self):
        pass

class PersistingObserver(InstanceTransitionObserver):

    def __init__(self, persistence):
        self._persistence = persistence

    def new_instance_phase(self, job_run: JobRun, previous_phase: PhaseRun, new_phase: PhaseRun, ordinal: int):
        if new_phase.run_state == RunState.ENDED:
            self._persistence.store_job_runs(job_run)
