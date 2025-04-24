import importlib
import pkgutil
from abc import ABC
from enum import Enum
from typing import Dict, Any

from pydantic import BaseModel, Field

from runtools import runcore
from runtools.runcore.err import RuntoolsException
from runtools.runcore.job import InstanceStageObserver, InstanceStageEvent
from runtools.runcore.run import Stage

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


def create_persistence(env_id, persistence_config):
    if not persistence_config.enabled:
        return None

    return load_database_module(persistence_config.type).create(env_id, **persistence_config.params)


class DatabaseNotFoundError(RuntoolsException):

    def __init__(self, module_):
        super().__init__(f'Cannot find database module {module_}. Ensure this module is installed '
                         f'or check that the provided persistence type value is correct.')


class PersistenceConfig(BaseModel):
    type: str
    enabled: bool = True
    params: Dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def default_sqlite(cls):
        return cls(type="sqlite", enabled=True, params={})


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


class PersistingObserver(InstanceStageObserver):

    def __init__(self, persistence):
        self._persistence = persistence

    def new_instance_stage(self, event: InstanceStageEvent):
        if event.new_stage == Stage.ENDED:
            self._persistence.store_job_runs(event.job_run)
