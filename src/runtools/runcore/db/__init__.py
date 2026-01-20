"""
Persistence layer for storing and retrieving job run history.

This package provides a pluggable persistence abstraction with database-specific implementations
loaded dynamically via :func:`load_database_module`. The default implementation uses SQLite.

Environment nodes use :class:`PersistingObserver` to automatically store job runs on completion,
while connectors use persistence for reading job run history.

Key Components:
    Persistence: Abstract base class defining the storage interface for job runs.
    NullPersistence: No-op implementation used when persistence is disabled.
    PersistenceConfig: Pydantic model for configuring persistence settings.
    PersistingObserver: Lifecycle observer that auto-persists job runs on completion.

Factory Functions:
    create_persistence: Creates a Persistence instance from config.
    load_database_module: Dynamically loads a database backend module.

See Also:
    runtools.runcore.db.sqlite: SQLite implementation with batch fetching.
"""

import importlib
import pkgutil
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

from pydantic import BaseModel, Field

from runtools import runcore
from runtools.runcore.criteria import SortOption
from runtools.runcore.err import RuntoolsException
from runtools.runcore.job import InstanceLifecycleObserver, InstanceLifecycleEvent
from runtools.runcore.run import Stage

_db_modules = {}


def load_database_module(db_type):
    """
    Load a database backend module by type name.

    Searches for a submodule named `db_type` within the ``runtools.runcore.db`` package
    and returns it. The module is expected to provide a ``create()`` factory function
    that returns a :class:`Persistence` instance.

    Args:
        db_type: Database type identifier (e.g., "sqlite").

    Returns:
        The loaded database module.

    Raises:
        DatabaseNotFoundError: If no matching module is found.
    """
    db_module = _db_modules.get(db_type)
    if db_module:
        return db_module

    for finder, name, is_pkg in pkgutil.iter_modules(runcore.db.__path__, runcore.db.__name__ + "."):
        if name == runcore.db.__name__ + "." + db_type:
            return importlib.import_module(name)

    raise DatabaseNotFoundError(runcore.db.__name__ + "." + db_type)


def create_persistence(env_id, persistence_config):
    """
    Create a Persistence instance from configuration.

    Args:
        env_id: Environment identifier, used to locate the database for the environment when not provided in the config.
        persistence_config: A :class:`PersistenceConfig` specifying the database type and settings.

    Returns:
        A :class:`Persistence` instance, or None if persistence is disabled.
    """
    if not persistence_config.enabled:
        return None

    return load_database_module(persistence_config.type).create(
        env_id, persistence_config.database, **persistence_config.params)


class DatabaseNotFoundError(RuntoolsException):

    def __init__(self, module_):
        super().__init__(f'Cannot find database module {module_}. Ensure this module is installed '
                         f'or check that the provided persistence type value is correct.')


class PersistenceConfig(BaseModel):
    type: str
    enabled: bool = True
    database: Optional[str] = Field(default=None)
    params: Dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def default_sqlite(cls):
        return cls(type="sqlite", enabled=True)

    @classmethod
    def in_memory_sqlite(cls):
        return cls(type="sqlite", enabled=True, database=":memory:")


class Persistence(ABC):

    @property
    def enabled(self):
        return True

    def open(self):
        pass

    @abstractmethod
    def read_history_runs(self, run_match=None, sort=SortOption.CREATED, *, asc, limit, offset, last=False):
        pass

    @abstractmethod
    def iter_history_runs(self, run_match=None, sort=SortOption.CREATED, *,
                          asc=True, limit=-1, offset=-1, last=False):
        """
        Iterate over ended job instances based on specified criteria.

        This method provides memory-efficient access to job history by yielding
        results one at a time rather than loading all records into memory.

        Args:
            run_match: Criteria to match specific job instances
            sort: Field by which records are sorted
            asc: Sort order (True for ascending)
            limit: Maximum number of records (-1 for unlimited)
            offset: Number of records to skip (-1 for no offset)
            last: If True, only the last record for each job

        Yields:
            JobRun: Individual job instances matching the criteria

        Returns:
            Iterator[JobRun]: An iterator over JobRun instances.
        """
        pass

    @abstractmethod
    def read_history_stats(self, run_match=None):
        pass

    @abstractmethod
    def store_job_runs(self, *job_runs):
        pass

    @abstractmethod
    def remove_job_runs(self, run_match):
        pass

    @abstractmethod
    def clean_up(self, max_records, max_age):
        pass

    @abstractmethod
    def close(self):
        pass


class NullPersistence(Persistence):
    """
    A no-op persistence. .enabled == False, and any attempt to read/write
    will raise PersistenceDisabledError so callers can distinguish “no-data”
    from “disabled”.
    """

    @property
    def enabled(self) -> bool:
        return False

    def read_history_runs(self, run_match=None, sort=SortOption.CREATED, *, asc, limit, offset, last=False):
        raise PersistenceDisabledError("Persistence is disabled; no history available.")

    def iter_history_runs(self, run_match=None, sort=SortOption.CREATED, *,
                          asc=True, limit=-1, offset=-1, last=False):
        raise PersistenceDisabledError("Persistence is disabled; no history available.")

    def read_history_stats(self, run_match=None) -> dict:
        raise PersistenceDisabledError("Persistence is disabled; no stats available.")

    def store_job_runs(self, *job_runs) -> None:
        pass

    def remove_job_runs(self, run_match) -> None:
        pass

    def clean_up(self, max_records: int, max_age: float) -> None:
        pass

    def close(self) -> None:
        pass


class PersistingObserver(InstanceLifecycleObserver):

    def __init__(self, persistence):
        """
        TODO Ensure highest priority so run is stored before other observers are notified
        This is to prevent race condition in commands like wait:
        1. Run ended event -> notification before wait set up its observer
        2. Wait set up observer now and check persistence <-- no event
        3. Run stored to persistence
        4. -> Event missed by wait <-
        """
        self._persistence = persistence

    def instance_lifecycle_update(self, event: InstanceLifecycleEvent):
        if event.new_stage == Stage.ENDED:
            self._persistence.store_job_runs(event.job_run)


class PersistenceDisabledError(RuntoolsException):
    pass
