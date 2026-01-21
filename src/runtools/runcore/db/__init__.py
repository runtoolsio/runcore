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

PERSISTING_OBSERVER_PRIORITY = 10  # High priority (low number) to ensure persistence before event dispatch

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
    """
    Abstract base class defining the storage interface for job run history.

    Environment nodes use persistence (via :class:`PersistingObserver`) to automatically store job runs
    when they complete. Connectors use persistence to read job history for querying and monitoring.

    Implementations must provide methods for reading, writing, and cleaning up job run records.
    The default implementation is :class:`~runtools.runcore.db.sqlite.SQLite`. Use :class:`NullPersistence`
    when persistence should be disabled.

    Lifecycle:
        Call :meth:`open` before any read/write operations, and :meth:`close` when done.
        Supports context manager protocol for automatic resource management.

    See Also:
        :func:`create_persistence`: Factory function to create instances from configuration.
        :class:`PersistingObserver`: Observer that auto-persists job runs on completion.
    """

    @property
    def enabled(self):
        """Whether this persistence instance is enabled. Returns False for :class:`NullPersistence`."""
        return True

    def open(self):
        """Open the persistence connection. Must be called before any read/write operations."""
        pass

    @abstractmethod
    def read_history_runs(self, run_match=None, sort=SortOption.CREATED, *, asc, limit, offset, last=False):
        """
        Fetch ended job runs matching the specified criteria.

        Loads all matching records into memory. For large result sets, use :meth:`iter_history_runs` instead.

        Args:
            run_match (JobRunCriteria): Criteria to filter job instances. None returns all records.
            sort (SortOption): Field to sort by (default: CREATED).
            asc (bool): Sort ascending if True, descending if False.
            limit (int): Maximum records to return (-1 for unlimited).
            offset (int): Number of records to skip (-1 for no offset).
            last (bool): If True, return only the most recent run for each job.

        Returns:
            List[JobRun]: Collection of matching job runs.
        """
        pass

    @abstractmethod
    def iter_history_runs(self, run_match=None, sort=SortOption.CREATED, *,
                          asc=True, limit=-1, offset=-1, last=False):
        """
        Iterate over ended job runs matching the specified criteria.

        This method provides memory-efficient access to job history by yielding
        results one at a time rather than loading all records into memory.

        Args:
            run_match (JobRunCriteria): Criteria to match specific job instances. None returns all records.
            sort (SortOption): Field by which records are sorted.
            asc (bool): Sort order (True for ascending).
            limit (int): Maximum number of records (-1 for unlimited).
            offset (int): Number of records to skip (-1 for no offset).
            last (bool): If True, only the last record for each job.

        Returns:
            Iterator[JobRun]: Iterator over matching job run records.
        """
        pass

    @abstractmethod
    def read_history_stats(self, run_match=None):
        """
        Compute aggregate statistics for jobs matching the specified criteria.

        Args:
            run_match (JobRunCriteria): Criteria to filter job instances. None includes all records.

        Returns:
            List[JobStats]: Statistics for each job including count, timing, and failure information.
        """
        pass

    @abstractmethod
    def store_job_runs(self, *job_runs):
        """
        Store one or more completed job runs.

        Args:
            *job_runs (JobRun): Job run instances to persist.
        """
        pass

    @abstractmethod
    def remove_job_runs(self, run_match):
        """
        Remove job runs matching the specified criteria.

        Args:
            run_match (JobRunCriteria): Criteria identifying which job runs to delete.

        Raises:
            ValueError: If no criteria provided (to prevent accidental deletion of all records).
        """
        pass

    @abstractmethod
    def clean_up(self, max_records, max_age):
        """
        Remove old records based on count or age limits.

        Args:
            max_records (int): Maximum number of records to keep (-1 to skip this check).
            max_age (datetime.timedelta): Maximum age of records. Records older than this are deleted. None to skip.
        """
        pass

    @abstractmethod
    def close(self):
        """Close the persistence connection and release resources."""
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
    """
    Lifecycle observer that automatically persists job runs when they complete.

    Important:
        Register this observer with high priority (low number) to ensure runs are stored before
        other observers are notified. This prevents race conditions where another observer checks
        persistence for a run that hasn't been stored yet.

    Example race condition with wait command if priority is not set correctly::

        1. Run ended event -> notification before wait set up its observer
        2. Wait set up observer now and check persistence <-- no event
        3. Run stored to persistence
        4. -> Event missed by wait <-
    """

    def __init__(self, persistence):
        """
        Args:
            persistence (Persistence): The persistence instance to store job runs to.
        """
        self._persistence = persistence

    def instance_lifecycle_update(self, event: InstanceLifecycleEvent):
        if event.new_stage == Stage.ENDED:
            self._persistence.store_job_runs(event.job_run)


class PersistenceDisabledError(RuntoolsException):
    """
    Raised when attempting to read from or query a disabled persistence layer.

    This error is thrown by :class:`NullPersistence` to distinguish between "no data found" and "persistence is disabled".
    """
    pass
