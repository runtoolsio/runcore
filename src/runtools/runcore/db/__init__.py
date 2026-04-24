"""
Environment database layer.

Each environment has a database that stores configuration, run history, and (future) permissions.
The database is not optional — every environment has one, though it may be transient/in-memory
for testing (in-process environments).

Key Components:
    EnvironmentDatabase: ABC combining ConfigStorage and RunStorage over a single backing store.
    ConfigStorage: Protocol for environment configuration (config table).
    RunStorage: Protocol for job run history (runs table).
Factory Functions:
    load_database_module: Dynamically loads a database backend module (e.g., sqlite).

Driver Module Contract:
    Each driver module must expose four module-level functions:

    create_environment(entry, config) -> None
        Provision the backing store, init schema, and seed config.
    create(entry, **kwargs) -> EnvironmentDatabase
        Return an (unopened) database handle for an existing environment.
    exists(entry) -> bool
        Check whether the backing store for the entry exists.
    delete(entry) -> None
        Delete the backing store if it exists.

See Also:
    runtools.runcore.db.sqlite: SQLite implementation.
"""

import importlib
from abc import ABC, abstractmethod
from typing import Any, Iterator

from runtools.runcore.err import RuntoolsException
from runtools.runcore.job import InstanceID, JobRun
from runtools.runcore.matching import SortOption
from runtools.runcore.retention import RetentionPolicy

_db_modules = {}


def load_database_module(db_type):
    """Load a database backend module by type name.

    Imports ``runtools.runcore.db.<db_type>`` directly. The module is expected to
    expose four module-level functions: ``create()``, ``create_environment()``,
    ``exists()``, and ``delete()``.

    Args:
        db_type: Database type identifier (e.g., "sqlite").

    Returns:
        The loaded database module.

    Raises:
        DatabaseNotFoundError: If no matching module is found.
    """
    module = _db_modules.get(db_type)
    if module:
        return module
    module_name = f"runtools.runcore.db.{db_type}"
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError as e:
        if e.name == module_name:
            raise DatabaseNotFoundError(db_type)
        raise
    _db_modules[db_type] = module
    return module


class DatabaseNotFoundError(RuntoolsException):

    def __init__(self, module_):
        super().__init__(f'Cannot find database module {module_}. Ensure this module is installed '
                         f'or check that the provided database driver is correct.')


class IncompatibleSchemaError(RuntoolsException):

    def __init__(self, found_version, expected_version):
        super().__init__(
            f'Database schema version mismatch: found {found_version}, expected {expected_version}. '
            f'The database was created by a different version of runtools.')
        self.found_version = found_version
        self.expected_version = expected_version


class ConfigStorage(ABC):
    """Environment configuration stored in the database."""

    @abstractmethod
    def load_config(self, env_id: str) -> dict[str, Any]:
        """Load environment config as a dict ready for Pydantic validation.

        Returns a dict with 'id' (injected) and all stored config keys with parsed values.
        """

    @abstractmethod
    def save_config(self, env_id: str, config: dict[str, Any]):
        """Replace all stored config keys.

        Performs a full replace (delete + insert). The dict should contain
        top-level config keys with JSON-serializable values (excluding id).
        """


class RunStorage(ABC):
    """Job run history stored in the database."""

    @abstractmethod
    def init_run(self, job_id: str, run_id: str, user_params=None, *,
                 auto_increment: bool = False, max_retries: int = 5) -> InstanceID:
        """Insert a partial record at instance creation time.

        Semantics:
            - auto_increment=False: insert with ordinal 1. If the (job_id, run_id, 1) key
              already exists, raise DuplicateInstanceError.
            - auto_increment=True: allocate the next free ordinal for (job_id, run_id),
              insert, and return the concrete InstanceID.
        """

    @abstractmethod
    def read_runs(self, run_match=None, sort=SortOption.CREATED, *, asc, limit, offset, last=False) -> list[JobRun]:
        """Fetch ended job runs matching the specified criteria."""

    @abstractmethod
    def iter_runs(self, run_match=None, sort=SortOption.CREATED, *,
                  asc=True, limit=-1, offset=-1, last=False) -> Iterator[JobRun]:
        """Iterate over ended job runs matching the specified criteria."""

    @abstractmethod
    def read_run_stats(self, run_match=None):
        """Compute aggregate statistics for jobs matching the specified criteria."""

    @abstractmethod
    def store_runs(self, *job_runs):
        """Store one or more completed job runs."""

    @abstractmethod
    def remove_runs(self, run_match) -> list[InstanceID]:
        """Remove job runs matching the specified criteria."""

    @abstractmethod
    def enforce_retention(self, job_id: str, policy: RetentionPolicy):
        """Prune old runs according to retention policy."""


class EnvironmentDatabase(ConfigStorage, RunStorage, ABC):
    """The environment's database — a single store implementing both ConfigStorage and RunStorage.

    Lifecycle:
        Call :meth:`open` before any read/write operations, and :meth:`close` when done.
        Supports context manager protocol for automatic resource management.
    """

    @abstractmethod
    def open(self):
        """Open the database connection. Must be called before any operations."""

    @abstractmethod
    def close(self):
        """Close the database connection and release resources."""

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
