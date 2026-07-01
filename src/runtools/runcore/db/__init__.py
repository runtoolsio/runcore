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
    load_database_module: Resolves the built-in database backend module for a driver name.

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

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Iterable, Iterator

from runtools.runcore.err import RuntoolsException
from runtools.runcore.job import InstanceID, JobRun
from runtools.runcore.matching import SortOption


def load_database_module(db_type):
    """Resolve the database backend module for a driver name.

    Each module exposes the driver contract (``create()``, ``create_environment()``, ``exists()``,
    ``delete()``). Imported lazily per branch so an unused backend's optional deps (e.g. ``psycopg``
    for postgres) are never imported.

    Args:
        db_type: Database driver identifier (``"sqlite"`` | ``"postgres"``).

    Returns:
        The database backend module.

    Raises:
        DatabaseNotFoundError: If ``db_type`` is not a known backend.
    """
    match db_type:
        case "sqlite":
            from runtools.runcore.db import sqlite
            return sqlite
        case "postgres":
            from runtools.runcore.db import postgres
            return postgres
    raise DatabaseNotFoundError(db_type)


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


class EnvironmentStoreNotProvisionedError(RuntoolsException):
    """The environment is registered but its backing store has not been provisioned.

    Distinct from a missing registry entry (``EnvironmentNotFoundError``): the environment is
    known, but its tables/store don't exist yet. Raised by drivers whose ``open()`` only validates
    (notably Postgres, where store DDL is an admin-only privilege). An administrator must create it
    via ``create_environment`` before it can be opened.
    """

    def __init__(self, env_id):
        super().__init__(
            f"Environment '{env_id}' backing store is not provisioned. "
            f"An administrator must create it (create_environment) before it can be opened.")
        self.env_id = env_id


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


RunVersion = tuple[InstanceID, str]
"""(instance, opaque version cursor) for change detection. The cursor is the row's write-time
``updated_at`` rendered as an opaque string — it changes on every accepted write (even when domain
freshness ties), so a poller compares it for equality to find changed instances. This relies on
writes to a given row being >1µs apart, which holds under single-writer-per-instance + per-statement
commit; if that invariant is ever relaxed (multi-writer per row), switch to a monotonic counter.
Never parsed, ordered, or treated as a datetime (its representation is backend-specific)."""


class RunStorage(ABC):
    """Job run history stored in the database."""

    @abstractmethod
    def init_run(self, job_id: str, run_id: str, user_params=None, *,
                 created_at: datetime,
                 tags: Iterable[str] = (),
                 auto_increment: bool = False, max_retries: int = 5) -> InstanceID:
        """Insert a partial record at instance creation time.

        Args:
            job_id: Job identifier.
            run_id: Run identifier within the logical run.
            user_params: Optional user-defined parameters serialized into the row.
            created_at: Canonical creation timestamp. Keyword-only so it can't be
                confused with ``user_params``. Caller is the source of truth —
                typically ``root_phase.created_at`` from which ``lifecycle.created_at``
                also derives. Stored in the runs table and meant to be reused for any
                co-recorded timestamp (e.g., S3 object metadata for retention ordering).
            tags: Optional user-set labels for grouping/filtering. Normalized
                (trimmed/lowercased/dedup'd) by the implementation; written into
                the tags junction table in the same transaction as the run row.
            auto_increment: If False, insert with ordinal 1 and raise
                DuplicateInstanceError on (job_id, run_id, 1) collision. If True,
                allocate the next free ordinal for (job_id, run_id) and insert.
            max_retries: Max ordinal allocation retries when auto_increment=True
                under concurrent inserts.

        Returns:
            The fully-qualified InstanceID (job_id, run_id, ordinal) of the inserted row.
        """

    @abstractmethod
    def read_runs(self, run_match=None, sort=SortOption.CREATED, *, asc, limit, offset, last=False) -> list[JobRun]:
        """Fetch ended job runs matching the specified criteria."""

    @abstractmethod
    def iter_runs(self, run_match=None, sort=SortOption.CREATED, *,
                  asc=True, limit=-1, offset=-1, last=False) -> Iterator[JobRun]:
        """Iterate over ended job runs matching the specified criteria."""

    @abstractmethod
    def read_active_runs(self, run_match=None) -> list[JobRun]:
        """Return the latest persisted snapshot of each active (non-ended) run.

        The mirror of :meth:`read_runs` (which returns only ended history): selects rows
        that have no termination and carry a real snapshot, skipping init-only rows the
        run-state persister has not filled yet. Snapshots lag the persister's flush
        interval and, after a producer crash, may name runs that are no longer running.
        """

    @abstractmethod
    def active_run_versions(self) -> list[RunVersion]:
        """Return a change-detection cursor for each materialized active run.

        Cheap poll primitive: scans (instance, updated_at) only, without deserializing full
        snapshots. Includes non-ended rows carrying a real snapshot (root_phase set), skips
        init-only rows. Compare the opaque cursor for equality against a prior scan to find changed
        instances, then deep-read those via :meth:`read_active_runs`.
        """

    @abstractmethod
    def read_run_stats(self, run_match=None):
        """Compute aggregate statistics for jobs matching the specified criteria."""

    @abstractmethod
    def store_runs(self, *job_runs):
        """Authoritatively update the stored rows for one or more job runs, terminal included."""

    @abstractmethod
    def store_active_runs(self, *job_runs):
        """Update persisted snapshots for runs that are still active.

        Implementations must ignore candidates that would regress storage
        state: a candidate must not overwrite a terminal row or a newer active
        snapshot. This lets callers safely retry retained snapshots after
        transient failures.

        Args:
            *job_runs: Latest active snapshots to persist.
        """

    @abstractmethod
    def remove_runs(self, run_match) -> list[InstanceID]:
        """Remove job runs matching the specified criteria."""


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
