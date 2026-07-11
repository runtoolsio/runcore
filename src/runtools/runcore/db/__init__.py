"""
Environment database layer.

Each environment has a database that stores configuration, run history, and (future) permissions.
The database is not optional — every environment has one, though it may be transient/in-memory
for testing (in-process environments).

Key Components:
    EnvironmentDatabase: ABC combining ConfigStorage and RunStorage over a single backing store.
    ConfigStorage: Protocol for environment configuration (config table).
    RunStorage: Protocol for job run history (runs table).

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
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Iterator, NamedTuple, Optional

from runtools.runcore.err import RuntoolsException
from runtools.runcore.job import InstanceID, JobRun, SignalSender
from runtools.runcore.matching import SortOption


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

        Returns a dict of all stored config keys with parsed values.
        """

    @abstractmethod
    def save_config(self, env_id: str, config: dict[str, Any]):
        """Replace all stored config keys.

        Performs a full replace (delete + insert). The dict should contain
        top-level config keys with JSON-serializable values.
        """


HEARTBEAT_INTERVAL = 15.0
"""Seconds between liveness touches of a node's active runs (see :meth:`RunStorage.touch_heartbeats`).
Readers treat a heartbeat older than a few multiples of this as a lost run."""

HEARTBEAT_STALE_AFTER = 3 * HEARTBEAT_INTERVAL
"""Seconds without a heartbeat before a run's owner counts as dead — the shared liveness policy:
consumer proxies report the run lost, and the signal sweep treats its pending commands as orphaned."""


class RunVersion(NamedTuple):
    """Change-detection and liveness row for one materialized active run.

    ``cursor`` is the row's write-time ``updated_at`` rendered as an opaque string — it changes on
    every accepted write (even when domain freshness ties), so a poller compares it for equality to
    find changed instances. This relies on writes to a given row being >1µs apart, which holds under
    single-writer-per-instance + per-statement commit; if that invariant is ever relaxed
    (multi-writer per row), switch to a monotonic counter. Never parsed, ordered, or treated as a
    datetime (its representation is backend-specific).

    ``heartbeat_age`` is seconds since the run's last liveness touch, computed on the storage's own
    clock at read time — write and read share one clock, so node/consumer skew cancels. None when
    the row predates heartbeats.
    """
    instance_id: InstanceID
    cursor: str
    heartbeat_age: Optional[float]


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
            user_params: Optional user-defined parameters. Stored in the full run document once
                the first snapshot is persisted; init-only rows keep only identity/projection data.
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
    def read_instance_ids(self, run_match=None) -> list[InstanceID]:
        """Return the IDs of runs matching the criteria, without reading run documents.

        The identity-projection query: same criteria semantics as the full reads, but selecting
        identity only — so init-only rows participate (matched via identity/tags and the
        lifecycle projections; they cannot satisfy phase criteria). E.g. a caller checking
        whether pending signals still have live targets expresses "non-ended" as lifecycle
        criteria (stage CREATED or RUNNING).
        """

    @abstractmethod
    def active_run_versions(self) -> list[RunVersion]:
        """Return a change-detection cursor for each materialized active run.

        Cheap poll primitive: scans (instance, updated_at) only, without deserializing full
        snapshots. Includes non-ended rows carrying a real run document (``run IS NOT NULL``),
        skips init-only rows. Compare the opaque cursor for equality against a prior scan to find
        changed instances, then deep-read those via :meth:`read_active_runs`.
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
    def touch_heartbeats(self, instance_ids: Iterable[InstanceID]):
        """Refresh the liveness timestamp of still-active runs.

        Sets ``heartbeat_at`` for the given instances' non-ended rows; ended rows are
        ignored. Must NOT move the ``updated_at`` change-detection cursor — liveness is
        orthogonal to state freshness, and bumping the cursor would make every consumer
        deep-read unchanged rows on each heartbeat.

        Args:
            instance_ids: Instances whose heartbeat to refresh.
        """

    @abstractmethod
    def remove_runs(self, run_match) -> list[InstanceID]:
        """Remove job runs matching the specified criteria."""


@dataclass(frozen=True)
class Signal:
    """An in-flight remote control command from the signals mailbox (design point 5).

    Consumer-written, node-applied, deleted on apply — the durable record of applied control
    lives on the run itself (``JobRun.control_requests``).
    """
    signal_id: int
    instance_id: InstanceID
    phase_id: Optional[str]
    op: str
    args: tuple
    requested_at: datetime


class SignalStorage(SignalSender, ABC):
    """Remote control mailbox stored in the database (design point 5).

    Consumers post command rows (the :class:`SignalSender` side); the owning node reads the rows
    for its instances, applies them at its control surface, and deletes them. Duplicate signals
    are permitted — remote control ops are idempotent by contract, so re-application is harmless.
    """

    @abstractmethod
    def read_signals(self, instance_ids: Optional[Iterable[InstanceID]] = None, *,
                     older_than: Optional[float] = None) -> list[Signal]:
        """Return the pending command rows matching the given filters, oldest first.

        The age filter is the primitive behind orphan sweeping — deciding which aged rows
        actually *are* orphans is the caller's policy (it needs the run view, which is not
        this storage's domain).

        Args:
            instance_ids: Restrict to these instances. None means all instances — note that
                an (explicitly passed) empty iterable means, and returns, none.
            older_than: Only rows older than this many seconds, on the storage's own clock
                (matching the write side).
        """

    @abstractmethod
    def delete_signals(self, signal_ids: Iterable[int]):
        """Remove applied (or rejected-and-logged) command rows by their surrogate ids."""


class EnvironmentDatabase(ConfigStorage, RunStorage, SignalStorage, ABC):
    """The environment's database — a single store implementing config, run and signal storage.

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
