"""PostgreSQL implementation of :class:`EnvironmentDatabase`.

Unlike the SQLite driver (one connection guarded by a process lock), this driver uses a
``psycopg_pool.ConnectionPool`` and lets Postgres serialize concurrent writers — each
operation borrows a connection and runs in its own transaction. JSON is stored as ``JSONB``
and timestamps as ``TIMESTAMPTZ`` (bound as explicit UTC, since the domain uses naive UTC).

The target database named by ``EnvironmentEntry.location`` (a libpq connection string) must
already exist; this driver manages the schema (tables) within it, not the database itself.
"""

import hashlib
import logging
import re
import time
from datetime import timezone
from functools import wraps
from typing import Iterator, List, override

import psycopg
from psycopg import sql
from psycopg.errors import LockNotAvailable, UniqueViolation
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from runtools.runcore.db import (EnvironmentDatabase, EnvironmentStoreNotProvisionedError, IncompatibleSchemaError,
                                 RunVersion, Signal)
from runtools.runcore.db.sql import (build_job_stats, build_order_by, build_where_clause, Dialect, last_run_ids,
                                     LAST_PER_JOB_SQL, matching_pks)
from runtools.runcore.err import InvalidStateError
from runtools.runcore.job import (JobStats, JobRun, JobInstanceMetadata, InstanceID,
                                  DuplicateInstanceError, normalize_tags)
from runtools.runcore.matching import SortOption
from runtools.runcore.output import OutputLine
from runtools.runcore.run import TerminationStatus, Outcome
from runtools.runcore.util import utc_now
from runtools.runcore.util.lock import LockAcquireTimeoutError

log = logging.getLogger(__name__)

SCHEMA_VERSION = 2
DEFAULT_POOL_MAX_SIZE = 10


# --- Driver module contract (create / create_environment / exists / delete) ---

# Environments are isolated by Postgres schema within the database named by `location`: each env
# gets its own `runtools_<readable>_<hash>` schema. The prefix guarantees the driver only ever
# creates and (on delete) drops schemas it owns — so pointing `location` at a shared database can
# never clobber `public` or a foreign app's schema, even if an env_id collides with one.
_SCHEMA_PREFIX = "runtools_"
_MAX_IDENTIFIER = 63  # Postgres truncates identifiers past this (silently) — names must fit within.
_HASH_LEN = 12        # hex chars of the env-id digest; uniqueness comes from this, not the readable part.


def _dsn(entry) -> str:
    if not entry.location:
        raise ValueError("Postgres environment requires a libpq connection string in 'location'")
    return entry.location


def _schema_name(env_id: str) -> str:
    """The (unquoted) schema name for an environment — deterministic and ≤63 bytes.

    Layout: ``runtools_<readable>_<hash>``. The hash (over the full env_id) guarantees uniqueness
    and bounded length; the readable slice is cosmetic (sanitized, truncated to fit), so two long
    env_ids sharing a prefix can't collide even after truncation. Wrap in ``sql.Identifier`` to
    emit as SQL.
    """
    digest = hashlib.blake2b(env_id.encode("utf-8"), digest_size=_HASH_LEN // 2).hexdigest()
    budget = _MAX_IDENTIFIER - len(_SCHEMA_PREFIX) - 1 - len(digest)  # room for prefix + '_' + hash
    readable = re.sub(r"[^a-z0-9_]", "_", env_id.lower())[:budget]
    return f"{_SCHEMA_PREFIX}{readable}_{digest}"


def _schema_present(conn, schema: str) -> bool:
    """Whether ``schema``'s ``schema_info`` table exists — bound param, no search-path reliance."""
    row = conn.execute(
        "SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = %s AND c.relname = 'schema_info'", (schema,)).fetchone()
    return row is not None


def exists(entry) -> bool:
    """True if the environment's schema is provisioned in the target database."""
    with psycopg.connect(_dsn(entry)) as conn:
        return _schema_present(conn, _schema_name(entry.id))


def delete(entry) -> None:
    """Drop the environment's schema (and everything in it). The database is left in place."""
    with psycopg.connect(_dsn(entry)) as conn:
        conn.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
            sql.Identifier(_schema_name(entry.id))))


def create_environment(entry, config) -> None:
    """Provision the schema (privileged DDL) and seed the initial configuration.

    The one place that issues DDL — separate from ``open()``, which only validates. Run by an
    administrator; the role needs ``CREATE`` on the database.
    """
    db = create(entry)
    db._open_pool()
    try:
        db._provision()
        db.save_config(entry.id, config.model_dump(mode='json'))
    finally:
        db.close()


def create(entry, **kwargs) -> 'PostgreSQL':
    """Create a (not yet opened) Postgres database handle for ``entry``."""
    return PostgreSQL(_dsn(entry), entry.id, **kwargs)


# --- Coordination: session-level advisory locks ---

def _advisory_key(env_id: str, lock_id: str) -> int:
    """Stable 64-bit advisory key namespaced by environment.

    Advisory lock space is per database (not per schema), so the env ID is part of the
    key. The hash must be stable across processes (Python's ``hash()`` is per-process
    salted), and the components are length-prefixed so their boundary is unambiguous —
    IDs are arbitrary strings, so ``('a:b', 'c')`` must not key like ``('a', 'b:c')``.
    A key collision over-locks (false contention), never under-locks.
    """
    hasher = hashlib.blake2b(digest_size=8)
    for part in ("runtools", env_id, lock_id):
        encoded = part.encode()
        hasher.update(len(encoded).to_bytes(4, "big"))
        hasher.update(encoded)
    return int.from_bytes(hasher.digest(), signed=True)


class AdvisoryLock:
    """A Postgres session-level advisory lock; its dedicated connection is the lock token.

    Acquiring opens the connection, releasing closes it — session end releases the
    server-side lock, so release cannot fail halfway or leak. Crash release comes for
    free: a dead holder's session drops and Postgres frees the lock. Non-reentrant;
    not shareable between threads.
    """

    def __init__(self, dsn, lock_id, key, *, timeout=10):
        self.lock_id = lock_id
        self._dsn = dsn
        self._key = key
        self._timeout = timeout
        self._conn = None
        self._start_time = None

    def acquire(self):
        """Block until the lock is held, waiting server-side up to the timeout.

        Raises:
            InvalidStateError: If the lock has already been acquired.
            LockAcquireTimeoutError: If the lock cannot be acquired within the timeout.
        """
        if self._conn:
            raise InvalidStateError("Lock is already acquired")

        timeout_ms = int(self._timeout * 1000)
        if timeout_ms <= 0:
            # Postgres treats lock_timeout = 0 as *disabled* (infinite wait), so a zero
            # timeout must become a non-blocking attempt instead
            if not self.try_acquire():
                raise LockAcquireTimeoutError(f"Advisory lock '{self.lock_id}' is held elsewhere (no-wait)")
            return

        conn = psycopg.connect(self._dsn, autocommit=True)
        start_time = time.time()
        try:
            conn.execute("SELECT set_config('lock_timeout', %s, false)", (str(timeout_ms),))
            conn.execute("SELECT pg_advisory_lock(%s)", (self._key,))
        except LockNotAvailable:
            conn.close()
            raise LockAcquireTimeoutError(f"Advisory lock '{self.lock_id}' not acquired within {self._timeout}s")
        except BaseException:
            conn.close()
            raise

        self._conn = conn
        self._start_time = start_time
        wait_time_ms = (time.time() - start_time) * 1000
        log.debug("Lock acquired", extra={"lock": self.lock_id, "wait_ms": round(wait_time_ms, 2)})

    def try_acquire(self) -> bool:
        """Attempt to acquire without waiting.

        Returns:
            bool: True if acquired, False if the lock is held elsewhere.

        Raises:
            InvalidStateError: If the lock has already been acquired.
        """
        if self._conn:
            raise InvalidStateError("Lock is already acquired")

        conn = psycopg.connect(self._dsn, autocommit=True)
        try:
            acquired = conn.execute("SELECT pg_try_advisory_lock(%s)", (self._key,)).fetchone()[0]
        except BaseException:
            conn.close()
            raise
        if not acquired:
            conn.close()
            return False

        self._conn = conn
        self._start_time = time.time()
        log.debug("Lock acquired", extra={"lock": self.lock_id, "wait_ms": 0})
        return True

    def release(self):
        """Release the lock by closing its connection.

        Raises:
            InvalidStateError: If the lock hasn't been acquired.
        """
        if not self._conn:
            raise InvalidStateError("Lock is not acquired")

        if self._conn.closed:
            log.warning("Lock connection died while held; mutual exclusion may have been broken",
                        extra={"lock": self.lock_id})
        self._conn.close()
        self._conn = None

        lock_time_ms = (time.time() - self._start_time) * 1000
        log.debug("Lock released", extra={"lock": self.lock_id, "locked_ms": round(lock_time_ms, 2)})

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


class AdvisoryLockProvider:
    """Postgres-backed LockProvider using session-level advisory locks.

    Locks are server-held, so every process connected to the environment's database
    contends on the same keys — cross-machine coordination with no shared filesystem.
    Each held lock pins its own dedicated connection, deliberately separate from the
    environment pool: the session is the lock owner, and pooled connections are shared,
    reused, and not cleared of advisory locks on return.
    """

    def __init__(self, dsn: str, env_id: str, *, timeout=10):
        self._dsn = dsn
        self._env_id = env_id
        self._timeout = timeout

    def lock(self, lock_id: str) -> AdvisoryLock:
        return AdvisoryLock(self._dsn, lock_id, _advisory_key(self._env_id, lock_id), timeout=self._timeout)


def create_lock_provider(entry, *, timeout=10) -> AdvisoryLockProvider:
    """Create the advisory-lock LockProvider for an environment entry."""
    return AdvisoryLockProvider(_dsn(entry), entry.id, timeout=timeout)


# --- Dialect: Postgres uses %s placeholders and aware-UTC timestamps ---

def _bind_dt(dt):
    """Domain datetimes are naive UTC; bind them to TIMESTAMPTZ as explicit UTC."""
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


def _read_dt(dt):
    """Read a TIMESTAMPTZ back as the naive UTC the domain uses."""
    return dt.astimezone(timezone.utc).replace(tzinfo=None) if dt is not None else None


_PG_DIALECT = Dialect(placeholder="%s", bind_dt=_bind_dt)


# --- Row mapping (JSONB columns arrive already deserialized) ---

_SELECT_RUNS = (
    "SELECT h.*, "
    "(SELECT json_agg(t.tag) FROM run_tags t "
    " WHERE t.job_id = h.job_id AND t.run_id = h.run_id AND t.ordinal = h.ordinal) AS tags "
    "FROM runs h"
)

_RUN_UPDATE_SQL = (
    "UPDATE runs SET run=%s, created=%s, started=%s, ended=%s, exec_time=%s, termination_status=%s, "
    "warnings=%s, state_updated_at=%s, updated_at=%s "
    "WHERE job_id=%s AND run_id=%s AND ordinal=%s"
)


def _to_metadata(row) -> JobInstanceMetadata:
    """Identity metadata for init-only rows (no ``run`` document yet): PK fields + tags — the
    fields criteria can match. Params/features live only in the document."""
    return JobInstanceMetadata(
        InstanceID(row['job_id'], row['run_id'], row['ordinal']),
        {},
        tags=tuple(row['tags'] or ()),
    )


def _to_job_run(row) -> JobRun:
    """Deserialize the whole-run document of a runs row (see the document + projections note
    on the runs DDL). JSONB reads back as a Python dict."""
    return JobRun.deserialize(row['run'])


def _run_update_params(r: JobRun):
    """SET values plus (job_id, run_id, ordinal) keys for :data:`_RUN_UPDATE_SQL`.

    The whole run is stored as one document; the discrete values are query projections
    derived from it (criteria SQL, sorts, stats) and must never be read back into a JobRun.
    """
    term = r.lifecycle.termination
    return (
        Jsonb(r.serialize()),
        _bind_dt(r.lifecycle.created_at),
        _bind_dt(r.lifecycle.started_at) if r.lifecycle.started_at else None,
        _bind_dt(term.terminated_at) if term else None,
        round(r.lifecycle.total_run_time.total_seconds(), 3) if r.lifecycle.total_run_time else None,
        term.status.value if term else None,
        len(r.status.warnings) if r.status else None,
        _bind_dt(r.last_updated),  # state_updated_at: domain freshness (newer-wins guard)
        _bind_dt(utc_now()),       # updated_at: row write time
        r.metadata.job_id,
        r.metadata.run_id,
        r.metadata.ordinal,
    )


def _order_by(sort: SortOption, asc: bool) -> str:
    # TIME sorts by the stored exec_time; PK tiebreaker keeps paging stable.
    return build_order_by(sort, asc, time_expr="h.exec_time",
                          tiebreaker=("h.job_id", "h.run_id", "h.ordinal"), nulls_last=True)


def _status_csv(*outcomes) -> str:
    """Comma-separated termination_status codes for the given outcomes (literal ints, no params)."""
    return ', '.join(str(s.value) for s in TerminationStatus.get_statuses(*outcomes))


def ensure_open(f):
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        if self._pool is None:
            raise InvalidStateError("Database connection not opened")
        return f(self, *args, **kwargs)

    return wrapper


_SCHEMA_DDL = f"""
-- Document + projections: `run` holds the whole serialized JobRun (the wire format is the
-- storage format; NULL = init-only row); the discrete columns are query projections derived
-- from it at write (criteria SQL, sorts, stats) plus the persistence machinery's own
-- timestamps. Never read a JobRun from projections.
CREATE TABLE IF NOT EXISTS runs (
    job_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL DEFAULT 1,
    run JSONB,
    created TIMESTAMPTZ NOT NULL,
    started TIMESTAMPTZ,
    ended TIMESTAMPTZ,
    exec_time DOUBLE PRECISION,
    termination_status INTEGER,
    warnings INTEGER,
    state_updated_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL,
    heartbeat_at TIMESTAMPTZ,
    PRIMARY KEY (job_id, run_id, ordinal)
);
CREATE INDEX IF NOT EXISTS runs_ended_idx ON runs (ended);
CREATE INDEX IF NOT EXISTS runs_created_idx ON runs (created);
CREATE INDEX IF NOT EXISTS runs_exec_time_idx ON runs (exec_time);
CREATE INDEX IF NOT EXISTS runs_job_ended_idx ON runs (job_id, ended DESC) WHERE ended IS NOT NULL;
CREATE TABLE IF NOT EXISTS run_tags (
    job_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    tag TEXT NOT NULL CHECK (length(tag) BETWEEN 1 AND 64),
    PRIMARY KEY (job_id, run_id, ordinal, tag),
    FOREIGN KEY (job_id, run_id, ordinal) REFERENCES runs (job_id, run_id, ordinal) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS run_tags_tag_idx ON run_tags (tag);
CREATE TABLE IF NOT EXISTS signals (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    phase_id TEXT,
    op TEXT NOT NULL,
    args JSONB,
    requested_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS signals_instance_idx ON signals (job_id, run_id, ordinal);
-- UNLOGGED: the tail is expendable cache (WAL for state, none for cache) — crash recovery
-- truncates it and it refills from the still-running instances' ongoing appends
CREATE UNLOGGED TABLE IF NOT EXISTS output_tail (
    job_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    line_ordinal BIGINT NOT NULL,
    line JSONB NOT NULL,
    PRIMARY KEY (job_id, run_id, ordinal, line_ordinal)
);
CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value JSONB NOT NULL);
CREATE TABLE IF NOT EXISTS schema_info (version INTEGER NOT NULL);
"""


class PostgreSQL(EnvironmentDatabase):

    def __init__(self, dsn: str, env_id: str, max_size: int = DEFAULT_POOL_MAX_SIZE):
        self._dsn = dsn
        self._env_id = env_id
        self._schema = _schema_name(env_id)
        self._max_size = max_size
        self._pool: ConnectionPool | None = None

    def _configure(self, conn):
        # Every pooled connection resolves unqualified names in this env's schema only — so all the
        # storage SQL stays schema-agnostic. (Timezone is pinned via the connection options.)
        # Session-level SET persists past the commit; the commit is required so the pool doesn't
        # discard the connection for being left in a transaction.
        conn.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(self._schema)))
        conn.commit()

    def _open_pool(self):
        if self._pool is not None:
            raise InvalidStateError("Database connection already opened")
        # Pin the session to UTC so naive-UTC binds and TIMESTAMPTZ comparisons stay consistent.
        pool = ConnectionPool(self._dsn, min_size=1, max_size=self._max_size, open=False,
                              kwargs={"options": "-c timezone=UTC"}, configure=self._configure)
        pool.open(wait=True)
        self._pool = pool

    @override
    def open(self):
        """Open the pool and validate the schema. Never issues DDL — see :func:`create_environment`
        for the privileged provisioning path. Raises if the environment is not yet provisioned."""
        self._open_pool()
        try:
            self._validate_schema()
        except BaseException:
            self.close()  # don't leak the pool on a failed open
            raise

    def is_open(self):
        return self._pool is not None

    @ensure_open
    def _validate_schema(self):
        """Confirm the env's schema is provisioned and at the expected version. Read-only."""
        with self._pool.connection() as conn, conn.cursor() as cur:
            if not _schema_present(conn, self._schema):
                raise EnvironmentStoreNotProvisionedError(self._env_id)
            cur.execute("SELECT version FROM schema_info")
            row = cur.fetchone()
            version = row[0] if row else None
            if version != SCHEMA_VERSION:
                raise IncompatibleSchemaError(version, SCHEMA_VERSION)

    @ensure_open
    def _provision(self):
        """Create the env's schema + tables (privileged DDL). Idempotent: a no-op if already at the
        current version, an error if a different version is present."""
        with self._pool.connection() as conn, conn.cursor() as cur:
            # Must precede table DDL; search_path already points here, so tables land in-schema.
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(self._schema)))
            if _schema_present(conn, self._schema):
                cur.execute("SELECT version FROM schema_info")
                row = cur.fetchone()
                version = row[0] if row else None
                if version != SCHEMA_VERSION:
                    raise IncompatibleSchemaError(version, SCHEMA_VERSION)
                return
            cur.execute(_SCHEMA_DDL)
            cur.execute("INSERT INTO schema_info (version) VALUES (%s)", (SCHEMA_VERSION,))
            log.debug("Schema provisioned")

    @override
    def close(self):
        if self._pool is not None:
            self._pool.close()
            self._pool = None

    def _fetch(self, query, params):
        with self._pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, params)
            return cur.fetchall()

    def _matching_pks(self, where, params, run_match):
        """(job_id, run_id, ordinal) of rows satisfying the full ``run_match`` — for criteria the
        SQL ``where`` prefilter can't express (phase, PARTIAL/FN_MATCH)."""
        return matching_pks(self._fetch(_SELECT_RUNS + where, params), run_match, _to_job_run, _to_metadata)

    @override
    @ensure_open
    def init_run(self, job_id, run_id, user_params=None, *,
                 created_at, tags=(), auto_increment=False, max_retries=5):
        """See `RunStorage.init_run`."""
        created = _bind_dt(created_at)
        normalized_tags = normalize_tags(tags) if tags else ()

        if not auto_increment:
            try:
                self._insert_run(job_id, run_id, 1, created, normalized_tags)
            except UniqueViolation:
                raise DuplicateInstanceError(InstanceID(job_id, run_id, 1))
            return InstanceID(job_id, run_id, 1)

        for _ in range(max_retries):
            with self._pool.connection() as conn, conn.cursor() as cur:
                cur.execute("SELECT COALESCE(MAX(ordinal), 0) + 1 FROM runs WHERE job_id = %s AND run_id = %s",
                            (job_id, run_id))
                ordinal = cur.fetchone()[0]
            try:
                self._insert_run(job_id, run_id, ordinal, created, normalized_tags)
            except UniqueViolation:
                continue  # Lost the race for this ordinal — recompute and retry
            return InstanceID(job_id, run_id, ordinal)
        raise RuntimeError(f"Failed to allocate ordinal for ({job_id}, {run_id}) after {max_retries} retries")

    def _insert_run(self, job_id, run_id, ordinal, created, tags):
        with self._pool.connection() as conn, conn.cursor() as cur:
            # heartbeat_at is written in *server* time — heartbeat ages are computed against now()
            # at read, so one clock measures both edges and node/consumer skew cancels
            cur.execute(
                "INSERT INTO runs (job_id, run_id, ordinal, created, updated_at, heartbeat_at) "
                "VALUES (%s, %s, %s, %s, %s, now())",
                (job_id, run_id, ordinal, created, created))
            if tags:
                cur.executemany(
                    "INSERT INTO run_tags (job_id, run_id, ordinal, tag) VALUES (%s, %s, %s, %s)",
                    [(job_id, run_id, ordinal, t) for t in tags])

    @override
    @ensure_open
    def read_runs(self, run_match=None, sort=SortOption.ENDED, *,
                  asc=True, limit=-1, offset=-1, last=False) -> list[JobRun]:
        """See `RunStorage.read_runs`."""
        return list(self.iter_runs(run_match, sort, asc=asc, limit=limit, offset=offset, last=last))

    @override
    @ensure_open
    def iter_runs(self, run_match=None, sort=SortOption.ENDED, *,
                  asc=True, limit=-1, offset=-1, last=False) -> Iterator[JobRun]:
        """See `RunStorage.iter_runs`.

        Without a match, SQL paginates (and picks last-per-job) directly. With a match, the rows
        are post-filtered with the full ``run_match`` first — so ``last`` chooses the newest run
        *among the matching ones* (not the newest overall, which might not match) — and offset/limit
        then apply to that stream.
        """
        where, params, _ = build_where_clause(run_match, _PG_DIALECT, alias='h')  # read always post-filters
        where = (where + " AND h.ended IS NOT NULL") if where else " WHERE h.ended IS NOT NULL"

        user_offset = offset if offset >= 0 else 0
        user_limit = limit if limit >= 0 else None

        if run_match is None:
            if last:
                where += " AND " + LAST_PER_JOB_SQL
            sql = _SELECT_RUNS + where + " ORDER BY " + _order_by(sort, asc)
            if user_limit is not None:
                sql += " LIMIT %s OFFSET %s"
                params = params + [user_limit, user_offset]
            elif user_offset:
                sql += " OFFSET %s"
                params = params + [user_offset]
            return iter([_to_job_run(row) for row in self._fetch(sql, params)])

        # The full run_match is the authority: fetch matches, then reduce/paginate in order.
        sql = _SELECT_RUNS + where + " ORDER BY " + _order_by(sort, asc)
        matched = [run for run in (_to_job_run(row) for row in self._fetch(sql, params)) if run_match(run)]
        if last:
            keep = last_run_ids(matched)
            matched = [run for run in matched if run.metadata.instance_id in keep]  # keeps sort order
        end = None if user_limit is None else user_offset + user_limit
        return iter(matched[user_offset:end])

    @override
    @ensure_open
    def read_active_runs(self, run_match=None) -> list[JobRun]:
        """See `RunStorage.read_active_runs`."""
        where, params, _ = build_where_clause(run_match, _PG_DIALECT, alias='h')  # post-filtered below
        # ended IS NULL → active; run IS NOT NULL → the persister has written a real
        # snapshot, so skip init-only rows that _to_job_run cannot deserialize.
        guard = "h.ended IS NULL AND h.run IS NOT NULL"
        sql = _SELECT_RUNS + ((where + " AND " + guard) if where else (" WHERE " + guard))
        runs = [_to_job_run(row) for row in self._fetch(sql, params)]
        # Re-apply the full criteria for predicates SQL can't express (e.g. phase).
        return [run for run in runs if run_match(run)] if run_match else runs

    @override
    @ensure_open
    def read_instance_ids(self, run_match=None):
        """See `RunStorage.read_instance_ids`."""
        where, params, complete = build_where_clause(run_match, _PG_DIALECT, alias='h')
        if complete:
            rows = self._fetch("SELECT h.job_id, h.run_id, h.ordinal FROM runs h" + where, params)
            return [InstanceID(r['job_id'], r['run_id'], r['ordinal']) for r in rows]
        return [InstanceID(*pk) for pk in self._matching_pks(where, params, run_match)]

    @override
    @ensure_open
    def active_run_versions(self) -> List[RunVersion]:
        """See `RunStorage.active_run_versions`."""
        # updated_at::text → opaque str cursor; a bare TIMESTAMPTZ would read back as a datetime.
        # See RunVersion for why write-time is the cursor and the invariant it relies on.
        rows = self._fetch(
            "SELECT job_id, run_id, ordinal, updated_at::text AS version, "
            "EXTRACT(EPOCH FROM now() - heartbeat_at)::float AS heartbeat_age FROM runs "
            "WHERE ended IS NULL AND run IS NOT NULL", ())
        return [RunVersion(InstanceID(r['job_id'], r['run_id'], r['ordinal']), r['version'], r['heartbeat_age'])
                for r in rows]

    @override
    @ensure_open
    def touch_heartbeats(self, instance_ids):
        """See `RunStorage.touch_heartbeats`."""
        ids = [(i.job_id, i.run_id, i.ordinal) for i in instance_ids]
        if not ids:
            return
        # Server time, matching the read-side now() age computation — see _insert_run
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.executemany(
                "UPDATE runs SET heartbeat_at = now() "
                "WHERE job_id = %s AND run_id = %s AND ordinal = %s AND ended IS NULL",
                ids)

    @override
    @ensure_open
    def send_signal(self, instance_id, op, *, phase_id=None, args=()):
        """See `SignalSender.send_signal`."""
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO signals (job_id, run_id, ordinal, phase_id, op, args, requested_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, now())",
                (instance_id.job_id, instance_id.run_id, instance_id.ordinal, phase_id, op,
                 Jsonb(list(args)) if args else None))

    @override
    @ensure_open
    def read_signals(self, instance_ids=None, *, older_than=None):
        """See `SignalStorage.read_signals`."""
        conditions, params = [], []
        if instance_ids is not None:
            ids = [(i.job_id, i.run_id, i.ordinal) for i in instance_ids]
            if not ids:
                return []
            conditions.append("(job_id, run_id, ordinal) IN (" + ", ".join(["(%s, %s, %s)"] * len(ids)) + ")")
            params += [value for iid in ids for value in iid]
        if older_than is not None:
            conditions.append("requested_at < now() - make_interval(secs => %s)")
            params.append(older_than)
        where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

        rows = self._fetch(f"SELECT * FROM signals{where} ORDER BY id", params)
        return [Signal(r['id'], InstanceID(r['job_id'], r['run_id'], r['ordinal']), r['phase_id'], r['op'],
                       tuple(r['args'] or ()), _read_dt(r['requested_at']))
                for r in rows]

    @override
    @ensure_open
    def delete_signals(self, signal_ids):
        """See `SignalStorage.delete_signals`."""
        ids = list(signal_ids)
        if not ids:
            return
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.executemany("DELETE FROM signals WHERE id = %s", [(i,) for i in ids])

    @override
    @ensure_open
    def append_output(self, lines):
        """See `OutputTailStorage.append_output`."""
        rows = [(iid.job_id, iid.run_id, iid.ordinal, line.ordinal, Jsonb(line.serialize()))
                for iid, line in lines]
        if not rows:
            return
        # DO NOTHING: a batch re-flushed after a failed write must be harmless (idempotent appends)
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO output_tail (job_id, run_id, ordinal, line_ordinal, line) "
                "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", rows)

    @override
    @ensure_open
    def prune_output_tail(self, instance_id, keep):
        """See `OutputTailStorage.prune_output_tail`."""
        iid = (instance_id.job_id, instance_id.run_id, instance_id.ordinal)
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(
                "DELETE FROM output_tail WHERE job_id = %s AND run_id = %s AND ordinal = %s"
                " AND line_ordinal NOT IN ("
                " SELECT line_ordinal FROM output_tail WHERE job_id = %s AND run_id = %s AND ordinal = %s"
                " ORDER BY line_ordinal DESC LIMIT %s)",
                iid + iid + (keep,))

    @override
    @ensure_open
    def delete_output_tails(self, instance_ids):
        """See `OutputTailStorage.delete_output_tails`."""
        ids = [(i.job_id, i.run_id, i.ordinal) for i in instance_ids]
        if not ids:
            return
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.executemany("DELETE FROM output_tail WHERE job_id = %s AND run_id = %s AND ordinal = %s", ids)

    @override
    @ensure_open
    def output_tail_instances(self):
        """See `OutputTailStorage.output_tail_instances`."""
        rows = self._fetch("SELECT DISTINCT job_id, run_id, ordinal FROM output_tail", ())
        return [InstanceID(r['job_id'], r['run_id'], r['ordinal']) for r in rows]

    @override
    @ensure_open
    def read_output_tail(self, instance_id, max_lines, *, after_ordinal=None):
        """See `OutputTailReader.read_output_tail`."""
        iid = (instance_id.job_id, instance_id.run_id, instance_id.ordinal)
        max_lines = max_lines if max_lines > 0 else None  # postgres: LIMIT NULL = unlimited
        if after_ordinal is None:
            rows = self._fetch(
                "SELECT line FROM output_tail WHERE job_id = %s AND run_id = %s AND ordinal = %s"
                " ORDER BY line_ordinal DESC LIMIT %s", iid + (max_lines,))
            rows = list(reversed(rows))  # newest N, returned oldest first
        else:
            rows = self._fetch(
                "SELECT line FROM output_tail WHERE job_id = %s AND run_id = %s AND ordinal = %s"
                " AND line_ordinal > %s ORDER BY line_ordinal LIMIT %s", iid + (after_ordinal, max_lines))
        return [OutputLine.deserialize(r['line']) for r in rows]

    @override
    @ensure_open
    def read_run_stats(self, run_match=None) -> List[JobStats]:
        """See `RunStorage.read_run_stats`."""
        where, params, complete = build_where_clause(run_match, _PG_DIALECT, alias='h')
        where = (where + " AND h.ended IS NOT NULL") if where else " WHERE h.ended IS NOT NULL"
        if not complete:
            # Criteria SQL can't express (phase, PARTIAL/FN_MATCH): aggregate only the rows that
            # match the full criteria, otherwise unrelated runs would be folded into the stats.
            pks = self._matching_pks(where, params, run_match)
            if not pks:
                return []
            where = " WHERE (h.job_id, h.run_id, h.ordinal) IN (" + ", ".join(["(%s, %s, %s)"] * len(pks)) + ")"
            params = [value for pk in pks for value in pk]
        sql = f"""
            WITH filtered AS (
                SELECT * FROM runs h{where}
            ),
            last_per_job AS (
                SELECT DISTINCT ON (job_id)
                       job_id, exec_time AS last_time,
                       termination_status AS last_term_status, warnings AS last_warnings
                FROM filtered
                ORDER BY job_id, ended DESC, run_id DESC, ordinal DESC
            )
            SELECT
                f.job_id,
                COUNT(*) AS count,
                MIN(f.created) AS first_created,
                MAX(f.created) AS last_created,
                MIN(f.exec_time) AS fastest_time,
                AVG(f.exec_time) AS average_time,
                MAX(f.exec_time) AS slowest_time,
                lp.last_time,
                lp.last_term_status,
                COUNT(*) FILTER (WHERE f.termination_status IN ({_status_csv(Outcome.SUCCESS)})) AS succeeded,
                COUNT(*) FILTER (WHERE f.termination_status IN ({_status_csv(Outcome.FAULT)})) AS failed,
                COUNT(*) FILTER (WHERE f.termination_status IN ({_status_csv(Outcome.ABORTED)})) AS aborted,
                COUNT(*) FILTER (WHERE f.termination_status IN ({_status_csv(Outcome.REJECTED)})) AS rejected,
                COUNT(*) FILTER (WHERE f.termination_status IN ({_status_csv(Outcome.IGNORED)})) AS ignored,
                lp.last_warnings
            FROM filtered f
            JOIN last_per_job lp ON f.job_id = lp.job_id
            GROUP BY f.job_id, lp.last_time, lp.last_term_status, lp.last_warnings
        """

        return [build_job_stats(row, _read_dt) for row in self._fetch(sql, params)]

    @override
    @ensure_open
    def store_runs(self, *job_runs):
        """See `RunStorage.store_runs`. Tags are immutable; ``init_run`` is their sole writer."""
        with self._pool.connection() as conn, conn.cursor() as cur:
            for run in job_runs:
                cur.execute(_RUN_UPDATE_SQL, _run_update_params(run))
                if cur.rowcount == 0:
                    log.warning("No init row found instance=%s", run.metadata.instance_id,
                                extra={"instance": str(run.metadata.instance_id)})

    @override
    @ensure_open
    def store_active_runs(self, *job_runs):
        """Update active snapshots without regressing terminal or newer state.

        Args:
            *job_runs: Latest active snapshots to persist.

        Notes:
            The SQL predicate enforces the storage invariant: terminal rows are
            left untouched, and active rows are updated only when the candidate's
            domain freshness is at least as new as the stored snapshot. Under
            the single-writer model this should normally accept every candidate;
            a skipped row indicates a stale retry or an ended run.
        """
        with self._pool.connection() as conn, conn.cursor() as cur:
            for run in job_runs:
                cur.execute(
                    _RUN_UPDATE_SQL + " AND ended IS NULL"
                    " AND (state_updated_at IS NULL OR state_updated_at <= %s)",
                    _run_update_params(run) + (_bind_dt(run.last_updated),))

    @override
    @ensure_open
    def remove_runs(self, run_match):
        """See `RunStorage.remove_runs`.

        When the criteria are fully SQL-expressed, delete directly. Otherwise (phase or
        PARTIAL/FN_MATCH criteria) apply the full matcher to the candidates first — deleting from
        the prefilter alone would remove rows the criteria don't actually select.
        """
        where, params, complete = build_where_clause(run_match, _PG_DIALECT)
        if complete:
            if not where:
                raise ValueError("No rows to remove")
            with self._pool.connection() as conn, conn.cursor() as cur:
                cur.execute("DELETE FROM runs" + where + " RETURNING job_id, run_id, ordinal", params)
                return [InstanceID(job_id=r[0], run_id=r[1], ordinal=r[2]) for r in cur.fetchall()]

        # Criteria SQL can't express: delete only candidates that match the full criteria.
        where, params, _ = build_where_clause(run_match, _PG_DIALECT, alias='h')
        ids = self._matching_pks(where, params, run_match)
        if not ids:
            return []
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.executemany("DELETE FROM runs WHERE job_id = %s AND run_id = %s AND ordinal = %s", ids)
        return [InstanceID(job_id=j, run_id=r, ordinal=o) for j, r, o in ids]

    @override
    @ensure_open
    def load_config(self, env_id: str) -> dict:
        """Load environment config as a dict with parsed JSON values."""
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT key, value FROM config")
            return dict(cur.fetchall())  # JSONB already deserialized

    @override
    @ensure_open
    def save_config(self, env_id: str, config: dict):
        """Replace all config from a dict of non-default settings."""
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM config")
            for key, value in config.items():
                cur.execute("INSERT INTO config (key, value) VALUES (%s, %s)", (key, Jsonb(value)))
