"""
SQLite implementation of EnvironmentDatabase. Intended for in-process and development use;
networked/concurrent deployments use the Postgres driver.
"""

import json
import logging
import sqlite3
from functools import wraps
from pathlib import Path
from threading import Lock
from typing import List, Iterator, override

from runtools.runcore import paths
from runtools.runcore.db import EnvironmentDatabase, IncompatibleSchemaError, RunVersion, Signal
from runtools.runcore.db.sql import (build_job_stats, build_order_by, build_where_clause, Dialect, last_run_ids,
                                     LAST_PER_JOB_SQL, matching_pks)
from runtools.runcore.err import InvalidStateError
from runtools.runcore.job import (JobStats, JobRun, JobInstanceMetadata, InstanceID,
                                  DuplicateInstanceError, normalize_tags)
from runtools.runcore.matching import SortOption
from runtools.runcore.run import TerminationStatus, Outcome
from runtools.runcore.util import format_dt_sql, parse_dt_sql, utc_now

log = logging.getLogger(__name__)

SCHEMA_VERSION = 2


def _resolve_path(entry) -> Path:
    """Resolve the database file path from an EnvironmentEntry."""
    return Path(paths.expand_user(entry.location)) if entry.location else paths.sqlite_db_path(entry.id)


def exists(entry) -> bool:
    """Check if the SQLite database file exists."""
    return _resolve_path(entry).exists()


def delete(entry) -> None:
    """Delete the SQLite database file if it exists."""
    db_file = _resolve_path(entry)
    if db_file.exists():
        db_file.unlink()


def create_environment(entry, config) -> None:
    """Create a new environment: provision backing store, init schema, and seed config.

    Args:
        entry: EnvironmentEntry describing the database location.
        config: EnvironmentConfig to seed as the initial configuration.
    """
    _resolve_path(entry).parent.mkdir(parents=True, exist_ok=True)
    with create(entry) as db:
        db.save_config(entry.id, config.model_dump(mode='json'))


def create(entry, **kwargs):
    """
    Creates SQLite persistence with configurable connection parameters.

    Args:
        entry: EnvironmentEntry describing the database location.
        **kwargs: Any valid keyword arguments for sqlite3.connect()
            Common options include:
            - timeout: Float timeout value in seconds (default: 5.0)
            - detect_types: Control whether string or binary is converted to SQLite types (default: 0)
            - isolation_level: Sets transaction isolation level (default: 'DEFERRED')
            - cached_statements: Number of statements to cache (default: 128)
            - uri: True if database parameter is a URI (default: False)
            - autocommit: Transaction control mode (default: sqlite3.LEGACY_TRANSACTION_CONTROL)

    Returns:
        SQLite: Configured SQLite persistence instance
    """
    kwargs['check_same_thread'] = False
    resolved = str(_resolve_path(entry))

    return SQLite(lambda: sqlite3.connect(resolved, **kwargs))


def create_memory(env_id: str, **kwargs) -> 'SQLite':
    """Create an in-memory SQLite database for testing."""
    kwargs['check_same_thread'] = False
    return SQLite(lambda: sqlite3.connect(':memory:', **kwargs))


def ensure_open(f):
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        with self._conn_lock:
            if not self._conn:
                raise InvalidStateError("Database connection not opened")
            return f(self, *args, **kwargs)

    return wrapper


_SQLITE_DIALECT = Dialect(placeholder="?", bind_dt=format_dt_sql)


def _build_where_clause(run_match, alias=""):
    return build_where_clause(run_match, _SQLITE_DIALECT, alias)


def _to_metadata(r) -> JobInstanceMetadata:
    """Identity metadata for init-only rows (no ``run`` document yet): PK fields + tags — the
    fields criteria can match. Params/features live only in the document.

    Uses the optional ``tags`` synthetic column (a ``json_group_array(tag)`` over run_tags); rows
    without it read back as having no tags, so callers needing tag fidelity must include the
    subselect in their query.
    """
    tags_raw = r['tags'] if 'tags' in r.keys() else None
    return JobInstanceMetadata(
        InstanceID(r['job_id'], r['run_id'], r['ordinal']),
        {},
        tags=tuple(json.loads(tags_raw)) if tags_raw else (),
    )


def _to_job_run(r) -> JobRun:
    """Deserialize the whole-run document of a runs row (see the document + projections note
    on the runs DDL)."""
    return JobRun.deserialize(json.loads(r['run']))


_SELECT_RUNS = (
    "SELECT h.*, "
    "(SELECT json_group_array(tag) FROM run_tags t "
    " WHERE t.job_id = h.job_id AND t.run_id = h.run_id AND t.ordinal = h.ordinal) "
    "AS tags "
    "FROM runs h"
)


def _full_ts(dt):
    """Full-microsecond timestamp string for the freshness/version columns. ``format_dt_sql``
    truncates to milliseconds, which would collapse sub-ms-distinct writes: an older snapshot could
    compare equal and slip past the ``state_updated_at <= ?`` guard, and two rapid writes could
    share an ``updated_at`` cursor so a poller skips the deep read. Both columns therefore store all
    six fractional digits (lexicographic = chronological, since ``%f`` is fixed-width zero-padded).
    Never parsed back — comparator/cursor only."""
    return dt.strftime('%Y-%m-%d %H:%M:%S.%f')


_RUN_UPDATE_SQL = (
    "UPDATE runs SET run=?, created=?, started=?, ended=?, exec_time=?, termination_status=?, "
    "warnings=?, state_updated_at=?, updated_at=? "
    "WHERE job_id=? AND run_id=? AND ordinal=?"
)


def _run_update_values(r: JobRun):
    """SET-clause values plus the (job_id, run_id, ordinal) keys for :data:`_RUN_UPDATE_SQL`.

    The whole run is stored as one document; the discrete values are query projections
    derived from it (criteria SQL, sorts, stats) and must never be read back into a JobRun.
    """
    return (json.dumps(r.serialize()),
            format_dt_sql(r.lifecycle.created_at),
            format_dt_sql(r.lifecycle.started_at) if r.lifecycle.started_at else None,
            format_dt_sql(r.lifecycle.termination.terminated_at) if r.lifecycle.termination else None,
            round(r.lifecycle.total_run_time.total_seconds(), 3) if r.lifecycle.total_run_time else None,
            r.lifecycle.termination.status.value if r.lifecycle.termination else None,
            len(r.status.warnings) if r.status else None,
            _full_ts(r.last_updated),  # state_updated_at: domain freshness (newer-wins guard)
            _full_ts(utc_now()),       # updated_at: write-time cursor (changes on every accepted write)
            r.metadata.job_id,
            r.metadata.run_id,
            r.metadata.ordinal,
            )


class SQLite(EnvironmentDatabase):

    def __init__(self, connection_factory):
        """
        Args:
            connection_factory: Callable that returns a sqlite3.Connection
        """
        self._connection_factory = connection_factory
        self._conn = None
        self._conn_lock = Lock()

    @override
    def open(self):
        with self._conn_lock:
            if self._conn is not None:
                raise InvalidStateError("Database connection already opened")
            self._conn = self._connection_factory()
            # Required for `ON DELETE CASCADE` on run_tags. SQLite has FKs off by
            # default and the pragma is per-connection, so set it on every open.
            self._conn.execute('PRAGMA foreign_keys = ON')
        self._init_schema()

    def is_open(self):
        return self._conn is not None

    @ensure_open
    def _init_schema(self):
        c = self._conn.cursor()
        c.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='runs'")
        if c.fetchone()[0] == 1:
            version = c.execute('PRAGMA user_version').fetchone()[0]
            if version != SCHEMA_VERSION:
                raise IncompatibleSchemaError(version, SCHEMA_VERSION)
            c.close()
            return

        # Fresh schema — document + projections: `run` holds the whole serialized JobRun (the wire
        # format is the storage format; NULL = init-only row); the discrete columns are query
        # projections derived from it at write (criteria SQL, sorts, stats) plus the persistence
        # machinery's own timestamps. Never read a JobRun from projections.
        c.execute('''CREATE TABLE runs (
                     job_id TEXT NOT NULL,
                     run_id TEXT NOT NULL,
                     ordinal INTEGER NOT NULL DEFAULT 1,
                     run TEXT CHECK (run IS NULL OR json_valid(run)),
                     created TIMESTAMP NOT NULL,
                     started TIMESTAMP,
                     ended TIMESTAMP,
                     exec_time REAL,
                     termination_status INT,
                     warnings INT,
                     state_updated_at TIMESTAMP,
                     updated_at TIMESTAMP NOT NULL,
                     heartbeat_at TIMESTAMP,
                     PRIMARY KEY (job_id, run_id, ordinal)
                     )
                     ''')
        # No standalone runs_job_id_idx — the PRIMARY KEY (job_id, run_id, ordinal)
        # already provides a left-prefix index on job_id.
        c.execute('CREATE INDEX runs_ended_idx ON runs (ended)')
        c.execute('CREATE INDEX runs_created_idx ON runs (created)')
        c.execute('CREATE INDEX runs_exec_time_idx ON runs (exec_time)')
        # Speeds up the dashboard's "last completed run per job" query — partial
        # so the index ignores not-yet-ended rows.
        c.execute('CREATE INDEX runs_job_ended_idx '
                  'ON runs (job_id, ended DESC) WHERE ended IS NOT NULL')

        c.execute('''CREATE TABLE run_tags (
                     job_id TEXT NOT NULL,
                     run_id TEXT NOT NULL,
                     ordinal INTEGER NOT NULL,
                     tag TEXT NOT NULL CHECK (length(tag) BETWEEN 1 AND 64),
                     PRIMARY KEY (job_id, run_id, ordinal, tag),
                     FOREIGN KEY (job_id, run_id, ordinal)
                         REFERENCES runs (job_id, run_id, ordinal) ON DELETE CASCADE
                     )
                     ''')
        c.execute('CREATE INDEX run_tags_tag_idx ON run_tags (tag)')

        c.execute('''CREATE TABLE signals (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     job_id TEXT NOT NULL,
                     run_id TEXT NOT NULL,
                     ordinal INTEGER NOT NULL,
                     phase_id TEXT,
                     op TEXT NOT NULL,
                     args TEXT CHECK (args IS NULL OR json_valid(args)),
                     requested_by TEXT,
                     requested_at TIMESTAMP NOT NULL
                     )
                     ''')
        c.execute('CREATE INDEX signals_instance_idx ON signals (job_id, run_id, ordinal)')

        c.execute('CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
        c.execute(f'PRAGMA user_version = {SCHEMA_VERSION}')
        log.debug("Schema created")
        self._conn.commit()
        c.close()

    @override
    @ensure_open
    def init_run(self, job_id, run_id, user_params=None, *,
                 created_at, tags=(), auto_increment=False, max_retries=5):
        """See `RunStorage.init_run`."""
        created_str = format_dt_sql(created_at)
        # Normalize once up-front. Idempotent — safe even if caller pre-normalized.
        normalized_tags = normalize_tags(tags) if tags else ()

        if not auto_increment:
            try:
                self._conn.execute(
                    "INSERT INTO runs (job_id, run_id, ordinal, created, updated_at, heartbeat_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (job_id, run_id, 1, created_str, created_str, created_str))
            except sqlite3.IntegrityError:
                raise DuplicateInstanceError(InstanceID(job_id, run_id, 1))
            self._insert_tags(job_id, run_id, 1, normalized_tags)
            self._conn.commit()
            return InstanceID(job_id, run_id, 1)

        cursor = self._conn.execute(
            "SELECT MAX(ordinal) FROM runs WHERE job_id = ? AND run_id = ?",
            (job_id, run_id))
        ordinal = (cursor.fetchone()[0] or 0) + 1
        for _ in range(max_retries):
            try:
                self._conn.execute(
                    "INSERT INTO runs (job_id, run_id, ordinal, created, updated_at, heartbeat_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (job_id, run_id, ordinal, created_str, created_str, created_str))
            except sqlite3.IntegrityError:
                ordinal += 1
                continue
            self._insert_tags(job_id, run_id, ordinal, normalized_tags)
            self._conn.commit()
            return InstanceID(job_id, run_id, ordinal)
        raise sqlite3.IntegrityError(
            f"Failed to allocate ordinal for ({job_id}, {run_id}) after {max_retries} retries")

    def _insert_tags(self, job_id, run_id, ordinal, tags):
        if not tags:
            return
        self._conn.executemany(
            "INSERT INTO run_tags (job_id, run_id, ordinal, tag) VALUES (?, ?, ?, ?)",
            [(job_id, run_id, ordinal, t) for t in tags])

    @override
    def read_runs(self, run_match=None, sort=SortOption.ENDED, *,
                  asc=True, limit=-1, offset=-1, last=False) -> list[JobRun]:
        """See `RunStorage.read_runs`."""
        return list(self.iter_runs(run_match, sort, asc=asc, limit=limit, offset=offset, last=last))

    @override
    def iter_runs(self, run_match=None, sort=SortOption.ENDED, *,
                  asc=True, limit=-1, offset=-1, last=False) -> Iterator[JobRun]:
        """See `RunStorage.iter_runs`.

        Without a match, last/offset/limit are pushed into SQL. With a match, the SQL clause is
        only a superset (``_build_where_clause`` can't express phase or PARTIAL/FN_MATCH), so the
        rows are post-filtered with the full ``run_match`` and then last/offset/limit apply to
        that result.
        """
        user_offset = offset if offset >= 0 else 0
        user_limit = limit if limit >= 0 else None

        if run_match is None:
            return iter(self._query_runs(None, sort, asc, last=last, limit=user_limit, offset=user_offset))

        runs = [run for run in self._query_runs(run_match, sort, asc) if run_match(run)]
        if last:
            keep = last_run_ids(runs)
            runs = [run for run in runs if run.metadata.instance_id in keep]  # keeps sort order
        end = None if user_limit is None else user_offset + user_limit
        return iter(runs[user_offset:end])

    @ensure_open
    def _query_runs(self, run_match, sort, asc, *, last=False, limit=None, offset=0) -> List[JobRun]:
        """Run one ended-rows-only history query and map the rows to JobRuns.

        ``last``/``limit``/``offset`` are SQL-applied here; the criteria path leaves them to the
        caller (it must post-filter first), passing only ``run_match``/``sort``/``asc``.
        """
        statement = _SELECT_RUNS
        where_clause, where_params, _ = _build_where_clause(run_match, alias='h')  # caller post-filters
        # Exclude incomplete (init-only) records
        statement += (where_clause + " AND h.ended IS NOT NULL") if where_clause else " WHERE h.ended IS NOT NULL"
        if last:
            statement += " AND " + LAST_PER_JOB_SQL

        # TIME sorts by computed duration; rowid tiebreaker keeps paging stable.
        statement += " ORDER BY " + build_order_by(
            sort, asc, time_expr="julianday(h.ended) - julianday(h.created)", tiebreaker=("h.rowid",))

        params = list(where_params)
        if limit is not None:
            statement += " LIMIT ? OFFSET ?"
            params += [limit, offset]
        elif offset:
            statement += " LIMIT -1 OFFSET ?"  # SQLite requires a LIMIT to use OFFSET
            params.append(offset)

        cursor = self._conn.cursor()
        cursor.row_factory = sqlite3.Row
        cursor.execute(statement, params)
        try:
            return [_to_job_run(row) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def _matching_pks(self, where_clause, where_params, run_match):
        """(job_id, run_id, ordinal) of rows satisfying the full ``run_match`` — for criteria the
        SQL ``where_clause`` can't express (phase, PARTIAL/FN_MATCH). Caller holds ``_conn_lock``."""
        cursor = self._conn.cursor()
        cursor.row_factory = sqlite3.Row
        cursor.execute(_SELECT_RUNS + where_clause, where_params)
        try:
            rows = cursor.fetchall()
        finally:
            cursor.close()
        return matching_pks(rows, run_match, _to_job_run, _to_metadata)

    @override
    @ensure_open
    def read_run_stats(self, run_match=None) -> List[JobStats]:
        """See `RunStorage.read_run_stats`."""

        where_clause, where_params, complete = _build_where_clause(run_match, alias='h')
        # Exclude incomplete (init-only) records
        where_clause = (where_clause + " AND h.ended IS NOT NULL") if where_clause else " WHERE h.ended IS NOT NULL"
        if not complete:
            # Criteria SQL can't express (phase, PARTIAL/FN_MATCH): aggregate only the rows that
            # match the full criteria, otherwise unrelated runs would be folded into the stats.
            pks = self._matching_pks(where_clause, where_params, run_match)
            if not pks:
                return []
            rows = ", ".join(["(?, ?, ?)"] * len(pks))
            where_clause = f" WHERE (h.job_id, h.run_id, h.ordinal) IN (VALUES {rows})"
            where_params = [value for pk in pks for value in pk]
        def outcome_placeholders(outcome):
            return ', '.join(str(s.value) for s in TerminationStatus.get_statuses(outcome))

        sql = f'''
            WITH filtered AS (
                SELECT rowid, * FROM runs h
                {where_clause}
            ),
            last_per_job AS (
                SELECT job_id, MAX(rowid) AS max_rowid
                FROM filtered
                GROUP BY job_id
            )
            SELECT
                f.job_id,
                COUNT(*) AS "count",
                min(f.created) AS "first_created",
                max(f.created) AS "last_created",
                min(f.exec_time) AS "fastest_time",
                avg(f.exec_time) AS "average_time",
                max(f.exec_time) AS "slowest_time",
                lh.exec_time AS "last_time",
                lh.termination_status AS "last_term_status",
                COUNT(CASE WHEN f.termination_status IN ({outcome_placeholders(Outcome.SUCCESS)}) THEN 1 END) AS succeeded,
                COUNT(CASE WHEN f.termination_status IN ({outcome_placeholders(Outcome.FAULT)}) THEN 1 END) AS failed,
                COUNT(CASE WHEN f.termination_status IN ({outcome_placeholders(Outcome.ABORTED)}) THEN 1 END) AS aborted,
                COUNT(CASE WHEN f.termination_status IN ({outcome_placeholders(Outcome.REJECTED)}) THEN 1 END) AS rejected,
                COUNT(CASE WHEN f.termination_status IN ({outcome_placeholders(Outcome.IGNORED)}) THEN 1 END) AS ignored,
                lh.warnings AS "last_warnings"
            FROM filtered f
            JOIN last_per_job lp ON f.job_id = lp.job_id
            JOIN filtered lh ON lh.rowid = lp.max_rowid
            GROUP BY f.job_id
        '''
        c = self._conn.cursor()
        c.row_factory = sqlite3.Row
        c.execute(sql, where_params)

        return [build_job_stats(row, parse_dt_sql) for row in c.fetchall()]

    @override
    @ensure_open
    def read_active_runs(self, run_match=None) -> list[JobRun]:
        """See `RunStorage.read_active_runs`."""
        statement = _SELECT_RUNS
        where_clause, where_params, _ = _build_where_clause(run_match, alias='h')  # post-filtered below
        # ended IS NULL → active; run IS NOT NULL → the persister has written a real
        # snapshot, so skip init-only rows that _to_job_run cannot deserialize.
        guard = "h.ended IS NULL AND h.run IS NOT NULL"
        statement += (where_clause + " AND " + guard) if where_clause else (" WHERE " + guard)

        cursor = self._conn.cursor()
        cursor.row_factory = sqlite3.Row
        cursor.execute(statement, where_params)
        try:
            runs = [_to_job_run(row) for row in cursor.fetchall()]
        finally:
            cursor.close()
        # _build_where_clause covers only metadata/lifecycle; re-apply the full criteria
        # here so predicates it can't express (e.g. phase) don't leak false positives.
        return [run for run in runs if run_match(run)] if run_match else runs

    @override
    @ensure_open
    def active_run_versions(self) -> List[RunVersion]:
        """See `RunStorage.active_run_versions`."""
        cursor = self._conn.cursor()
        cursor.row_factory = sqlite3.Row
        cursor.execute(
            "SELECT job_id, run_id, ordinal, updated_at, "
            "(julianday('now') - julianday(heartbeat_at)) * 86400.0 AS heartbeat_age FROM runs "
            "WHERE ended IS NULL AND run IS NOT NULL")
        try:
            return [RunVersion(InstanceID(r['job_id'], r['run_id'], r['ordinal']), r['updated_at'],
                               r['heartbeat_age'])
                    for r in cursor.fetchall()]
        finally:
            cursor.close()

    @override
    @ensure_open
    def touch_heartbeats(self, instance_ids):
        """See `RunStorage.touch_heartbeats`."""
        ids = [(i.job_id, i.run_id, i.ordinal) for i in instance_ids]
        if not ids:
            return
        heartbeat = format_dt_sql(utc_now())
        self._conn.executemany(
            "UPDATE runs SET heartbeat_at = ? WHERE job_id = ? AND run_id = ? AND ordinal = ? AND ended IS NULL",
            [(heartbeat, j, r, o) for j, r, o in ids])
        self._conn.commit()

    @override
    @ensure_open
    def write_signal(self, instance_id, op, *, phase_id=None, args=(), requested_by=None):
        """See `SignalStorage.write_signal`."""
        self._conn.execute(
            "INSERT INTO signals (job_id, run_id, ordinal, phase_id, op, args, requested_by, requested_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (instance_id.job_id, instance_id.run_id, instance_id.ordinal, phase_id, op,
             json.dumps(list(args)) if args else None, requested_by, format_dt_sql(utc_now())))
        self._conn.commit()

    @override
    @ensure_open
    def read_signals(self, instance_ids):
        """See `SignalStorage.read_signals`."""
        ids = [(i.job_id, i.run_id, i.ordinal) for i in instance_ids]
        if not ids:
            return []
        placeholders = ", ".join(["(?, ?, ?)"] * len(ids))
        cursor = self._conn.cursor()
        cursor.row_factory = sqlite3.Row
        cursor.execute(
            f"SELECT * FROM signals WHERE (job_id, run_id, ordinal) IN ({placeholders}) ORDER BY id",
            [value for iid in ids for value in iid])
        try:
            return [Signal(r['id'], InstanceID(r['job_id'], r['run_id'], r['ordinal']), r['phase_id'], r['op'],
                           tuple(json.loads(r['args'])) if r['args'] else (), r['requested_by'],
                           parse_dt_sql(r['requested_at']))
                    for r in cursor.fetchall()]
        finally:
            cursor.close()

    @override
    @ensure_open
    def delete_signals(self, signal_ids):
        """See `SignalStorage.delete_signals`."""
        ids = list(signal_ids)
        if not ids:
            return
        self._conn.executemany("DELETE FROM signals WHERE id = ?", [(i,) for i in ids])
        self._conn.commit()

    @override
    @ensure_open
    def store_runs(self, *job_runs):
        """See `RunStorage.store_runs`.

        Tags are NOT updated here — ``init_run`` is the sole writer of the
        run_tags junction. Tags are immutable through the run's lifecycle.
        """
        for run in job_runs:
            cursor = self._conn.execute(_RUN_UPDATE_SQL, _run_update_values(run))
            if cursor.rowcount == 0:
                log.warning("No init row found instance=%s", run.metadata.instance_id,
                            extra={"instance": str(run.metadata.instance_id)})
        self._conn.commit()

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
        for run in job_runs:
            self._conn.execute(
                _RUN_UPDATE_SQL + " AND ended IS NULL"
                " AND (state_updated_at IS NULL OR state_updated_at <= ?)",
                _run_update_values(run) + (_full_ts(run.last_updated),))
        self._conn.commit()

    @override
    @ensure_open
    def remove_runs(self, run_match):
        """See `RunStorage.remove_runs`.

        When the criteria are fully SQL-expressed, delete directly. Otherwise (phase or
        PARTIAL/FN_MATCH criteria) apply the full matcher to the candidates first — deleting from
        the prefilter alone would remove rows the criteria don't actually select.
        """
        where_clause, where_params, complete = _build_where_clause(run_match)
        if complete:
            if not where_clause:
                raise ValueError("No rows to remove")
            cursor = self._conn.execute(
                "DELETE FROM runs" + where_clause + " RETURNING job_id, run_id, ordinal", where_params)
            removed = [InstanceID(job_id=r[0], run_id=r[1], ordinal=r[2]) for r in cursor.fetchall()]
            self._conn.commit()
            return removed

        # Criteria SQL can't express: delete only candidates that match the full criteria.
        where_clause, where_params, _ = _build_where_clause(run_match, alias='h')
        ids = self._matching_pks(where_clause, where_params, run_match)
        if not ids:
            return []
        self._conn.executemany("DELETE FROM runs WHERE job_id = ? AND run_id = ? AND ordinal = ?", ids)
        self._conn.commit()
        return [InstanceID(job_id=j, run_id=r, ordinal=o) for j, r, o in ids]

    @override
    @ensure_open
    def load_config(self, env_id: str) -> dict:
        """Load environment config as a dict with parsed JSON values."""
        c = self._conn.execute("SELECT key, value FROM config")
        return {key: json.loads(value_json) for key, value_json in c.fetchall()}

    @override
    @ensure_open
    def save_config(self, env_id: str, config: dict):
        """Replace all config from a dict of non-default settings."""
        self._conn.execute("DELETE FROM config")
        for key, value in config.items():
            self._conn.execute(
                "INSERT INTO config (key, value) VALUES (?, ?)", (key, json.dumps(value)))
        self._conn.commit()

    @override
    def close(self):
        with self._conn_lock:
            if self._conn:
                self._conn.close()
                self._conn = None
