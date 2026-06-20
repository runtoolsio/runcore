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
from runtools.runcore.db import EnvironmentDatabase, IncompatibleSchemaError
from runtools.runcore.db.sql import build_job_stats, build_where_clause, Dialect, last_run_ids, LAST_PER_JOB_SQL
from runtools.runcore.err import InvalidStateError
from runtools.runcore.job import (JobStats, JobRun, JobInstanceMetadata, InstanceID, DuplicateInstanceError,
                                  normalize_tags)
from runtools.runcore.matching import SortOption
from runtools.runcore.output import OutputLocation
from runtools.runcore.retention import RetentionPolicy
from runtools.runcore.run import TerminationStatus, Outcome, PhaseRun, Fault
from runtools.runcore.status import Status
from runtools.runcore.util import MatchingStrategy, format_dt_sql, parse_dt_sql, utc_now

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
        db.save_config(entry.id, config.model_dump(mode='json', exclude={'id'}))


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


def _sqlite_pattern_match(column, strategy):
    """SQLite's id/tag match operators: case-sensitive GLOB for wildcard matches."""
    match strategy:
        case MatchingStrategy.EXACT:
            return f"{column} = ?", lambda p: p
        case MatchingStrategy.PARTIAL:
            return f"{column} GLOB ?", lambda p: f"*{p}*"
        case MatchingStrategy.FN_MATCH:
            return f"{column} GLOB ?", lambda p: p
    return None, None


_SQLITE_DIALECT = Dialect(placeholder="?", pattern_match=_sqlite_pattern_match, bind_dt=format_dt_sql)


def _build_where_clause(run_match, alias=""):
    return build_where_clause(run_match, _SQLITE_DIALECT, alias)


def _to_job_run(r) -> JobRun:
    """Convert a sqlite3.Row from the history table into a JobRun.

    Reconstructs ``JobInstanceMetadata`` from dedicated columns plus an
    optional ``tags`` synthetic column (a ``json_group_array(tag)`` over
    run_tags). Rows without a ``tags`` column read back as having no tags —
    callers that need tag fidelity must include the subselect in their query.
    """
    tags_raw = r['tags'] if 'tags' in r.keys() else None
    tags = tuple(json.loads(tags_raw)) if tags_raw else ()
    features = tuple(json.loads(r['features'])) if r['features'] else ()
    metadata = JobInstanceMetadata(
        InstanceID(r['job_id'], r['run_id'], r['ordinal']),
        json.loads(r['user_params']) if r['user_params'] else {},
        features=features,
        tags=tags,
    )
    root_phase = PhaseRun.deserialize(json.loads(r['root_phase']))
    output_locations = (
        tuple(OutputLocation.deserialize(loc) for loc in json.loads(r['output_locations']))
        if r['output_locations'] else ()
    )
    faults = tuple(Fault.deserialize(f) for f in json.loads(r['faults'])) if r['faults'] else ()
    status = Status.deserialize(json.loads(r['status'])) if r['status'] else None
    return JobRun(metadata, root_phase, output_locations, faults, status)


_RUN_UPDATE_SQL = (
    "UPDATE runs SET user_params=?, features=?, created=?, started=?, ended=?, exec_time=?, "
    "root_phase=?, output_locations=?, termination_status=?, faults=?, status=?, warnings=?, "
    "updated_at=? "
    "WHERE job_id=? AND run_id=? AND ordinal=?"
)


def _run_update_values(r: JobRun):
    """SET-clause values plus the (job_id, run_id, ordinal) keys for :data:`_RUN_UPDATE_SQL`."""
    return (json.dumps(dict(r.metadata.user_params)) if r.metadata.user_params else None,
            json.dumps(list(r.metadata.features)) if r.metadata.features else None,
            format_dt_sql(r.lifecycle.created_at),
            format_dt_sql(r.lifecycle.started_at) if r.lifecycle.started_at else None,
            format_dt_sql(r.lifecycle.termination.terminated_at) if r.lifecycle.termination else None,
            round(r.lifecycle.total_run_time.total_seconds(), 3) if r.lifecycle.total_run_time else None,
            json.dumps(r.root_phase.serialize()),
            json.dumps([l.serialize() for l in r.output_locations]) if r.output_locations else None,
            r.lifecycle.termination.status.value if r.lifecycle.termination else None,
            json.dumps([f.serialize() for f in r.faults]) if r.faults else None,
            json.dumps(r.status.serialize()) if r.status else None,
            len(r.status.warnings) if r.status else None,
            format_dt_sql(utc_now()),
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

        # Fresh schema
        c.execute('''CREATE TABLE runs (
                     job_id TEXT NOT NULL,
                     run_id TEXT NOT NULL,
                     ordinal INTEGER NOT NULL DEFAULT 1,
                     user_params TEXT CHECK (user_params IS NULL OR json_valid(user_params)),
                     features TEXT CHECK (features IS NULL OR json_valid(features)),
                     created TIMESTAMP NOT NULL,
                     started TIMESTAMP,
                     ended TIMESTAMP,
                     exec_time REAL,
                     root_phase TEXT CHECK (root_phase IS NULL OR json_valid(root_phase)),
                     output_locations TEXT CHECK (output_locations IS NULL OR json_valid(output_locations)),
                     termination_status INT,
                     faults TEXT CHECK (faults IS NULL OR json_valid(faults)),
                     status TEXT CHECK (status IS NULL OR json_valid(status)),
                     warnings INT,
                     updated_at TIMESTAMP NOT NULL,
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
        params_json = json.dumps(dict(user_params)) if user_params else None
        created_str = format_dt_sql(created_at)
        # Normalize once up-front. Idempotent — safe even if caller pre-normalized.
        normalized_tags = normalize_tags(tags) if tags else ()

        if not auto_increment:
            try:
                self._conn.execute(
                    "INSERT INTO runs (job_id, run_id, ordinal, user_params, created, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (job_id, run_id, 1, params_json, created_str, created_str))
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
                    "INSERT INTO runs (job_id, run_id, ordinal, user_params, created, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (job_id, run_id, ordinal, params_json, created_str, created_str))
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
        def sort_exp():
            match sort:
                case SortOption.CREATED:
                    return 'h.created, h.rowid'
                case SortOption.STARTED:
                    return 'h.started, h.rowid'
                case SortOption.ENDED:
                    return 'h.ended, h.rowid'
                case SortOption.TIME:
                    return "julianday(h.ended) - julianday(h.created), h.rowid"
                case SortOption.JOB_ID:
                    return 'h.job_id, h.rowid'
                case SortOption.RUN_ID:
                    return 'h.run_id, h.rowid'
                case _:
                    raise ValueError(f"Unsupported sort option: {sort}")

        statement = (
            "SELECT h.*, "
            "(SELECT json_group_array(tag) FROM run_tags t "
            " WHERE t.job_id = h.job_id AND t.run_id = h.run_id AND t.ordinal = h.ordinal) "
            "AS tags "
            "FROM runs h"
        )
        where_clause, where_params = _build_where_clause(run_match, alias='h')
        # Exclude incomplete (init-only) records
        statement += (where_clause + " AND h.ended IS NOT NULL") if where_clause else " WHERE h.ended IS NOT NULL"
        if last:
            statement += " AND " + LAST_PER_JOB_SQL

        direction = " ASC" if asc else " DESC"
        statement += " ORDER BY " + ', '.join(f"{col.strip()}{direction}" for col in sort_exp().split(', '))

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

    @override
    @ensure_open
    def enforce_retention(self, job_id: str, policy: RetentionPolicy):
        """Prune old runs according to retention policy (per-job then per-env)."""
        if policy.max_runs_per_job >= 0:
            self._conn.execute(
                "DELETE FROM runs WHERE job_id = ? AND rowid NOT IN "
                "(SELECT rowid FROM runs WHERE job_id = ? ORDER BY ended DESC LIMIT ?)",
                (job_id, job_id, policy.max_runs_per_job))
        if policy.max_runs_per_env >= 0:
            self._conn.execute(
                "DELETE FROM runs WHERE rowid NOT IN "
                "(SELECT rowid FROM runs ORDER BY ended DESC LIMIT ?)",
                (policy.max_runs_per_env,))
        self._conn.commit()

    @override
    @ensure_open
    def read_run_stats(self, run_match=None) -> List[JobStats]:
        """See `RunStorage.read_run_stats`."""

        where_clause, where_params = _build_where_clause(run_match, alias='h')
        # Exclude incomplete (init-only) records
        if where_clause:
            where_clause += " AND h.ended IS NOT NULL"
        else:
            where_clause = " WHERE h.ended IS NOT NULL"
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
        statement = (
            "SELECT h.*, "
            "(SELECT json_group_array(tag) FROM run_tags t "
            " WHERE t.job_id = h.job_id AND t.run_id = h.run_id AND t.ordinal = h.ordinal) "
            "AS tags "
            "FROM runs h"
        )
        where_clause, where_params = _build_where_clause(run_match, alias='h')
        # ended IS NULL → active; root_phase IS NOT NULL → the persister has written a real
        # snapshot, so skip init-only rows that _to_job_run cannot deserialize.
        guard = "h.ended IS NULL AND h.root_phase IS NOT NULL"
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
        """See `RunStorage.store_active_runs`. The ``ended IS NULL`` guard makes a stale
        active write a no-op once the row is terminal, so it cannot resurrect an ended run."""
        for run in job_runs:
            self._conn.execute(_RUN_UPDATE_SQL + " AND ended IS NULL", _run_update_values(run))
        self._conn.commit()

    @override
    @ensure_open
    def remove_runs(self, run_match):
        """See `RunStorage.remove_runs`."""

        where_clause, where_params = _build_where_clause(run_match)
        if not where_clause:
            raise ValueError("No rows to remove")
        cursor = self._conn.execute(
            "DELETE FROM runs" + where_clause + " RETURNING job_id, run_id, ordinal", where_params)
        rows = cursor.fetchall()
        removed = [InstanceID(job_id=r[0], run_id=r[1], ordinal=r[2]) for r in rows]
        self._conn.commit()
        return removed

    @override
    @ensure_open
    def load_config(self, env_id: str) -> dict:
        """Load environment config as a dict with parsed JSON values."""
        c = self._conn.execute("SELECT key, value FROM config")
        config_dict = {"id": env_id}
        for key, value_json in c.fetchall():
            config_dict[key] = json.loads(value_json)
        return config_dict

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
