"""
SQLite implementation of EnvironmentDatabase with batch fetching to avoid lock starvation.
"""

import datetime
import json
import logging
import sqlite3
from functools import wraps
from pathlib import Path
from threading import Lock
from typing import List, Iterator, override

from runtools.runcore import paths
from runtools.runcore.db import EnvironmentDatabase, IncompatibleSchemaError
from runtools.runcore.err import InvalidStateError
from runtools.runcore.job import (JobStats, JobRun, JobInstanceMetadata, InstanceID, DuplicateInstanceError,
                                  normalize_tags)
from runtools.runcore.matching import LifecycleCriterion, SortOption
from runtools.runcore.output import OutputLocation
from runtools.runcore.retention import RetentionPolicy
from runtools.runcore.run import TerminationStatus, Outcome, PhaseRun, Fault, Stage
from runtools.runcore.status import Status
from runtools.runcore.util import MatchingStrategy, format_dt_sql, parse_dt_sql, utc_now

log = logging.getLogger(__name__)

SCHEMA_VERSION = 2
DEFAULT_BATCH_SIZE = 100


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
            - batch_size: Number of records to fetch per batch (default: 100)

    Returns:
        SQLite: Configured SQLite persistence instance
    """
    batch_size = kwargs.pop('batch_size', DEFAULT_BATCH_SIZE)
    kwargs['check_same_thread'] = False
    resolved = str(_resolve_path(entry))

    return SQLite(lambda: sqlite3.connect(resolved, **kwargs), batch_size)


def create_memory(env_id: str, **kwargs) -> 'SQLite':
    """Create an in-memory SQLite database for testing."""
    kwargs['check_same_thread'] = False
    batch_size = kwargs.pop('batch_size', DEFAULT_BATCH_SIZE)
    return SQLite(lambda: sqlite3.connect(':memory:', **kwargs), batch_size)


def ensure_open(f):
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        with self._conn_lock:
            if not self._conn:
                raise InvalidStateError("Database connection not opened")
            return f(self, *args, **kwargs)

    return wrapper


def _build_where_clause(run_match, alias=''):
    """
    Builds a parameterized SQL WHERE clause from the provided run match criteria.

    TODO: Post fetch filter for criteria not supported in WHERE (instance parameters, etc.)
    Only root phase details are stored as direct fields in the database.
    Phase criteria are only applied if they target the root phase.
    Other phase criteria require post-fetch filtering.

    Args:
        run_match (JobRunCriteria): The run match criteria.
        alias (str): Optional table alias prefix.

    Returns:
        tuple[str, list]: WHERE clause with ? placeholders, and list of parameter values.
    """
    if not run_match:
        return "", []

    if alias and not alias.endswith('.'):
        alias = alias + "."
    # Inside correlated subqueries (tag predicates), bare column names like
    # `job_id` would bind to the INNER table (run_tags), not the outer runs row.
    # Use an explicit outer-table reference instead.
    outer_ref = alias if alias else 'runs.'

    conditions = []
    params = []

    def _id_match_fragment(field: str, pattern: str, strategy: MatchingStrategy):
        match strategy:
            case MatchingStrategy.PARTIAL:
                return f'{alias}{field} GLOB ?', f'*{pattern}*'
            case MatchingStrategy.FN_MATCH:
                return f'{alias}{field} GLOB ?', pattern
            case MatchingStrategy.EXACT:
                return f'{alias}{field} = ?', pattern
            case _:
                return None, None

    def _tag_match_fragment(pattern: str, strategy: MatchingStrategy):
        """EXISTS clause: ``any run-tag matches pattern under strategy``."""
        prefix = (f"EXISTS (SELECT 1 FROM run_tags t "
                  f"WHERE t.job_id={outer_ref}job_id AND t.run_id={outer_ref}run_id "
                  f"AND t.ordinal={outer_ref}ordinal AND ")
        match strategy:
            case MatchingStrategy.EXACT:
                return prefix + "t.tag = ?)", pattern
            case MatchingStrategy.PARTIAL:
                return prefix + "t.tag GLOB ?)", f'*{pattern}*'
            case MatchingStrategy.FN_MATCH:
                return prefix + "t.tag GLOB ?)", pattern
            case _:
                return None, None

    def _metadata_clause(c) -> tuple[str | None, list]:
        """Build one criterion's WHERE fragment. Returns (sql, params).

        - ``None``   — criterion is unconstrained (match all).
        - otherwise — a single parenthesized AND-joined predicate.
        """
        parts: list[str] = []
        params: list = []

        if c.strategy != MatchingStrategy.ALWAYS_TRUE:
            id_parts: list[str] = []
            id_params: list = []
            if c.job_id:
                frag, val = _id_match_fragment('job_id', c.job_id, c.strategy)
                if frag is not None:
                    id_parts.append(frag)
                    id_params.append(val)
            if c.run_id:
                frag, val = _id_match_fragment('run_id', c.run_id, c.strategy)
                if frag is not None:
                    id_parts.append(frag)
                    id_params.append(val)
            if id_parts:
                join_op = ' OR ' if c.match_any_field else ' AND '
                parts.append('(' + join_op.join(id_parts) + ')')
                params.extend(id_params)

            # Tags are always an AND filter (each pattern must find a matching run-tag).
            # The bare-token tag-axis search is composed as a separate OR'd criterion
            # at the JobRunCriteria level — not via tags participating in this OR group.
            for pat in c.tags:
                frag, val = _tag_match_fragment(pat, c.strategy)
                if frag is not None:
                    parts.append(frag)
                    params.append(val)

        if c.ordinal is not None:
            parts.append(f'{alias}ordinal = ?')
            params.append(c.ordinal)

        if not parts:
            return None, []
        return '(' + ' AND '.join(parts) + ')', params

    metadata_conditions = []
    metadata_params = []
    exclude_conditions = []
    exclude_params = []
    match_all_seen = False
    for c in run_match.metadata_criteria:
        if c.exclude is not None:
            excl = c.exclude
            exclude_conditions.append(
                f"NOT ({alias}job_id = ? AND {alias}run_id = ? AND {alias}ordinal = ?)")
            exclude_params.extend([excl.job_id, excl.run_id, excl.ordinal])

        clause, clause_params = _metadata_clause(c)
        if clause is None:
            # Unconstrained criterion — OR'd with anything = match-all.
            # Discard accumulated metadata predicates; excludes still apply.
            match_all_seen = True
            continue
        if not match_all_seen:
            metadata_conditions.append(clause)
            metadata_params.extend(clause_params)

    if match_all_seen:
        metadata_conditions = []
        metadata_params = []

    if metadata_conditions:
        conditions.append('(' + ' OR '.join(metadata_conditions) + ')')
        params.extend(metadata_params)
    conditions.extend(exclude_conditions)
    params.extend(exclude_params)

    def add_datetime_conditions(column: str, dt_range) -> tuple[list[str], list]:
        conds = []
        prms = []
        if not dt_range:
            return conds, prms

        # Check if this is an unbounded range that just checks for existence
        if dt_range.is_unbounded():
            conds.append(f"{alias}{column} IS NOT NULL")
            return conds, prms

        if dt_range.since:
            conds.append(f"{alias}{column} >= ?")
            prms.append(format_dt_sql(dt_range.since))
        if dt_range.until:
            if dt_range.until_included:
                conds.append(f"{alias}{column} <= ?")
            else:
                conds.append(f"{alias}{column} < ?")
            prms.append(format_dt_sql(dt_range.until))
        return conds, prms

    def add_time_range_conditions(time_range) -> tuple[list[str], list]:
        """Add SQL conditions for TimeRange on exec_time column."""
        conds = []
        prms = []
        if time_range.min is not None:
            conds.append(f"{alias}exec_time >= ?")
            prms.append(time_range.min.total_seconds())
        if time_range.max is not None:
            conds.append(f"{alias}exec_time <= ?")
            prms.append(time_range.max.total_seconds())
        return conds, prms

    def add_lifecycle_conditions(lifecycle_criterion: LifecycleCriterion) -> tuple[list[str], list]:
        """Add SQL conditions for lifecycle criteria."""
        if not lifecycle_criterion:
            return [], []

        conds = []
        prms = []

        if lifecycle_criterion.stage:
            match lifecycle_criterion.stage:
                case Stage.CREATED:
                    # Created but not started yet
                    conds.append(f"{alias}started IS NULL")
                case Stage.RUNNING:
                    # Started but not ended yet
                    conds.append(f"{alias}started IS NOT NULL AND {alias}ended IS NULL")
                case Stage.ENDED:
                    # Has ended timestamp
                    conds.append(f"{alias}ended IS NOT NULL")

        if lifecycle_criterion.created:
            c, p = add_datetime_conditions('created', lifecycle_criterion.created)
            conds.extend(c)
            prms.extend(p)
        if lifecycle_criterion.started:
            c, p = add_datetime_conditions('started', lifecycle_criterion.started)
            conds.extend(c)
            prms.extend(p)
        if lifecycle_criterion.ended:
            c, p = add_datetime_conditions('ended', lifecycle_criterion.ended)
            conds.extend(c)
            prms.extend(p)

        if lifecycle_criterion.total_run_time:
            c, p = add_time_range_conditions(lifecycle_criterion.total_run_time)
            conds.extend(c)
            prms.extend(p)

        if lifecycle_criterion.termination:
            term = lifecycle_criterion.termination
            if term.status:
                conds.append(f"{alias}termination_status = ?")
                prms.append(term.status.value)

            if term.outcome is not None:
                statuses = TerminationStatus.get_statuses(term.outcome)
                placeholders = ', '.join('?' * len(statuses))
                conds.append(f"{alias}termination_status IN ({placeholders})")
                prms.extend(s.value for s in statuses)

            if term.success is not None:
                outcomes = Outcome.get_outcomes(success=term.success)
                statuses = TerminationStatus.get_statuses(*outcomes)
                placeholders = ', '.join('?' * len(statuses))
                conds.append(f"{alias}termination_status IN ({placeholders})")
                prms.extend(s.value for s in statuses)

            if term.ended_range:
                c, p = add_datetime_conditions('ended', term.ended_range)
                conds.extend(c)
                prms.extend(p)

        return conds, prms

    lifecycle_conditions = []
    lifecycle_params = []
    for lc in run_match.lifecycle_criteria:
        phase_conditions, phase_params = add_lifecycle_conditions(lc)
        if phase_conditions:
            lifecycle_conditions.append('(' + ' AND '.join(phase_conditions) + ')')
            lifecycle_params.extend(phase_params)

    if lifecycle_conditions:
        conditions.append('(' + ' OR '.join(lifecycle_conditions) + ')')
        params.extend(lifecycle_params)

    return (" WHERE " + " AND ".join(conditions), params) if conditions else ("", [])


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

    def __init__(self, connection_factory, batch_size=DEFAULT_BATCH_SIZE):
        """
        Args:
            connection_factory: Callable that returns a sqlite3.Connection
            batch_size: Number of records to fetch per batch
        """
        self._connection_factory = connection_factory
        self._conn = None
        self._conn_lock = Lock()
        self._batch_size = batch_size

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

        Uses batched fetching to minimize lock contention.
        """
        current_offset = offset if offset >= 0 else 0
        remaining_limit = limit if limit >= 0 else float('inf')

        while remaining_limit > 0:
            # Fetch next batch
            batch_size = min(self._batch_size, remaining_limit) if remaining_limit != float('inf') else self._batch_size
            batch = self._fetch_batch_runs(
                run_match, sort, asc=asc, batch_offset=current_offset, batch_size=batch_size, last=last
            )

            if not batch:
                break

            for job_run in batch:
                yield job_run

            # Update for next iteration
            current_offset += len(batch)
            remaining_limit -= len(batch)

            # If we got fewer records than batch size, we've reached the end
            if len(batch) < self._batch_size:
                break

    @ensure_open
    def _fetch_batch_runs(self, run_match=None, sort=SortOption.ENDED, *,
                                  asc=True, batch_offset=0, batch_size=DEFAULT_BATCH_SIZE,
                                  last=False) -> List[JobRun]:
        """
        Fetches a batch of job runs from the database.

        This is an internal method that fetches a specific batch of records. It acquires
        the lock only for the duration of the database query and releases it after
        fetching the batch.

        Args:
            run_match: Match criteria for filtering records
            sort: Sort criteria
            asc: Sort order
            batch_offset: Number of records to skip
            batch_size: Number of records to fetch
            last: Whether to fetch only the last record per job

        Returns:
            List[JobRun]: A batch of job runs
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
        if where_clause:
            where_clause += " AND h.ended IS NOT NULL"
        else:
            where_clause = " WHERE h.ended IS NOT NULL"
        statement += where_clause

        if last:
            statement += " AND h.rowid IN (SELECT MAX(rowid) FROM runs WHERE ended IS NOT NULL GROUP BY job_id)"

        # Apply the sort direction to all columns in the ORDER BY clause
        sort_direction = " ASC" if asc else " DESC"
        sort_columns = sort_exp().split(', ')
        order_by_clause = ', '.join(f"{col.strip()}{sort_direction}" for col in sort_columns)
        statement += " ORDER BY " + order_by_clause
        statement += " LIMIT ? OFFSET ?"

        log.debug("Executing batch query", extra={"statement": statement, "batch_size": batch_size, "offset": batch_offset})

        cursor = self._conn.cursor()
        cursor.row_factory = sqlite3.Row
        cursor.execute(statement, where_params + [batch_size, batch_offset])

        try:
            rows = cursor.fetchall()
            return [_to_job_run(row) for row in rows]
        finally:
            cursor.close()

    @ensure_open
    def count_instances(self, run_match):
        """
        Counts the total number of job instances based on the specified match criteria.
        Datasource: The database as defined by the configured persistence type.

        Args:
            run_match (InstanceMatchCriteria): Criteria to filter job instances.

        Returns:
            int: Total count of job instances matching the specified criteria.
        """
        where_clause, where_params = _build_where_clause(run_match, alias='h')
        sql = f"SELECT COUNT(*) FROM runs h {where_clause}"
        c = self._conn.execute(sql, where_params)
        return c.fetchone()[0]

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

        def to_job_stats(r):
            return JobStats(
                job_id=r['job_id'],
                count=r['count'],
                first_created=parse_dt_sql(r['first_created']),
                last_created=parse_dt_sql(r['last_created']),
                fastest_time=datetime.timedelta(seconds=r['fastest_time']) if r['fastest_time'] is not None else None,
                average_time=datetime.timedelta(seconds=r['average_time']) if r['average_time'] is not None else None,
                slowest_time=datetime.timedelta(seconds=r['slowest_time']) if r['slowest_time'] is not None else None,
                last_time=datetime.timedelta(seconds=r['last_time']) if r['last_time'] is not None else None,
                termination_status=(
                    TerminationStatus.from_code(r['last_term_status'])
                    if r['last_term_status'] is not None else TerminationStatus.UNKNOWN
                ),
                success_count=r['succeeded'] or 0,
                failed_count=r['failed'] or 0,
                aborted_count=r['aborted'] or 0,
                rejected_count=r['rejected'] or 0,
                ignored_count=r['ignored'] or 0,
                warning_count=r['last_warnings'] or 0,
            )

        return [to_job_stats(row) for row in c.fetchall()]

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
