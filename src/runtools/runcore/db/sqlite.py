"""
Persistence storage implementation using SQLite with batch fetching to avoid lock starvation.
"""

import datetime
import json
import logging
import sqlite3
from datetime import timezone
from functools import wraps
from threading import Lock
from typing import List, Iterator, override

from runtools.runcore import paths
from runtools.runcore.criteria import LifecycleCriterion, SortOption
from runtools.runcore.db import Persistence
from runtools.runcore.err import InvalidStateError
from runtools.runcore.job import JobStats, JobRun, JobInstanceMetadata, InstanceID
from runtools.runcore.output import OutputLocation
from runtools.runcore.run import TerminationStatus, Outcome, PhaseDetail, Fault, Stage
from runtools.runcore.status import Status
from runtools.runcore.util import MatchingStrategy, format_dt_sql, parse_dt_sql

log = logging.getLogger(__name__)

# Default batch size for fetching records
DEFAULT_BATCH_SIZE = 100


def create(env_id, database=None, **kwargs):
    """
    Creates SQLite persistence with configurable connection parameters.

    Args:
        env_id: ID of database environment
        database: Database path or ':memory:' for in-memory database.
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
    # Extract batch_size if provided
    batch_size = kwargs.pop('batch_size', DEFAULT_BATCH_SIZE)

    # Force check_same_thread to False since we're using _conn_lock
    kwargs['check_same_thread'] = False

    def connection_factory():
        return sqlite3.connect(database or paths.sqlite_db_path(env_id, create=True), **kwargs)

    return SQLite(connection_factory, batch_size)


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

    conditions = []
    params = []

    metadata_conditions = []
    metadata_params = []
    for c in run_match.metadata_criteria:
        if c.strategy == MatchingStrategy.ALWAYS_TRUE:
            metadata_conditions.clear()
            metadata_params.clear()
            break
        if c.strategy == MatchingStrategy.ALWAYS_FALSE:
            return " WHERE 1=0", []  # Early return as nothing can match

        id_conditions = []
        id_params = []
        if c.job_id:
            match c.strategy:
                case MatchingStrategy.PARTIAL:
                    id_conditions.append(f'{alias}job_id GLOB ?')
                    id_params.append(f'*{c.job_id}*')
                case MatchingStrategy.FN_MATCH:
                    id_conditions.append(f'{alias}job_id GLOB ?')
                    id_params.append(c.job_id)
                case MatchingStrategy.EXACT:
                    id_conditions.append(f'{alias}job_id = ?')
                    id_params.append(c.job_id)
                case _:
                    continue

        if c.run_id:
            match c.strategy:
                case MatchingStrategy.PARTIAL:
                    id_conditions.append(f'{alias}run_id GLOB ?')
                    id_params.append(f'*{c.run_id}*')
                case MatchingStrategy.FN_MATCH:
                    id_conditions.append(f'{alias}run_id GLOB ?')
                    id_params.append(c.run_id)
                case MatchingStrategy.EXACT:
                    id_conditions.append(f'{alias}run_id = ?')
                    id_params.append(c.run_id)
                case _:
                    continue

        if id_conditions:
            join_op = ' OR ' if c.match_any_field else ' AND '
            metadata_conditions.append('(' + join_op.join(id_conditions) + ')')
            metadata_params.extend(id_params)

    if metadata_conditions:
        conditions.append('(' + ' OR '.join(metadata_conditions) + ')')
        params.extend(metadata_params)

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


class SQLite(Persistence):

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

    def __enter__(self):
        self.open()
        return self

    @override
    def open(self):
        with self._conn_lock:
            if self._conn is not None:
                raise InvalidStateError("Database connection already opened")
            self._conn = self._connection_factory()
        self.check_tables_exist()

    def is_open(self):
        return self._conn is not None

    @ensure_open
    def check_tables_exist(self):
        # Version 5
        c = self._conn.cursor()
        c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='history' ''')
        if c.fetchone()[0] != 1:
            c.execute('''CREATE TABLE history
                         (job_id text,
                         run_id text,
                         user_params text,
                         created timestamp,
                         started timestamp,
                         ended timestamp,
                         exec_time real,
                         lifecycle text,
                         phases text,
                         output_locations text,
                         termination_status int,
                         faults text,
                         status text,
                         warnings int,
                         misc text)
                         ''')
            c.execute('''CREATE INDEX job_id_index ON history (job_id)''')
            c.execute('''CREATE INDEX run_id_index ON history (run_id)''')
            c.execute('''CREATE INDEX ended_index ON history (ended)''')
            c.execute('''CREATE INDEX created_index ON history (created)''')
            c.execute('''CREATE INDEX exec_time_index ON history (exec_time)''')
            log.debug('event=[table_created] table=[history]')
            self._conn.commit()
        c.close()

    @override
    def read_history_runs(self, run_match=None, sort=SortOption.ENDED, *,
                          asc=True, limit=-1, offset=-1, last=False) -> list[JobRun]:
        """See `Persistence.read_history_runs`."""
        return list(self.iter_history_runs(run_match, sort, asc=asc, limit=limit, offset=offset, last=last))

    @override
    def iter_history_runs(self, run_match=None, sort=SortOption.ENDED, *,
                          asc=True, limit=-1, offset=-1, last=False) -> Iterator[JobRun]:
        """See `Persistence.iter_history_runs`.

        Note:
            Uses batched fetching to minimize lock contention. Batch size is configurable
            via the `batch_size` parameter when creating the SQLite instance.
        """
        current_offset = offset if offset >= 0 else 0
        remaining_limit = limit if limit >= 0 else float('inf')

        while remaining_limit > 0:
            # Fetch next batch
            batch_size = min(self._batch_size, remaining_limit) if remaining_limit != float('inf') else self._batch_size
            batch = self._fetch_batch_history_runs(
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
    def _fetch_batch_history_runs(self, run_match=None, sort=SortOption.ENDED, *,
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

        statement = "SELECT * FROM history h"
        where_clause, where_params = _build_where_clause(run_match, alias='h')
        statement += where_clause

        if last:
            if where_clause:
                statement += " AND h.rowid IN (SELECT MAX(rowid) FROM history GROUP BY job_id)"
            else:
                statement += " WHERE h.rowid IN (SELECT MAX(rowid) FROM history GROUP BY job_id)"

        # Apply the sort direction to all columns in the ORDER BY clause
        sort_direction = " ASC" if asc else " DESC"
        sort_columns = sort_exp().split(', ')
        order_by_clause = ', '.join(f"{col.strip()}{sort_direction}" for col in sort_columns)
        statement += " ORDER BY " + order_by_clause
        statement += " LIMIT ? OFFSET ?"

        log.debug("event=[executing_batch_query] statement=[%s] batch_size=[%d] offset=[%d]",
                  statement, batch_size, batch_offset)

        cursor = self._conn.cursor()
        cursor.execute(statement, where_params + [batch_size, batch_offset])

        def to_job_run(t):
            metadata = JobInstanceMetadata(InstanceID(t[0], t[1]), json.loads(t[2]) if t[2] else dict())
            root_phase = PhaseDetail.deserialize(json.loads(t[8]))
            output_locations = tuple(OutputLocation.deserialize(l) for l in json.loads(t[9])) if t[9] else ()
            faults = tuple(Fault.deserialize(f) for f in json.loads(t[11])) if t[11] else ()
            status = Status.deserialize(json.loads(t[12])) if t[12] else None
            return JobRun(metadata, root_phase, output_locations, faults, status)

        try:
            rows = cursor.fetchall()
            return [to_job_run(row) for row in rows]
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
        sql = f"SELECT COUNT(*) FROM history h {where_clause}"
        c = self._conn.execute(sql, where_params)
        return c.fetchone()[0]

    @override
    @ensure_open
    def clean_up(self, max_records, max_age):
        if max_records >= 0:
            self._max_rows(max_records)
        if max_age:
            self._delete_old_jobs(max_age)

    def _max_rows(self, limit):
        c = self._conn.execute("SELECT COUNT(*) FROM history")
        count = c.fetchone()[0]
        c.close()
        if count > limit:
            self._conn.execute(
                "DELETE FROM history WHERE rowid not in (SELECT rowid FROM history ORDER BY ended DESC LIMIT (?))",
                (limit,))
            self._conn.commit()

    def _delete_old_jobs(self, max_age):
        self._conn.execute("DELETE FROM history WHERE ended < (?)",
                           (format_dt_sql(datetime.datetime.now(tz=timezone.utc) - max_age),))
        self._conn.commit()

    @override
    @ensure_open
    def read_history_stats(self, run_match=None) -> List[JobStats]:
        """See `Persistence.read_history_stats`."""

        where_clause, where_params = _build_where_clause(run_match, alias='h')
        fault_statuses = TerminationStatus.get_statuses(Outcome.FAULT)
        fault_placeholders = ', '.join(str(s.value) for s in fault_statuses)
        sql = f'''
            WITH filtered AS (
                SELECT * FROM history h
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
                COUNT(CASE WHEN f.termination_status IN ({fault_placeholders}) THEN 1 END) AS failed,
                lh.warnings AS "last_warnings"
            FROM filtered f
            JOIN last_per_job lp ON f.job_id = lp.job_id
            JOIN filtered lh ON lh.rowid = lp.max_rowid
            GROUP BY f.job_id
        '''
        c = self._conn.execute(sql, where_params)

        def to_job_stats(t):
            job_id = t[0]
            count = t[1]
            first_at = parse_dt_sql(t[2])
            last_at = parse_dt_sql(t[3])
            fastest = datetime.timedelta(seconds=t[4]) if t[4] is not None else None
            average = datetime.timedelta(seconds=t[5]) if t[5] is not None else None
            slowest = datetime.timedelta(seconds=t[6]) if t[6] is not None else None
            last_time = datetime.timedelta(seconds=t[7]) if t[7] is not None else None
            last_term_status = TerminationStatus.from_code(t[8]) if t[8] is not None else TerminationStatus.UNKNOWN
            failed_count = t[9] or 0
            warn_count = t[10] or 0

            return JobStats(
                job_id, count, first_at, last_at, fastest, average, slowest, last_time, last_term_status, failed_count,
                warn_count
            )

        return [to_job_stats(row) for row in c.fetchall()]

    @override
    @ensure_open
    def store_job_runs(self, *job_runs):
        """See `Persistence.store_job_runs`."""

        def to_tuple(r: JobRun):
            return (r.metadata.job_id,
                    r.metadata.run_id,
                    json.dumps(dict(r.metadata.user_params)) if r.metadata.user_params else None,
                    format_dt_sql(r.lifecycle.created_at),
                    format_dt_sql(r.lifecycle.started_at),
                    format_dt_sql(r.lifecycle.termination.terminated_at) if r.lifecycle.termination else None,
                    round(r.lifecycle.total_run_time.total_seconds(), 3) if r.lifecycle.total_run_time else None,
                    json.dumps(r.lifecycle.serialize()),
                    json.dumps(r.root_phase.serialize()),
                    json.dumps([l.serialize() for l in r.output_locations]) if r.output_locations else None,
                    r.lifecycle.termination.status.value if r.lifecycle.termination else None,
                    json.dumps([f.serialize() for f in r.faults]) if r.faults else None,
                    json.dumps(r.status.serialize()) if r.status else None,
                    len(r.status.warnings) if r.status else None,
                    None  # misc
                    )

        jobs = [to_tuple(j) for j in job_runs]
        self._conn.executemany(
            "INSERT INTO history VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            jobs
        )
        self._conn.commit()

    @override
    @ensure_open
    def remove_job_runs(self, run_match):
        """See `Persistence.remove_job_runs`."""

        where_clause, where_params = _build_where_clause(run_match)
        if not where_clause:
            raise ValueError("No rows to remove")
        self._conn.execute("DELETE FROM history" + where_clause, where_params)
        self._conn.commit()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @override
    def close(self):
        with self._conn_lock:
            if self._conn:
                self._conn.close()
                self._conn = None
