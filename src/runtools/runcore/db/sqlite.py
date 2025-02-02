"""
Persistence storage implementation using SQLite. See `runtools.runcore.persistence` module doc for much more details.
"""

import datetime
import json
import logging
import sqlite3
from datetime import timezone
from functools import wraps
from typing import List

from runtools.runcore import paths
from runtools.runcore.common import InvalidStateError
from runtools.runcore.criteria import PhaseMatch
from runtools.runcore.db import SortCriteria, Persistence
from runtools.runcore.job import JobStats, JobRun, JobRuns, JobInstanceMetadata, JobFaults
from runtools.runcore.run import TerminationStatus, Outcome, PhaseDetail
from runtools.runcore.status import Status
from runtools.runcore.util import MatchingStrategy, format_dt_sql, parse_dt_sql

log = logging.getLogger(__name__)


def ensure_open(f):
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        if not self._conn:
            raise InvalidStateError("Database connection not opened")
        return f(self, *args, **kwargs)

    return wrapper


def create_database(db_conf):
    config = db_conf if db_conf else {}
    db_con = sqlite3.connect(config.get('database') or str(paths.sqlite_db_path(True)))
    sqlite_ = SQLite(db_con)
    sqlite_.check_tables_exist()
    return sqlite_


def _build_where_clause(run_match, alias=''):
    """
    # TODO Post fetch filter for criteria not supported in WHERE (instance parameters, etc.)

    Builds a SQL WHERE clause from the provided run match criteria.
    Only root phase details are stored as direct fields in the database.
    Phase criteria are only applied if they target the root phase.
    Other phase criteria require post-fetch filtering.

    Args:
        run_match: The run match criteria
        alias: Optional table alias prefix

    Returns:
        str: The WHERE clause, or empty string if no criteria
    """
    if not run_match:
        return ""

    if alias and not alias.endswith('.'):
        alias = alias + "."

    conditions = []

    # Handle job ID direct matches
    if run_match.jobs:
        job_conditions = [f'{alias}job_id = "{j}"' for j in run_match.jobs]
        if job_conditions:
            conditions.append('(' + ' OR '.join(job_conditions) + ')')

    # Handle metadata criteria
    metadata_conditions = []
    for c in run_match.metadata_criteria:
        if c.strategy == MatchingStrategy.ALWAYS_TRUE:
            metadata_conditions.clear()
            break
        if c.strategy == MatchingStrategy.ALWAYS_FALSE:
            return " WHERE 1=0"  # Early return as nothing can match

        id_conditions = []
        if c.job_id:
            match c.strategy:
                case MatchingStrategy.PARTIAL:
                    id_conditions.append(f'{alias}job_id GLOB "*{c.job_id}*"')
                case MatchingStrategy.FN_MATCH:
                    id_conditions.append(f'{alias}job_id GLOB "{c.job_id}"')
                case MatchingStrategy.EXACT:
                    id_conditions.append(f'{alias}job_id = "{c.job_id}"')
                case _:
                    continue

        if c.run_id:
            match c.strategy:
                case MatchingStrategy.PARTIAL:
                    id_conditions.append(f'{alias}run_id GLOB "*{c.run_id}*"')
                case MatchingStrategy.FN_MATCH:
                    id_conditions.append(f'{alias}run_id GLOB "{c.run_id}"')
                case MatchingStrategy.EXACT:
                    id_conditions.append(f'{alias}run_id = "{c.run_id}"')
                case _:
                    continue

        if c.instance_id:
            match c.strategy:
                case MatchingStrategy.PARTIAL:
                    id_conditions.append(f'{alias}instance_id GLOB "*{c.instance_id}*"')
                case MatchingStrategy.FN_MATCH:
                    id_conditions.append(f'{alias}instance_id GLOB "{c.instance_id}"')
                case MatchingStrategy.EXACT:
                    id_conditions.append(f'{alias}instance_id = "{c.instance_id}"')
                case _:
                    continue

        if id_conditions:
            join_op = ' OR ' if c.match_any_id else ' AND '
            metadata_conditions.append('(' + join_op.join(id_conditions) + ')')

    if metadata_conditions:
        conditions.append('(' + ' OR '.join(metadata_conditions) + ')')

    def add_datetime_conditions(column: str, dt_range) -> list:
        dt_conditions = []
        if not dt_range:
            return dt_conditions

        if dt_range.start:
            dt_conditions.append(f"{alias}{column} >= '{format_dt_sql(dt_range.start)}'")
        if dt_range.end:
            if dt_range.end_excluded:
                dt_conditions.append(f"{alias}{column} < '{format_dt_sql(dt_range.end)}'")
            else:
                dt_conditions.append(f"{alias}{column} <= '{format_dt_sql(dt_range.end)}'")
        return dt_conditions

    def add_time_range_conditions(time_range) -> list:
        """Add SQL conditions for TimeRange on exec_time column."""
        conditions_ = []
        if time_range.min is not None:
            conditions_.append(f"{alias}exec_time >= {time_range.min.total_seconds()}")
        if time_range.max is not None:
            conditions_.append(f"{alias}exec_time <= {time_range.max.total_seconds()}")
        return conditions_

    # Handle phase criteria that target root phase
    for phase in run_match.phase_criteria:
        # Skip non-root phase criteria
        if phase.match_type != PhaseMatch.ROOT:
            continue

        phase_conditions = []

        if phase.created_range:
            phase_conditions.extend(add_datetime_conditions('created', phase.created_range))
        if phase.started_range:
            phase_conditions.extend(add_datetime_conditions('started', phase.started_range))
        if phase.ended_range:
            phase_conditions.extend(add_datetime_conditions('ended', phase.ended_range))

        if phase.exec_range:
            phase_conditions.extend(add_time_range_conditions(phase.exec_range))

        if phase.termination:
            if phase.termination.status:
                phase_conditions.append(f"{alias}termination_status = '{phase.termination.status.name}'")

            if phase.termination.outcome != Outcome.ANY:
                start, end = phase.termination.outcome.value.start, phase.termination.outcome.value.stop
                phase_conditions.append(f"({alias}termination_status BETWEEN {start} AND {end})")

            if phase.termination.ended_range:
                phase_conditions.extend(add_datetime_conditions('ended', phase.termination.ended_range))

        if phase_conditions:
            conditions.append('(' + ' AND '.join(phase_conditions) + ')')

    return " WHERE " + " AND ".join(conditions) if conditions else ""

def create(database=None, *,
           timeout=5.0,
           detect_types=0,
           isolation_level='DEFERRED',
           check_same_thread=True,
           cached_statements=128,
           uri=False,
           autocommit=sqlite3.LEGACY_TRANSACTION_CONTROL):
    """
    Creates SQLite persistence with configurable connection parameters.

    Args:
        database: Database path or ':memory:' for in-memory database. If None, uses default path
        timeout: Float timeout value in seconds
        detect_types: Control whether string or binary is converted to SQLite types
        isolation_level: Sets transaction isolation level
        check_same_thread: Enforce thread safety checks
        cached_statements: Number of statements to cache
        uri: True if database parameter is a URI
        autocommit: Transaction control mode

    Returns:
        SQLite: Configured SQLite persistence instance
    """

    def connection_factory():
        return sqlite3.connect(
            database or str(paths.sqlite_db_path(True)),
            timeout=timeout,
            detect_types=detect_types,
            isolation_level=isolation_level,
            check_same_thread=check_same_thread,
            cached_statements=cached_statements,
            uri=uri,
            autocommit=autocommit
        )

    return SQLite(connection_factory)


class SQLite(Persistence):

    def __init__(self, connection_factory):
        """
        Args:
            connection_factory: Callable that returns a sqlite3.Connection
        """
        self._connection_factory = connection_factory
        self._conn = None

    def __enter__(self):
        self.open()
        return self

    def open(self):
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
                         instance_id text,
                         user_params text,
                         created timestamp,
                         started timestamp,
                         ended timestamp,
                         exec_time real,
                         phase text,
                         termination_status int,
                         faults text,
                         status text,
                         warnings int,
                         misc text)
                         ''')
            c.execute('''CREATE INDEX job_id_index ON history (job_id)''')
            c.execute('''CREATE INDEX instance_id_index ON history (instance_id)''')
            c.execute('''CREATE INDEX ended_index ON history (ended)''')  # TODO created + exec_time idx too
            log.debug('event=[table_created] table=[history]')
            self._conn.commit()

    @ensure_open
    def read_history_runs(self, run_match=None, sort=SortCriteria.ENDED, *, asc=True, limit=-1, offset=-1, last=False) \
            -> JobRuns:
        """
        Fetches ended job instances based on specified criteria.
        Datasource: The database as defined by the configured persistence type.

        Args:
            run_match (InstanceMatchCriteria, optional):
                Criteria to match specific job instances. None means fetch all. Defaults to None.
            sort (SortCriteria):
                Determines the field by which records are sorted. Defaults to `SortCriteria.ENDED`.
            asc (bool, optional):
                Determines if the sorting is in ascending order. Defaults to True.
            limit (int, optional):
                Maximum number of records to return. -1 means no limit. Defaults to -1.
            offset (int, optional):
                Number of records to skip before starting to return. -1 means no offset. Defaults to -1.
            last (bool, optional):
                If set to True, only the last record for each job is returned. Defaults to False.

        Returns:
            JobRuns: A collection of job instances that match the given criteria.
        """

        def sort_exp():
            if sort == SortCriteria.CREATED:
                return 'h.created'
            if sort == SortCriteria.ENDED:
                return 'h.ended'
            if sort == SortCriteria.TIME:
                return "julianday(h.ended) - julianday(h.created)"
            raise ValueError(sort)

        statement = "SELECT * FROM history h"
        statement += _build_where_clause(run_match, alias='h')

        if last:
            statement += " GROUP BY h.job_id HAVING ROWID = max(ROWID) "

        statement += " ORDER BY " + sort_exp() + (" ASC" if asc else " DESC") + " LIMIT ? OFFSET ?"

        log.debug("event=[executing_query] statement=[%s]", statement)
        print(statement)
        c = self._conn.execute(statement, (limit, offset))

        def to_job_run(t):
            metadata = JobInstanceMetadata(t[0], t[1], t[2], json.loads(t[3]) if t[3] else dict())
            phase = PhaseDetail.deserialize(json.loads(t[8]))
            faults = JobFaults.deserialize(json.loads(t[10])) if t[10] else None
            status = Status.deserialize(json.loads(t[11])) if t[11] else None
            return JobRun(metadata, phase, faults, status)

        return JobRuns((to_job_run(row) for row in c.fetchall()))

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
        return sum(s.count for s in (self.read_history_stats(run_match)))

    @ensure_open
    def clean_up(self, max_records, max_age):
        if max_records >= 0:
            self._max_rows(max_records)
        if max_age:
            self._delete_old_jobs(max_age)

    def _max_rows(self, limit):
        c = self._conn.execute("SELECT COUNT(*) FROM history")
        count = c.fetchone()[0]
        if count > limit:
            self._conn.execute(
                "DELETE FROM history WHERE rowid not in (SELECT rowid FROM history ORDER BY ended DESC LIMIT (?))",
                (limit,))
            self._conn.commit()

    def _delete_old_jobs(self, max_age):
        self._conn.execute("DELETE FROM history WHERE ended < (?)",
                           ((datetime.datetime.now(tz=timezone.utc) - max_age),))
        self._conn.commit()

    @ensure_open
    def read_history_stats(self, run_match=None) -> List[JobStats]:
        """
        Returns job statistics for each job based on specified criteria.
        Datasource: The database as defined by the configured persistence type.

        Args:
            run_match (InstanceMatchCriteria, optional):
                Criteria to match records used to calculate the statistics. None means fetch all. Defaults to None.
        """

        where = _build_where_clause(run_match, alias='h')
        sql = f'''
            SELECT
                h.job_id,
                count(h.job_id) AS "count",
                min(created) AS "first_created",
                max(created) AS "last_created",
                min(h.exec_time) AS "fastest_time",
                avg(h.exec_time) AS "average_time",
                max(h.exec_time) AS "slowest_time",
                last.exec_time AS "last_time",
                last.termination_status AS "last_term_status",
                COUNT(CASE WHEN h.termination_status BETWEEN {Outcome.FAULT.value.start} AND {Outcome.FAULT.value.stop} THEN 1 ELSE NULL END) AS failed,
                h.warnings
            FROM
                history h
            INNER JOIN
                (SELECT job_id, exec_time, termination_status FROM history h {where} GROUP BY job_id HAVING ROWID = max(ROWID)) AS last
                ON h.job_id = last.job_id
            {where}
            GROUP BY
                h.job_id
        '''
        c = self._conn.execute(sql)

        def to_job_stats(t):
            job_id = t[0]
            count = t[1]
            first_at = parse_dt_sql(t[2])
            last_at = parse_dt_sql(t[3])
            fastest = datetime.timedelta(seconds=t[4]) if t[4] else None
            average = datetime.timedelta(seconds=t[5]) if t[5] else None
            slowest = datetime.timedelta(seconds=t[6]) if t[6] else None
            last_time = datetime.timedelta(seconds=t[7]) if t[7] else None
            last_term_status = TerminationStatus.from_code(t[8]) if t[8] else TerminationStatus.UNKNOWN
            failed_count = t[9]
            warn_count = t[10]

            return JobStats(
                job_id, count, first_at, last_at, fastest, average, slowest, last_time, last_term_status, failed_count,
                warn_count
            )

        return [to_job_stats(row) for row in c.fetchall()]

    @ensure_open
    def store_job_runs(self, *job_runs):
        """
        Stores the provided job instances to the configured persistence source.
        After storing, it also initiates a cleanup based on configured criteria.

        Args:
            *job_runs (JobInst): Variable number of job instances to be stored.
        """

        def to_tuple(r):
            return (r.metadata.job_id,
                    r.metadata.run_id,
                    r.metadata.instance_id,
                    json.dumps(r.metadata.user_params) if r.metadata.user_params else None,
                    format_dt_sql(r.phase.created_at),
                    format_dt_sql(r.phase.started_at),
                    format_dt_sql(r.phase.termination.terminated_at) if r.phase.termination else None,
                    round(r.phase.total_run_time.total_seconds(), 3) if r.phase.total_run_time else None,
                    json.dumps(r.phase.serialize()),
                    r.phase.termination.status.value if r.phase.termination else None,
                    json.dumps(r.faults.serialize()) if r.faults else None,
                    json.dumps(r.status.serialize()) if r.status else None,
                    len(r.status.warnings) if r.status else None,
                    None  # misc
                    )

        jobs = [to_tuple(j) for j in job_runs]
        self._conn.executemany(
            "INSERT INTO history VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            jobs
        )
        self._conn.commit()

    @ensure_open
    def remove_job_runs(self, run_match):
        """
        Removes job instances based on the specified match criteria from the configured persistence source.

        Args:
            run_match (InstanceMatchCriteria): Criteria to filter job instances for removal.
        """

        where_clause = _build_where_clause(run_match)
        if not where_clause:
            raise ValueError("No rows to remove")
        self._conn.execute("DELETE FROM history" + where_clause)
        self._conn.commit()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self._conn.close()
