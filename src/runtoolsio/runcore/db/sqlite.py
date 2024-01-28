"""
Persistence storage implementation using SQLite. See `runtoolsio.runcore.persistence` module doc for much more details.
"""

import datetime
import json
import logging
import sqlite3
from datetime import timezone
from typing import List

from runtoolsio.runcore import paths
from runtoolsio.runcore.job import JobStats, JobRun, JobRuns, InstanceTransitionObserver
from runtoolsio.runcore.persistence import SortCriteria
from runtoolsio.runcore.run import RunState, Lifecycle, PhaseMetadata, RunFailure, RunError, Run, TerminationInfo, \
    TerminationStatus, Outcome, JobInstanceMetadata
from runtoolsio.runcore.track import TrackedTask
from runtoolsio.runcore.util import MatchingStrategy, format_dt_sql, parse_dt_sql

log = logging.getLogger(__name__)


def create_database(db_conf):
    config = db_conf if db_conf else {}
    db_con = sqlite3.connect(config.get('database') or str(paths.sqlite_db_path(True)))
    sqlite_ = SQLite(db_con)
    sqlite_.check_tables_exist()
    return sqlite_


def _build_where_clause(run_match, alias=''):
    # TODO Post fetch filter for criteria not supported in WHERE (instance parameters, etc.)
    if not run_match:
        return ""

    if alias and not alias.endswith('.'):
        alias = alias + "."

    job_conditions = [f'{alias}job_id = "{j}"' for j in run_match.jobs]

    id_conditions = []
    for c in run_match.metadata_criteria:
        if c.strategy == MatchingStrategy.ALWAYS_TRUE:
            id_conditions.clear()
            break
        if c.strategy == MatchingStrategy.ALWAYS_FALSE:
            id_conditions = ['1=0']
            break

        conditions = []
        op = ' AND ' if c.match_both_ids else ' OR '
        if c.entity_id:
            if c.strategy == MatchingStrategy.PARTIAL:
                conditions.append(f'{alias}job_id GLOB "*{c.entity_id}*"')
            elif c.strategy == MatchingStrategy.FN_MATCH:
                conditions.append(f'{alias}job_id GLOB "{c.entity_id}"')
            elif c.strategy == MatchingStrategy.EXACT:
                conditions.append(f'{alias}job_id = "{c.entity_id}"')
            else:
                raise ValueError(f"Matching strategy {c.strategy} is not supported")
        if c.run_id:
            if c.strategy == MatchingStrategy.PARTIAL:
                conditions.append(f'{alias}run_id GLOB "*{c.run_id}*"')
            elif c.strategy == MatchingStrategy.FN_MATCH:
                conditions.append(f'{alias}run_id GLOB "{c.run_id}"')
            elif c.strategy == MatchingStrategy.EXACT:
                conditions.append(f'{alias}run_id = "{c.run_id}"')
            else:
                raise ValueError(f"Matching strategy {c.strategy} is not supported")

        id_conditions.append(op.join(conditions))

    int_criteria = run_match.interval_criteria
    int_conditions = []

    def dt_conditions(column, dt_range):
        conditions_dt = []
        if not dt_range:
            return conditions_dt
        if dt_range.start:
            conditions_dt.append(f"{alias}{column} >= '{format_dt_sql(dt_range.start)}'")
        if dt_range.end:
            if dt_range.end_included:
                conditions_dt.append(f"{alias}{column} <= '{format_dt_sql(dt_range.end)}'")
            else:
                conditions_dt.append(f"{alias}{column} < '{format_dt_sql(dt_range.end)}'")

        return conditions_dt

    for c in int_criteria:
        conditions = dt_conditions('created', c.created_range) + dt_conditions('ended', c.ended_range)
        if conditions:
            int_conditions.append("(" + " AND ".join(conditions) + ")")

    term_conditions = []
    if run_match.termination_criteria:
        term_conditions = []

        range_conditions = []
        for c in run_match.termination_criteria:
            start, end = c.outcome.value.start, c.outcome.value.stop
            range_condition = f"({alias}termination_status BETWEEN {start} AND {end})"
            range_conditions.append(range_condition)

        if range_conditions:
            combined_condition = " OR ".join(range_conditions)
            term_conditions.append(f"({combined_condition})")

    all_conditions_list = (job_conditions, id_conditions, int_conditions, term_conditions)
    all_conditions_str = ["(" + " OR ".join(c_list) + ")" for c_list in all_conditions_list if c_list]

    return " WHERE {conditions}".format(conditions=" AND ".join(all_conditions_str))


class SQLite(InstanceTransitionObserver):

    def __init__(self, connection):
        self._conn = connection

    def new_instance_phase(self, job_run: JobRun, previous_phase, new_phase, ordinal):
        if new_phase.run_state == RunState.ENDED:
            self.store_job_runs(job_run)

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
                         ended timestamp,
                         exec_time real,
                         phases text,
                         lifecycle text,
                         termination_status int,
                         failure text,
                         error text,
                         track text,
                         warnings text,
                         misc text)
                         ''')
            c.execute('''CREATE INDEX job_id_index ON history (job_id)''')
            c.execute('''CREATE INDEX instance_id_index ON history (instance_id)''')
            c.execute('''CREATE INDEX ended_index ON history (ended)''')  # TODO created + exec_time idx too
            log.debug('event=[table_created] table=[history]')
            self._conn.commit()

    def read_job_runs(self, run_match=None, sort=SortCriteria.ENDED, *, asc=True, limit=-1, offset=-1, last=False) \
            -> JobRuns:
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
        c = self._conn.execute(statement, (limit, offset))

        def to_job_info(t):
            metadata = JobInstanceMetadata(t[0], t[1], t[2], {}, json.loads(t[3]) if t[3] else dict())
            ended_at = parse_dt_sql(t[5])
            phases = tuple(PhaseMetadata.deserialize(p) for p in json.loads(t[7]))
            lifecycle = Lifecycle.deserialize(json.loads(t[8]))
            term_status = TerminationStatus.from_code(t[9])
            failure = RunFailure.deserialize(json.loads(t[10])) if t[10] else None
            error = RunError.deserialize(json.loads(t[11])) if t[11] else None
            task = TrackedTask.deserialize(json.loads(t[12])) if t[12] else None
            run = Run(phases, lifecycle, TerminationInfo(term_status, ended_at, failure, error))

            return JobRun(metadata, run, task)

        return JobRuns((to_job_info(row) for row in c.fetchall()))

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

    def read_stats(self, run_match=None) -> List[JobStats]:
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
                COUNT(h.warnings) AS warnings
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

    def store_job_runs(self, *job_runs):
        def to_tuple(r):
            lifecycle = r.run.lifecycle
            return (r.job_id,
                    r.run_id,
                    r.metadata.instance_id,
                    json.dumps(r.metadata.user_params) if r.metadata.user_params else None,
                    format_dt_sql(lifecycle.created_at),
                    format_dt_sql(lifecycle.ended_at),
                    round(lifecycle.total_executing_time.total_seconds(),
                          3) if lifecycle.total_executing_time else None,
                    json.dumps([p.serialize() for p in r.run.phases]),
                    json.dumps(lifecycle.serialize()),
                    r.run.termination.status.value,
                    json.dumps(r.run.termination.failure.serialize()) if r.run.termination.failure else None,
                    json.dumps(r.run.termination.error.serialize()) if r.run.termination.error else None,
                    json.dumps(r.task.serialize()) if r.task else None,
                    None,  # TODO Warnings as a separate column?
                    None
                    )

        jobs = [to_tuple(j) for j in job_runs]
        self._conn.executemany(
            "INSERT INTO history VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            jobs
        )
        self._conn.commit()

    def remove_runs(self, run_match):
        where_clause = _build_where_clause(run_match)
        if not where_clause:
            raise ValueError("No rows to remove")
        self._conn.execute("DELETE FROM history" + where_clause)
        self._conn.commit()

    def close(self):
        self._conn.close()
