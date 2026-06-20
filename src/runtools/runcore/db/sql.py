"""SQL building shared across relational ``EnvironmentDatabase`` drivers.

The translation of a ``JobRunCriteria`` into a parameterized ``WHERE`` clause is the same
for every relational backend; only a few specifics differ (parameter marker, pattern-match
operator, how a datetime is bound). Each driver supplies those as a :class:`Dialect`, so the
matching logic itself is written and tested once here.

What the clause covers: instance id (job/run/ordinal), tags, and lifecycle (stage, datetime
ranges, run time, termination). It does NOT cover phase or user-param criteria — drivers must
post-filter results with the full ``run_match`` as the correctness backstop.
"""

from dataclasses import dataclass
from datetime import timedelta
from typing import Callable

from runtools.runcore.job import JobStats
from runtools.runcore.matching import LifecycleCriterion, MatchingStrategy, SortOption
from runtools.runcore.run import Outcome, Stage, TerminationStatus


@dataclass(frozen=True)
class Dialect:
    """The SQL specifics a relational driver supplies to the criteria translator.

    Attributes:
        placeholder: Parameter marker for the driver's DB-API (``?`` for sqlite, ``%s`` for psycopg).
        bind_dt: Maps a domain datetime to the parameter value for a timestamp-column comparison.
    """
    placeholder: str
    bind_dt: Callable


def is_sql_complete(run_match) -> bool:
    """Whether :func:`build_where_clause` expresses ``run_match`` exactly (no post-filter needed).

    The clause covers lifecycle criteria and EXACT id/tag matches; it cannot express phase
    criteria or the PARTIAL/FN_MATCH strategies (``re.search`` / ``fnmatch``), which only the
    in-memory matcher evaluates. When this returns False, the clause is a superset and the caller
    must apply the full ``run_match``.
    """
    if run_match is None:
        return True
    if getattr(run_match, 'phase_criteria', ()):
        return False
    return all(c.strategy in (MatchingStrategy.EXACT, MatchingStrategy.ALWAYS_TRUE)
               for c in getattr(run_match, 'metadata_criteria', ()))


def matches_metadata(run_match, metadata) -> bool:
    """Apply only ``run_match``'s metadata criteria — usable on init-only rows that have no JobRun.

    Mirrors ``JobRunCriteria.matches_metadata``; each criterion handles its own exclude logic.
    """
    criteria = getattr(run_match, 'metadata_criteria', ())
    return not criteria or any(c(metadata) for c in criteria)


def matching_pks(rows, run_match, to_job_run: Callable, to_metadata: Callable) -> list:
    """(job_id, run_id, ordinal) of ``rows`` satisfying the *full* ``run_match``.

    For criteria the SQL prefilter only approximates (phase, PARTIAL/FN_MATCH): rows with a
    snapshot match as full JobRuns; init-only rows (no ``root_phase``) match on metadata only,
    since they can't satisfy a phase criterion. ``to_job_run``/``to_metadata`` deserialize a row
    (per-backend). ``rows`` must expose ``root_phase``/``job_id``/``run_id``/``ordinal`` by key.
    """
    has_phase = bool(getattr(run_match, 'phase_criteria', ()))
    pks = []
    for r in rows:
        ok = run_match(to_job_run(r)) if r['root_phase'] is not None \
            else (not has_phase and matches_metadata(run_match, to_metadata(r)))
        if ok:
            pks.append((r['job_id'], r['run_id'], r['ordinal']))
    return pks


def build_where_clause(run_match, dialect: Dialect, alias: str = '') -> tuple[str, list, bool]:
    """Build a parameterized ``WHERE`` clause (incl. leading ``" WHERE "``), its params, and a
    ``complete`` flag.

    ``complete`` (see :func:`is_sql_complete`) is False when the clause is only a superset of
    ``run_match`` — the caller must then post-filter with the full matcher. Returns
    ``("", [], True)`` when ``run_match`` imposes no constraint.
    """
    if not run_match:
        return "", [], True

    complete = is_sql_complete(run_match)
    ph = dialect.placeholder
    if alias and not alias.endswith('.'):
        alias = alias + "."
    # Inside the correlated tag subquery, bare column names bind to run_tags, not the
    # outer runs row — reference the outer table explicitly.
    outer_ref = alias if alias else 'runs.'

    conditions: list[str] = []
    params: list = []

    def exact_predicate(column: str, strategy: MatchingStrategy):
        # EXACT is the only id/tag strategy faithfully expressible in SQL; PARTIAL (re.search)
        # and FN_MATCH (fnmatch) are left unconstrained for the post-filter (see is_sql_complete).
        return f"{column} = {ph}" if strategy == MatchingStrategy.EXACT else None

    def id_fragment(field: str, pattern: str, strategy: MatchingStrategy):
        predicate = exact_predicate(f"{alias}{field}", strategy)
        return (predicate, pattern) if predicate else (None, None)

    def tag_fragment(pattern: str, strategy: MatchingStrategy):
        predicate = exact_predicate("t.tag", strategy)
        if not predicate:
            return None, None
        exists = (f"EXISTS (SELECT 1 FROM run_tags t "
                  f"WHERE t.job_id={outer_ref}job_id AND t.run_id={outer_ref}run_id "
                  f"AND t.ordinal={outer_ref}ordinal AND {predicate})")
        return exists, pattern

    def metadata_clause(c) -> tuple[str | None, list]:
        """One criterion's predicate, or ``None`` if it constrains nothing."""
        parts: list[str] = []
        clause_params: list = []

        if c.strategy != MatchingStrategy.ALWAYS_TRUE:
            id_parts: list[str] = []
            id_params: list = []
            for field, value in (('job_id', c.job_id), ('run_id', c.run_id)):
                if not value:
                    continue
                frag, val = id_fragment(field, value, c.strategy)
                if frag is not None:
                    id_parts.append(frag)
                    id_params.append(val)
            if id_parts:
                join_op = ' OR ' if c.match_any_field else ' AND '
                parts.append('(' + join_op.join(id_parts) + ')')
                clause_params.extend(id_params)

            # Tags always AND (each pattern must find a matching run-tag). The bare-token
            # tag-axis search is OR'd in as a separate criterion at the JobRunCriteria level.
            for pat in c.tags:
                frag, val = tag_fragment(pat, c.strategy)
                if frag is not None:
                    parts.append(frag)
                    clause_params.append(val)

        if c.ordinal is not None:
            parts.append(f'{alias}ordinal = {ph}')
            clause_params.append(c.ordinal)

        if not parts:
            return None, []
        return '(' + ' AND '.join(parts) + ')', clause_params

    metadata_conditions: list[str] = []
    metadata_params: list = []
    exclude_conditions: list[str] = []
    exclude_params: list = []
    match_all_seen = False
    for c in run_match.metadata_criteria:
        if c.exclude is not None:
            excl = c.exclude
            exclude_conditions.append(
                f"NOT ({alias}job_id = {ph} AND {alias}run_id = {ph} AND {alias}ordinal = {ph})")
            exclude_params.extend([excl.job_id, excl.run_id, excl.ordinal])

        clause, clause_params = metadata_clause(c)
        if clause is None:
            # Unconstrained criterion OR'd with anything is match-all: drop accumulated
            # metadata predicates (excludes still apply).
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

    def datetime_conditions(column: str, dt_range) -> tuple[list[str], list]:
        if not dt_range:
            return [], []
        if dt_range.is_unbounded():
            return [f"{alias}{column} IS NOT NULL"], []
        conds, prms = [], []
        if dt_range.since:
            conds.append(f"{alias}{column} >= {ph}")
            prms.append(dialect.bind_dt(dt_range.since))
        if dt_range.until:
            conds.append(f"{alias}{column} {'<=' if dt_range.until_included else '<'} {ph}")
            prms.append(dialect.bind_dt(dt_range.until))
        return conds, prms

    def time_range_conditions(time_range) -> tuple[list[str], list]:
        conds, prms = [], []
        if time_range.min is not None:
            conds.append(f"{alias}exec_time >= {ph}")
            prms.append(time_range.min.total_seconds())
        if time_range.max is not None:
            conds.append(f"{alias}exec_time <= {ph}")
            prms.append(time_range.max.total_seconds())
        return conds, prms

    def lifecycle_conditions(lc: LifecycleCriterion) -> tuple[list[str], list]:
        if not lc:
            return [], []
        conds, prms = [], []

        if lc.stage:
            match lc.stage:
                case Stage.CREATED:
                    conds.append(f"{alias}started IS NULL")
                case Stage.RUNNING:
                    conds.append(f"{alias}started IS NOT NULL AND {alias}ended IS NULL")
                case Stage.ENDED:
                    conds.append(f"{alias}ended IS NOT NULL")

        for column, dt_range in (('created', lc.created), ('started', lc.started), ('ended', lc.ended)):
            c, p = datetime_conditions(column, dt_range)
            conds.extend(c)
            prms.extend(p)

        if lc.total_run_time:
            c, p = time_range_conditions(lc.total_run_time)
            conds.extend(c)
            prms.extend(p)

        if lc.termination:
            term = lc.termination
            if term.status:
                conds.append(f"{alias}termination_status = {ph}")
                prms.append(term.status.value)
            if term.outcome is not None:
                statuses = TerminationStatus.get_statuses(term.outcome)
                conds.append(f"{alias}termination_status IN ({', '.join([ph] * len(statuses))})")
                prms.extend(s.value for s in statuses)
            if term.success is not None:
                outcomes = Outcome.get_outcomes(success=term.success)
                statuses = TerminationStatus.get_statuses(*outcomes)
                conds.append(f"{alias}termination_status IN ({', '.join([ph] * len(statuses))})")
                prms.extend(s.value for s in statuses)
            if term.ended_range:
                c, p = datetime_conditions('ended', term.ended_range)
                conds.extend(c)
                prms.extend(p)

        return conds, prms

    lifecycle_groups: list[str] = []
    lifecycle_params: list = []
    for lc in run_match.lifecycle_criteria:
        conds, prms = lifecycle_conditions(lc)
        if conds:
            lifecycle_groups.append('(' + ' AND '.join(conds) + ')')
            lifecycle_params.extend(prms)

    if lifecycle_groups:
        conditions.append('(' + ' OR '.join(lifecycle_groups) + ')')
        params.extend(lifecycle_params)

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
    return where, params, complete


# --- "last run per job" — one definition shared by both backends ---
#
# The newest *ended* run per job, with a primary-key tiebreaker for determinism when two runs
# share an ended timestamp. The SQL form is for the no-criteria path (a single restriction over
# the whole table); when criteria the SQL can't express are present, the matcher must run first,
# so the Python form reduces the already-matched runs instead.

# Parameterless predicate; references the outer query's ``h`` alias. ROW_NUMBER + row-value IN are
# portable across SQLite (3.25+) and Postgres.
LAST_PER_JOB_SQL = (
    "(h.job_id, h.run_id, h.ordinal) IN ("
    " SELECT job_id, run_id, ordinal FROM ("
    "  SELECT job_id, run_id, ordinal,"
    "   ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY ended DESC, run_id DESC, ordinal DESC) AS rn"
    "  FROM runs WHERE ended IS NOT NULL"
    " ) ranked WHERE rn = 1)"
)


def last_run_ids(runs) -> set:
    """instance_ids of the newest run per job among ``runs`` (by ended time, PK tiebreak).

    The Python counterpart of :data:`LAST_PER_JOB_SQL`, for when criteria the SQL can't express
    must be applied before choosing the last run per job.
    """
    latest = {}
    for run in runs:
        iid = run.metadata.instance_id
        key = (run.lifecycle.last_transition_at, iid.run_id, iid.ordinal)
        if iid.job_id not in latest or key > latest[iid.job_id][0]:
            latest[iid.job_id] = (key, iid)
    return {iid for _, iid in latest.values()}


# --- ORDER BY — shared sort-option mapping, dialect supplies only the specifics ---

# The runs columns that every backend sorts by identically (all under the ``h`` alias).
# SortOption.TIME is dialect-specific (computed vs stored), so it is passed in separately.
SORT_COLUMNS = {
    SortOption.CREATED: "h.created",
    SortOption.STARTED: "h.started",
    SortOption.ENDED: "h.ended",
    SortOption.JOB_ID: "h.job_id",
    SortOption.RUN_ID: "h.run_id",
}


def build_order_by(sort: SortOption, asc: bool, *, time_expr: str, tiebreaker, nulls_last: bool = False) -> str:
    """Build an ``ORDER BY`` body (no leading keyword) shared by the relational drivers.

    Args:
        sort: the requested sort option.
        asc: ascending when True, descending otherwise.
        time_expr: SQL expression for ``SortOption.TIME`` (e.g. stored ``exec_time`` or a computed
            duration) — differs per backend.
        tiebreaker: columns appended for a deterministic, stable order (``rowid`` or the PK).
        nulls_last: append ``NULLS LAST`` to the primary column (Postgres; SQLite keeps its default).
    """
    column = time_expr if sort == SortOption.TIME else SORT_COLUMNS.get(sort)
    if column is None:
        raise ValueError(f"Unsupported sort option: {sort}")
    d = "ASC" if asc else "DESC"
    columns = [f"{column} {d}{' NULLS LAST' if nulls_last else ''}"]
    columns += [f"{col} {d}" for col in tiebreaker]
    return ", ".join(columns)


def build_job_stats(row, parse_dt: Callable) -> JobStats:
    """Map a ``read_run_stats`` result row to :class:`JobStats`.

    The aggregate columns are aliased identically by both backends; only the stored datetime
    representation differs, so ``parse_dt`` converts the created-timestamp columns to the
    domain's naive UTC (text parse for sqlite, tz-strip for postgres).
    """
    def seconds(value):
        return timedelta(seconds=value) if value is not None else None

    return JobStats(
        job_id=row['job_id'],
        count=row['count'],
        first_created=parse_dt(row['first_created']),
        last_created=parse_dt(row['last_created']),
        fastest_time=seconds(row['fastest_time']),
        average_time=seconds(row['average_time']),
        slowest_time=seconds(row['slowest_time']),
        last_time=seconds(row['last_time']),
        termination_status=(
            TerminationStatus.from_code(row['last_term_status'])
            if row['last_term_status'] is not None else TerminationStatus.UNKNOWN
        ),
        success_count=row['succeeded'] or 0,
        failed_count=row['failed'] or 0,
        aborted_count=row['aborted'] or 0,
        rejected_count=row['rejected'] or 0,
        ignored_count=row['ignored'] or 0,
        warning_count=row['last_warnings'] or 0,
    )
