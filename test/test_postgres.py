"""Postgres storage contract tests.

These run against a real Postgres: by default a throwaway container started via testcontainers
(one per session), or an existing database if RUNTOOLS_PG_TEST_DSN is set. They create and drop
the schema each test, so a provided DSN must point at a throwaway database. The only skip is when
Docker is unavailable and no DSN is given — Postgres is a first-class backend, not an optional one.
"""
import os

import pytest

from runtools.runcore.db import EnvironmentStoreNotProvisionedError, IncompatibleSchemaError, postgres
from runtools.runcore.db.postgres import PostgreSQL, SCHEMA_VERSION
from runtools.runcore.env import EnvironmentConfig, EnvironmentEntry
from runtools.runcore.job import DuplicateInstanceError, InstanceID
from runtools.runcore.matching import JobRunCriteria, LifecycleCriterion, MetadataCriterion, PhaseCriterion
from runtools.runcore.run import Stage, TerminationStatus
from runtools.runcore.test.job import fake_job_run
from runtools.runcore.util import MatchingStrategy

POSTGRES_IMAGE = "postgres:16-alpine"

parse = JobRunCriteria.parse


def _docker_available() -> bool:
    try:
        import docker
        docker.from_env().ping()
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def pg_dsn():
    """A libpq DSN for a real Postgres — an existing one via RUNTOOLS_PG_TEST_DSN, else a
    throwaway testcontainers instance for the session."""
    dsn = os.getenv("RUNTOOLS_PG_TEST_DSN")
    if dsn:
        yield dsn
        return
    if not _docker_available():
        pytest.skip("Postgres tests need Docker (or RUNTOOLS_PG_TEST_DSN)")
    from testcontainers.postgres import PostgresContainer
    with PostgresContainer(POSTGRES_IMAGE) as container:
        host, port = container.get_container_host_ip(), container.get_exposed_port(5432)
        yield f"postgresql://{container.username}:{container.password}@{host}:{port}/{container.dbname}"


def _provision(entry):
    """Admin-side provisioning (DDL + seed config) — open() is validate-only now."""
    postgres.create_environment(entry, EnvironmentConfig.default_local(entry.id))


@pytest.fixture
def sut(pg_dsn):
    entry = EnvironmentEntry(id="test_env", driver="postgres", location=pg_dsn)
    postgres.delete(entry)  # reset schema between tests (and clean up any prior leftovers)
    _provision(entry)
    db = postgres.create(entry)
    db.open()
    try:
        yield db
    finally:
        db.close()
        postgres.delete(entry)


def _init_and_store(db, *job_runs):
    for run in job_runs:
        iid = run.metadata.instance_id
        db.init_run(iid.job_id, iid.run_id, run.metadata.user_params,
                    created_at=run.lifecycle.created_at, tags=run.metadata.tags)
    db.store_runs(*job_runs)


def _init_active(db, run):
    iid = run.metadata.instance_id
    db.init_run(iid.job_id, iid.run_id, run.metadata.user_params, created_at=run.lifecycle.created_at)
    db.store_active_runs(run)


# --- schema ---

def test_schema_version_set_on_creation(sut):
    with sut._pool.connection() as conn:
        assert conn.execute("SELECT version FROM schema_info").fetchone()[0] == SCHEMA_VERSION


def test_schema_version_mismatch_raises(sut, pg_dsn):
    with sut._pool.connection() as conn:
        conn.execute("UPDATE schema_info SET version = 999")
    sut.close()
    reopened = postgres.create(EnvironmentEntry(id="test_env", driver="postgres", location=pg_dsn))
    with pytest.raises(IncompatibleSchemaError):
        reopened.open()


# --- lifecycle: provision (privileged) vs open (validate) ---

def test_open_unprovisioned_raises(pg_dsn):
    """open() must not provision — an unprovisioned env raises rather than silently creating it."""
    entry = EnvironmentEntry(id="unprov_env", driver="postgres", location=pg_dsn)
    postgres.delete(entry)
    db = postgres.create(entry)
    try:
        with pytest.raises(EnvironmentStoreNotProvisionedError):
            db.open()
        assert not postgres.exists(entry)  # nothing was created
        assert not db.is_open()            # pool was closed on the failed open, not leaked
    finally:
        db.close()
        postgres.delete(entry)


# --- environment isolation (schema-per-env on a shared database) ---

def test_exists_false_before_create_true_after(pg_dsn):
    entry = EnvironmentEntry(id="ex_env", driver="postgres", location=pg_dsn)
    postgres.delete(entry)
    try:
        assert not postgres.exists(entry)
        _provision(entry)
        assert postgres.exists(entry)
    finally:
        postgres.delete(entry)


def test_two_envs_same_dsn_are_isolated(pg_dsn):
    a = EnvironmentEntry(id="env_a", driver="postgres", location=pg_dsn)
    b = EnvironmentEntry(id="env_b", driver="postgres", location=pg_dsn)
    for e in (a, b):
        postgres.delete(e)
        _provision(e)
    db_a, db_b = postgres.create(a), postgres.create(b)
    db_a.open()
    db_b.open()
    try:
        _init_and_store(db_a, fake_job_run('only_in_a'))
        db_a.save_config('env_a', {'transport': {'type': 'a'}})
        db_b.save_config('env_b', {'transport': {'type': 'b'}})

        # Same database, different schemas → neither sees the other's runs or config.
        assert [r.metadata.instance_id.job_id for r in db_a.read_runs()] == ['only_in_a']
        assert db_b.read_runs() == []
        assert db_a.load_config('env_a')['transport'] == {'type': 'a'}
        assert db_b.load_config('env_b')['transport'] == {'type': 'b'}
    finally:
        db_a.close()
        db_b.close()
        for e in (a, b):
            postgres.delete(e)


def test_schema_name_bounded_and_collision_free_for_long_ids():
    # Two long ids sharing a 60-char prefix must map to distinct, ≤63-byte schema names —
    # the hash suffix disambiguates even after the readable part truncates.
    base = "x" * 60
    a = postgres._schema_name(base + "_a")
    b = postgres._schema_name(base + "_b")
    assert a != b
    assert len(a) <= 63 and len(b) <= 63


def test_long_id_envs_are_isolated(pg_dsn):
    base = "y" * 60
    a = EnvironmentEntry(id=base + "_a", driver="postgres", location=pg_dsn)
    b = EnvironmentEntry(id=base + "_b", driver="postgres", location=pg_dsn)
    for e in (a, b):
        postgres.delete(e)
        _provision(e)
    db_a, db_b = postgres.create(a), postgres.create(b)
    db_a.open()
    db_b.open()
    try:
        _init_and_store(db_a, fake_job_run('in_a'))
        assert [r.metadata.instance_id.job_id for r in db_a.read_runs()] == ['in_a']
        assert db_b.read_runs() == []  # distinct schemas despite the shared 60-char prefix
    finally:
        db_a.close()
        db_b.close()
        for e in (a, b):
            postgres.delete(e)


def test_delete_one_env_leaves_the_other(pg_dsn):
    a = EnvironmentEntry(id="env_a", driver="postgres", location=pg_dsn)
    b = EnvironmentEntry(id="env_b", driver="postgres", location=pg_dsn)
    for e in (a, b):
        postgres.delete(e)
        _provision(e)
    try:
        postgres.delete(a)
        assert not postgres.exists(a)
        assert postgres.exists(b)  # dropping one schema must not touch the other
    finally:
        for e in (a, b):
            postgres.delete(e)


def test_open_works_without_ddl_privilege(pg_dsn):
    """The point of the lifecycle split: a role with no DDL rights can open() an existing env
    (validate + read), but cannot provision one (create_environment → DDL → denied)."""
    import psycopg
    from psycopg import conninfo, errors, sql

    role, pw = "rt_readonly_test", "rt_pw"
    entry = EnvironmentEntry(id="role_env", driver="postgres", location=pg_dsn)
    other = EnvironmentEntry(id="role_env_other", driver="postgres", location=pg_dsn)

    # Admin: try to create a login role with no CREATE on the database. Skip if we can't.
    try:
        with psycopg.connect(pg_dsn, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role)))
            admin.execute(sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                sql.Identifier(role), sql.Literal(pw)))
    except errors.InsufficientPrivilege:
        pytest.skip("test DSN role cannot CREATE ROLE")

    limited = conninfo.conninfo_to_dict(pg_dsn) | {"user": role, "password": pw}
    limited_dsn = conninfo.make_conninfo(**limited)
    try:
        postgres.delete(entry)
        _provision(entry)  # admin provisions
        schema = postgres._schema_name(entry.id)
        with psycopg.connect(pg_dsn, autocommit=True) as admin:
            admin.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                sql.Identifier(schema), sql.Identifier(role)))
            admin.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}").format(
                sql.Identifier(schema), sql.Identifier(role)))

        # Read-only role: open() validates + reads, no DDL needed.
        db = PostgreSQL(limited_dsn, entry.id)
        db.open()
        try:
            assert db.read_runs() == []
        finally:
            db.close()

        # Same role: provisioning a new env requires DDL → denied.
        postgres.delete(other)
        with pytest.raises(errors.InsufficientPrivilege):
            postgres.create_environment(
                EnvironmentEntry(id=other.id, driver="postgres", location=limited_dsn),
                EnvironmentConfig.default_local(other.id))
    finally:
        postgres.delete(entry)
        postgres.delete(other)
        with psycopg.connect(pg_dsn, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role)))


# --- round-trip / reads ---

def test_store_and_fetch_round_trips_via_native_types(sut):
    run = fake_job_run('j1', term_status=TerminationStatus.FAILED)
    _init_and_store(sut, run)

    [restored] = sut.read_runs()
    assert restored == run  # JSONB/TIMESTAMPTZ serialization is lossless


def test_last(sut):
    _init_and_store(sut,
        fake_job_run('j1', 'r1-1', offset_min=1),
        fake_job_run('j2', 'r2-1', offset_min=2),
        fake_job_run('j1', 'r1-2', offset_min=3),
        fake_job_run('j3', 'r3-1', offset_min=4),
        fake_job_run('j2', 'r2-2', offset_min=5))

    jobs = sut.read_runs(last=True)
    assert [job.run_id for job in jobs] == ['r1-2', 'r3-1', 'r2-2']


def test_sort_and_direction(sut):
    _init_and_store(sut, fake_job_run('j1'), fake_job_run('j2', offset_min=1), fake_job_run('j3', offset_min=-1))

    assert [j.job_id for j in sut.read_runs()] == ['j3', 'j1', 'j2']
    assert [j.job_id for j in sut.read_runs(asc=False)] == ['j2', 'j1', 'j3']


def test_limit_and_offset(sut):
    _init_and_store(sut, fake_job_run('1'), fake_job_run('2', offset_min=1), fake_job_run('3', offset_min=-1))

    assert [j.job_id for j in sut.read_runs(limit=1)] == ['3']
    assert [j.job_id for j in sut.read_runs(offset=2)] == ['2']


def test_job_id_match(sut):
    _init_and_store(sut, fake_job_run('j1', 'i1'), fake_job_run('j12', 'i12'), fake_job_run('j11', 'i11'))

    assert len(sut.read_runs(parse('j1'))) == 1                          # EXACT
    assert len(sut.read_runs(parse('j1', MatchingStrategy.PARTIAL))) == 3  # PARTIAL (contains)


def test_partial_match_uses_regex_search(sut):
    _init_and_store(sut, fake_job_run('ja'), fake_job_run('jb'), fake_job_run('xx'))

    # PARTIAL is re.search: 'j.' matches 'ja'/'jb' but not 'xx'. A LIKE prefilter would miss these.
    matched = sut.read_runs(parse('j.', MatchingStrategy.PARTIAL))
    assert sorted(r.metadata.instance_id.job_id for r in matched) == ['ja', 'jb']


def test_fn_match_supports_bracket_classes(sut):
    _init_and_store(sut, fake_job_run('job1'), fake_job_run('job2'), fake_job_run('job3'))

    # FN_MATCH is fnmatch: 'job[12]' matches job1/job2 only. A LIKE prefilter would drop the class.
    matched = sut.read_runs(parse('job[12]', MatchingStrategy.FN_MATCH))
    assert sorted(r.metadata.instance_id.job_id for r in matched) == ['job1', 'job2']


def test_last_breaks_timestamp_ties(sut):
    # Two runs of the same job sharing an ended timestamp must still collapse to one.
    _init_and_store(sut, fake_job_run('j1', 'ra', offset_min=0), fake_job_run('j1', 'rb', offset_min=0))

    jobs = sut.read_runs(last=True)
    assert len([r for r in jobs if r.metadata.instance_id.job_id == 'j1']) == 1


def test_last_picks_newest_among_matching_runs(sut):
    # j1's newest run lacks the tag; an older one has it. last=True must return the older match,
    # not "nothing" (the newest-overall run is excluded by the criteria).
    older, newer = fake_job_run('j1', 'old', offset_min=1), fake_job_run('j1', 'new', offset_min=2)
    sut.init_run('j1', 'old', created_at=older.lifecycle.created_at, tags=('keep',))
    sut.init_run('j1', 'new', created_at=newer.lifecycle.created_at)
    sut.store_runs(older, newer)

    result = sut.read_runs(JobRunCriteria.builder().tags('keep').build(), last=True)
    assert [r.metadata.instance_id.run_id for r in result] == ['old']


def test_remove_runs_applies_phase_criteria(sut):
    _init_and_store(sut, fake_job_run('j1', 'r1'), fake_job_run('j1', 'r2'))

    # Job matches in SQL, but no run has this phase — nothing should be deleted (the SQL
    # prefilter alone would wrongly delete every j1 row).
    crit = (JobRunCriteria.builder()
            .metadata(MetadataCriterion(job_id='j1'))
            .phase(PhaseCriterion(phase_id='does-not-exist'))
            .build())
    assert sut.remove_runs(crit) == []
    assert len(sut.read_runs(parse('j1'))) == 2


def test_tag_match(sut):
    r1, r2 = fake_job_run('j1'), fake_job_run('j2')
    sut.init_run('j1', 'r1', created_at=r1.lifecycle.created_at, tags=('nightly',))
    sut.init_run('j2', 'r1', created_at=r2.lifecycle.created_at, tags=('adhoc',))
    sut.store_runs(r1, r2)

    matched = sut.read_runs(JobRunCriteria.builder().tags('nightly').build())
    assert [r.metadata.instance_id.job_id for r in matched] == ['j1']


def test_lifecycle_stage_match_excludes_active(sut):
    _init_and_store(sut, fake_job_run('done', term_status=TerminationStatus.COMPLETED))
    _init_active(sut, fake_job_run('running', term_status=None))

    ended = sut.read_runs(JobRunCriteria.builder().lifecycle(LifecycleCriterion.reached_stage(Stage.ENDED)).build())
    assert [r.metadata.instance_id.job_id for r in ended] == ['done']


# --- active reads ---

def test_read_active_runs_returns_only_persisted_active(sut):
    _init_active(sut, fake_job_run('active', term_status=None))
    _init_and_store(sut, fake_job_run('ended', term_status=TerminationStatus.COMPLETED))
    init_only = fake_job_run('pending', term_status=None)
    iid = init_only.metadata.instance_id
    sut.init_run(iid.job_id, iid.run_id, created_at=init_only.lifecycle.created_at)  # no snapshot yet

    active = sut.read_active_runs()
    assert [r.metadata.instance_id.job_id for r in active] == ['active']


def test_read_active_runs_post_filters_phase_criteria(sut):
    _init_active(sut, fake_job_run('j1', term_status=None))

    no_such_phase = JobRunCriteria.builder().phase(PhaseCriterion(phase_id='does-not-exist')).build()
    assert sut.read_active_runs(no_such_phase) == []


def test_store_active_runs_does_not_overwrite_ended(sut):
    run = fake_job_run('j1', term_status=TerminationStatus.COMPLETED)
    iid = run.metadata.instance_id
    sut.init_run(iid.job_id, iid.run_id, created_at=run.lifecycle.created_at)
    sut.store_runs(run)  # terminal row

    stale = fake_job_run('j1', run_id=iid.run_id, term_status=None)
    sut.store_active_runs(stale)

    assert sut.read_active_runs() == []  # guard kept the terminal row; nothing resurrected


# --- stats / remove / retention / config ---

def test_read_run_stats(sut):
    _init_and_store(sut,
        fake_job_run('j1', 'r1', offset_min=1, term_status=TerminationStatus.COMPLETED),
        fake_job_run('j1', 'r2', offset_min=2, term_status=TerminationStatus.FAILED))

    [stats] = sut.read_run_stats()
    assert stats.job_id == 'j1'
    assert stats.count == 2
    assert stats.termination_status == TerminationStatus.FAILED  # last (latest ended) run


def test_read_run_stats_applies_phase_criteria(sut):
    _init_and_store(sut, fake_job_run('j1', term_status=TerminationStatus.COMPLETED))
    assert len(sut.read_run_stats()) == 1  # the run is present

    # No run has this phase: stats must exclude it, not ignore the predicate and fold it in.
    crit = JobRunCriteria.builder().phase(PhaseCriterion(phase_id='does-not-exist')).build()
    assert sut.read_run_stats(crit) == []


def test_metadata_only_match_includes_init_only_rows(sut):
    # An init-only row (reserved, no snapshot yet) is still matchable by metadata-only criteria:
    # remove_runs goes through the non-SQL post-filter path and must select it via metadata.
    run = fake_job_run('job1', term_status=None)
    sut.init_run('job1', run.metadata.run_id, created_at=run.lifecycle.created_at)  # no snapshot

    crit = parse('job', MatchingStrategy.PARTIAL)
    assert [iid.job_id for iid in sut.remove_runs(crit)] == ['job1']  # matched + deleted via metadata
    assert sut.remove_runs(crit) == []                               # and it's gone


def test_read_run_stats_last_run_deterministic_on_ties(sut):
    # Same job, same ended timestamp, different outcomes: the PK tiebreaker (run_id DESC) makes
    # the "last run" deterministic — 'rb' wins, so its FAILED status is reported.
    _init_and_store(sut,
        fake_job_run('j1', 'ra', offset_min=0, term_status=TerminationStatus.COMPLETED),
        fake_job_run('j1', 'rb', offset_min=0, term_status=TerminationStatus.FAILED))

    [stats] = sut.read_run_stats()
    assert stats.termination_status == TerminationStatus.FAILED


def test_remove_runs(sut):
    _init_and_store(sut, fake_job_run('keep'), fake_job_run('drop'))

    removed = sut.remove_runs(parse('drop'))
    assert [iid.job_id for iid in removed] == ['drop']
    assert [r.metadata.instance_id.job_id for r in sut.read_runs()] == ['keep']


def test_config_round_trip(sut):
    sut.save_config('test_env', {'transport': {'type': 'unix_socket'}, 'plugins': ['p1']})

    config = sut.load_config('test_env')
    assert config['id'] == 'test_env'
    assert config['transport'] == {'type': 'unix_socket'}
    assert config['plugins'] == ['p1']


# --- init / duplicates ---

def test_duplicate_init_raises(sut):
    sut.init_run('j1', 'r1', created_at=fake_job_run('j1', 'r1').lifecycle.created_at)
    with pytest.raises(DuplicateInstanceError):
        sut.init_run('j1', 'r1', created_at=fake_job_run('j1', 'r1').lifecycle.created_at)


def test_auto_increment_allocates_sequential_ordinals(sut):
    created = fake_job_run('j1', 'r1').lifecycle.created_at
    ids = [sut.init_run('j1', 'r1', created_at=created, auto_increment=True) for _ in range(3)]
    assert [iid.ordinal for iid in ids] == [1, 2, 3]
