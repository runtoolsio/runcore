import pytest

from runtools.runcore.db import sqlite
from runtools.runcore.matching import JobRunCriteria
from runtools.runcore.test.job import fake_job_run
from runtools.runcore.transport.db import DbInstanceDiscovery


@pytest.fixture
def db():
    with sqlite.create_memory('test_env') as db:
        yield db


def _init_active(db, run):
    iid = run.metadata.instance_id
    db.init_run(iid.job_id, iid.run_id, run.metadata.user_params, created_at=run.lifecycle.created_at)
    db.store_active_runs(run)


def test_discovers_active_runs_complete(db):
    run = fake_job_run('j1', term_status=None)
    _init_active(db, run)

    discovered = DbInstanceDiscovery(db).discover_active_runs()

    assert discovered.complete  # A single-source DB sweep is always complete
    assert [r.metadata.instance_id for r in discovered.runs] == [run.metadata.instance_id]


def test_discovery_passes_run_match(db):
    _init_active(db, fake_job_run('j1', term_status=None))
    _init_active(db, fake_job_run('j2', term_status=None))

    discovered = DbInstanceDiscovery(db).discover_active_runs(JobRunCriteria.parse('j1'))

    assert [r.metadata.instance_id.job_id for r in discovered.runs] == ['j1']
