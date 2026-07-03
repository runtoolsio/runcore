"""Connector composition — the polling directory over the environment database."""
import pytest

from runtools.runcore.connector import compose
from runtools.runcore.db import sqlite
from runtools.runcore.run import TerminationStatus
from runtools.runcore.test.job import fake_job_run
from runtools.runcore.transport.db import PollingInstanceDirectory
from runtools.runcore.util import utc_now

BASE = utc_now().replace(microsecond=0)


def _seed_active(db, run):
    iid = run.metadata.instance_id
    db.init_run(iid.job_id, iid.run_id, created_at=run.lifecycle.created_at)
    db.store_active_runs(run)


@pytest.fixture
def env_db():
    with sqlite.create_memory('test_env') as database:
        yield database


def test_db_transport_observes_active_runs(env_db):
    _seed_active(env_db, fake_job_run('j1', created_at=BASE, term_status=None))

    # Opening the connector opens the polling directory — first poll seeds the view
    with compose('test_env', env_db, PollingInstanceDirectory(env_db), ()) as connector:
        assert [r.job_id for r in connector.get_active_runs()] == ['j1']


def test_db_transport_excludes_ended_runs(env_db):
    _seed_active(env_db, fake_job_run('running', created_at=BASE, term_status=None))
    iid = fake_job_run('done', created_at=BASE, term_status=TerminationStatus.COMPLETED).metadata.instance_id
    env_db.init_run(iid.job_id, iid.run_id, created_at=BASE)
    env_db.store_runs(fake_job_run('done', created_at=BASE, term_status=TerminationStatus.COMPLETED))

    with compose('test_env', env_db, PollingInstanceDirectory(env_db), ()) as connector:
        assert [r.job_id for r in connector.get_active_runs()] == ['running']
