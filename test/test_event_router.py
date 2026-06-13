import pytest

from runtools.runcore.job import InstanceLifecycleEvent
from runtools.runcore.listening import InstanceEventRouter
from runtools.runcore.run import Stage
from runtools.runcore.test.job import fake_job_run
from runtools.runcore.util import utc_now


class LifecycleObserver:

    def __init__(self):
        self.received = []

    def instance_lifecycle_update(self, event):
        self.received.append(event)


def wire_lifecycle(router, job_run, stage):
    event = InstanceLifecycleEvent(job_run, stage, job_run.last_updated)
    router(event.EVENT_TYPE, event.instance, event.serialize()["event"])


@pytest.fixture
def base_ts():
    return utc_now().replace(microsecond=0)


def test_routes_live_by_default(base_ts):
    router = InstanceEventRouter()
    observer = LifecycleObserver()
    router.add_observer_lifecycle(observer)

    wire_lifecycle(router, fake_job_run('j1', created_at=base_ts, term_status=None), Stage.RUNNING)

    assert len(observer.received) == 1


def test_buffering_holds_events_until_flush(base_ts):
    router = InstanceEventRouter(start_buffering=True)
    observer = LifecycleObserver()
    router.add_observer_lifecycle(observer)

    run1 = fake_job_run('j1', created_at=base_ts, term_status=None)
    run2 = fake_job_run('j2', created_at=base_ts, term_status=None)
    wire_lifecycle(router, run1, Stage.RUNNING)
    wire_lifecycle(router, run2, Stage.RUNNING)
    assert observer.received == []

    router.flush_buffer()
    assert [e.job_run.instance_id for e in observer.received] == [run1.instance_id, run2.instance_id]

    # Live after flush; flush is idempotent
    wire_lifecycle(router, run1, Stage.ENDED)
    router.flush_buffer()
    assert len(observer.received) == 3
