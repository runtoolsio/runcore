from datetime import timedelta

from runtools.runcore.job import JobRun, JobInstanceMetadata, InstanceID
from runtools.runcore.run import RunState, \
    TerminationStatus, Fault
from runtools.runcore.test.run import FakePhaseDetailBuilder
from runtools.runcore.util import utc_now, unique_timestamp_hex

INIT = 'init'
APPROVAL = 'approval'
PROGRAM = 'program'
TERM = 'term'


def job_run(job_id, phase, *, run_id=None, user_params=None):
    run_id = run_id or unique_timestamp_hex()
    meta = JobInstanceMetadata(InstanceID(job_id, run_id), user_params or {})
    return JobRun(meta, phase.lifecycle, phase.children)  # TODO Faults and status


def fake_job_run(job_id, run_id='r1', *, created_at=None, offset_min=0, ended_at=None,
                 term_status=TerminationStatus.COMPLETED) -> JobRun:
    start_time = (created_at or utc_now().replace(microsecond=0)) + timedelta(minutes=offset_min)
    phase_builder = FakePhaseDetailBuilder.root(base_ts=start_time)
    phase_builder.add_phase(APPROVAL, RunState.PENDING, TerminationStatus.COMPLETED)

    if not term_status:
        phase_builder.add_phase(PROGRAM, RunState.EXECUTING)
    elif term_status == TerminationStatus.FAILED:
        phase_builder.add_phase(
            PROGRAM, RunState.EXECUTING, term_status, term_ts=ended_at or (start_time + timedelta(minutes=5)),
            fault=Fault('err1', 'reason'))
    else:
        phase_builder.add_phase(
            PROGRAM, RunState.EXECUTING, term_status, term_ts=ended_at or (start_time + timedelta(minutes=5)))

    return job_run(job_id, phase_builder.build(), run_id=run_id, user_params={'name': 'value'})
