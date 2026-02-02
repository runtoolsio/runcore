from runtools.runcore.criteria import TerminationCriterion, JobRunCriteria
from runtools.runcore.run import TerminationStatus, TerminationInfo, Outcome

from runtools.runcore.util import utc_now, DateTimeRange


def test_interval_utc_conversion():
    c = JobRunCriteria().created(*DateTimeRange.parse_to_utc(from_val='2023-11-10T09:00+02:00', to_val=None))
    assert c.lifecycle_criteria[-1].created.since.hour == 7


def test_termination_criteria_by_outcome():
    matching = TerminationCriterion(outcome=Outcome.REJECTED)
    not_matching = TerminationCriterion(outcome=Outcome.ABORTED)

    tested_info = TerminationInfo(TerminationStatus.OVERLAP, utc_now())
    assert matching(tested_info)
    assert not not_matching(tested_info)


def test_termination_criteria_by_success():
    success_criterion = TerminationCriterion(success=True)
    nonsuccess_criterion = TerminationCriterion(success=False)

    completed_info = TerminationInfo(TerminationStatus.COMPLETED, utc_now())
    failed_info = TerminationInfo(TerminationStatus.FAILED, utc_now())
    aborted_info = TerminationInfo(TerminationStatus.STOPPED, utc_now())

    assert success_criterion(completed_info)
    assert not success_criterion(failed_info)
    assert not success_criterion(aborted_info)

    assert not nonsuccess_criterion(completed_info)
    assert nonsuccess_criterion(failed_info)
    assert nonsuccess_criterion(aborted_info)
