from runtools.runcore.criteria import TerminationCriterion, JobRunCriteria
from runtools.runcore.run import TerminationStatus, TerminationInfo, Outcome

from runtools.runcore.util import utc_now, DateTimeRange


def test_interval_utc_conversion():
    c = JobRunCriteria().created(*DateTimeRange.parse_to_utc(from_val='2023-11-10T09:00+02:00', to_val=None))
    assert c.lifecycle_criteria[-1].created.since.hour == 7


def test_termination_criteria():
    matching = TerminationCriterion(outcome=Outcome.REJECTED)
    not_matching = TerminationCriterion(outcome=Outcome.ABORTED)

    tested_info = TerminationInfo(TerminationStatus.OVERLAP, utc_now())
    assert matching(tested_info)
    assert not not_matching(tested_info)
