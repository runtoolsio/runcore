from runtoolsio.runcore.criteria import LifecycleCriterion, TerminationCriterion
from runtoolsio.runcore.run import TerminationStatus, TerminationInfo, Outcome

from runtoolsio.runcore.util import utc_now


def test_interval_utc_conversion():
    c = LifecycleCriterion.to_utc(from_val='2023-11-10T09:00+02:00', to_val=None)
    assert c.created_range.start.hour == 7


def test_termination_criteria():
    matching = TerminationCriterion(outcome=Outcome.REJECTED)
    not_matching = TerminationCriterion(outcome=Outcome.ABORTED)

    tested_info = TerminationInfo(TerminationStatus.INVALID_OVERLAP, utc_now())
    assert matching(tested_info)
    assert not not_matching(tested_info)
