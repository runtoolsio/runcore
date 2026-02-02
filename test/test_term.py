from runtools.runcore.run import TerminationStatus, Outcome


def test_get_statuses():
    fault_statuses = TerminationStatus.get_statuses(Outcome.FAULT)
    assert TerminationStatus.FAILED in fault_statuses
    assert TerminationStatus.ERROR in fault_statuses
    assert TerminationStatus.UNKNOWN in fault_statuses  # UNKNOWN uses FAULT as fail-safe
    assert len(fault_statuses) == 3

    aborted_statuses = TerminationStatus.get_statuses(Outcome.ABORTED)
    assert TerminationStatus.CANCELLED in aborted_statuses
    assert TerminationStatus.STOPPED in aborted_statuses
    assert TerminationStatus.INTERRUPTED in aborted_statuses
    assert TerminationStatus.SIGNAL in aborted_statuses


def test_get_outcomes():
    success_outcomes = Outcome.get_outcomes(success=True)
    assert success_outcomes == {Outcome.SUCCESS}

    nonsuccess_outcomes = Outcome.get_outcomes(success=False)
    assert nonsuccess_outcomes == {Outcome.ABORTED, Outcome.REJECTED, Outcome.FAULT}
