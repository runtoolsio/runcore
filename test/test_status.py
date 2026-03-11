from datetime import datetime, timedelta, UTC

from runtools.runcore.status import Operation, Status, Event
from runtools.runcore.util import utc_now


def test_operation_str():
    empty_op = Operation('name', None, None, None, utc_now(), utc_now())
    assert str(empty_op) == '[name]'

    assert '[Op 25/100 files (25%)]' == str(Operation('Op', 25, 100, 'files', utc_now(), utc_now()))


def test_progress_str():
    progress = Operation('', 25, 100, 'files', utc_now(), utc_now())
    assert str(progress) == '[25/100 files (25%)]'

    progress = Operation('', None, 100, 'files', utc_now(), utc_now())
    assert str(progress) == '[?/100 files (0%)]'

    progress = Operation('', 20, None, 'files', utc_now(), utc_now())
    assert str(progress) == '[20 files]'


def test_status_str():
    now = datetime.now(UTC)

    # Test empty status
    assert str(Status(None, [], [], None)) == ""

    # Test with just an event
    assert str(Status(Event("Processing", now), [], [], None)) == "Processing"

    # Test with single operation
    op = Operation("Copy", 45, 100, "files", now, now)
    assert str(Status(None, [op], [], None)) == "[Copy 45/100 files (45%)]"

    # Test with operation and event
    assert str(Status(Event("Working batch 2", now), [op], [], None)) == "[Copy 45/100 files (45%)]"

    # Test with multiple operations and event
    ops = [
        Operation("Copy", 45, 100, "files", now, now),
        Operation("Validate", 20, 50, "records", now, now)
    ]
    assert str(Status(Event("Processing batch 2", now), ops, [],
                      None)) == "[Copy 45/100 files (45%)] [Validate 20/50 records (40%)]"

    # Test with finished operation (should not show in status)
    finished_op = Operation("Copy", 100, 100, "files", now, now)
    assert finished_op.finished
    assert str(Status(Event("Finalizing", now), [finished_op], [], None)) == "Finalizing"

    # Test with result (should override everything else)
    assert str(Status(Event("Processing", now), [op], [], Event("Completed successfully", now))) == "Completed successfully"


def test_status_str_with_warnings():
    now = datetime.now(UTC)

    assert str(Status(None, [], [Event("Low disk space", now)], None)) == "(!Low disk space)"

    assert str(Status(None, [], [Event("Low disk space", now), Event("Network unstable", now)],
                      None)) == "(!Low disk space, Network unstable)"

    assert str(
        Status(Event("Processing", now), [], [Event("Low disk space", now)], None)) == "Processing  (!Low disk space)"

    op = Operation("Copy", 45, 100, "files", now, now)
    assert str(
        Status(None, [op], [Event("Low disk space", now)], None)) == "[Copy 45/100 files (45%)]  (!Low disk space)"

    assert str(
        Status(Event("Processing batch 2", now), [op], [Event("Low disk space", now), Event("Network unstable", now)],
               None)) == "[Copy 45/100 files (45%)]  (!Low disk space, Network unstable)"

    # Test that warnings are included with result
    assert str(Status(Event("Processing", now), [op], [Event("Low disk space", now)],
                      Event("Completed", now))) == "Completed  (!Low disk space)"

    # Test result with multiple warnings
    assert str(
        Status(None, [], [Event("Error 1", now), Event("Error 2", now)], Event("Failed", now))) == "Failed  (!Error 1, Error 2)"


def test_operation_str_with_result():
    now = utc_now()

    op = Operation("Copy", 100, 100, "files", now, now, result="Done")
    assert str(op) == "[Copy 100/100 files (100%) Done]"

    op = Operation("Validation", None, None, None, now, now, result="Failed")
    assert str(op) == "[Validation Failed]"

    # Test just progress and result, no name
    op = Operation("", 20, 50, "records", now, now, result="Failed")
    assert str(op) == "[20/50 records (40%) Failed]"

    op = Operation("Process", 75, 100, "items", now, now, result="Cancelled" )
    assert str(op) == "[Process 75/100 items (75%) Cancelled]"

    op = Operation("Check", None, None, None, now, now, result="Error: timeout")
    assert str(op) == "[Check Error: timeout]"

    op = Operation("", None, None, None, now, now, result="Done")
    assert str(op) == "[Done]"


def test_pct_done_with_total_only():
    now = utc_now()

    # total set, completed None → 0%
    op = Operation("Upload", None, 15495, "products", now, now)
    assert op.pct_done == 0.0
    assert op.has_progress
    assert str(op) == "[Upload ?/15495 products (0%)]"

    # completed=0 explicitly → also 0%
    op = Operation("Upload", 0, 15495, "products", now, now)
    assert op.pct_done == 0.0
    assert str(op) == "[Upload 0/15495 products (0%)]"

    # total=0 → no pct
    op = Operation("Upload", None, 0, None, now, now)
    assert op.pct_done is None


def test_operation_failed():
    now = utc_now()

    op = Operation("Upload", None, None, None, now, now, result="timeout", failed=True)
    assert op.finished
    assert op.failed
    assert op.finished_summary == "Upload ✗ timeout"

    # Success case for comparison
    op_ok = Operation("Upload", 100, 100, "files", now, now, result="done")
    assert op_ok.finished_summary == "Upload ✓ 100 files (done)"


def test_operation_failed_with_completed():
    now = utc_now()

    op = Operation("Copy", 50, 100, "files", now, now, result="disk full", failed=True)
    assert op.finished
    assert op.finished_summary == "Copy ✗ 50 files (disk full)"


def test_operation_failed_serialization():
    now = utc_now()

    op = Operation("Upload", None, None, None, now, now, result="error", failed=True)
    data = op.serialize()
    assert data['failed'] is True

    restored = Operation.deserialize(data)
    assert restored.failed is True
    assert restored.result == "error"


def test_operation_failed_false_not_serialized():
    now = utc_now()

    op = Operation("Upload", None, None, None, now, now, result="done")
    data = op.serialize()
    assert 'failed' not in data

    restored = Operation.deserialize(data)
    assert restored.failed is False


def test_finished_summary_with_elapsed():
    now = utc_now()
    later = now + timedelta(seconds=154)

    op = Operation("Sort", 110724, 110724, "lines", now, later, result="15495 products")
    assert op.finished_summary == "Sort ✓ 110724 lines (15495 products) 2m34s"

    op_failed = Operation("Upload", None, None, None, now, later, result="s3 access denied", failed=True)
    assert op_failed.finished_summary == "Upload ✗ s3 access denied 2m34s"

    # Zero elapsed — no time appended
    op_instant = Operation("Check", None, None, None, now, now, result="ok")
    assert op_instant.finished_summary == "Check ✓ ok"
