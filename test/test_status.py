from datetime import datetime, UTC

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
    assert str(progress) == '[?/100 files]'

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
    assert str(Status(Event("Working batch 2", now), [op], [], None)) == "[Copy 45/100 files (45%)]...  Working batch 2"

    # Test with multiple operations and event
    ops = [
        Operation("Copy", 45, 100, "files", now, now),
        Operation("Validate", 20, 50, "records", now, now)
    ]
    assert str(Status(Event("Processing batch 2", now), ops, [],
                      None)) == "[Copy 45/100 files (45%)] [Validate 20/50 records (40%)]...  Processing batch 2"

    # Test with finished operation (should not show in status)
    finished_op = Operation("Copy", 100, 100, "files", now, now, active=False)
    assert str(Status(Event("Finalizing", now), [finished_op], [], None)) == "Finalizing"

    # Test with result (should override everything else)
    assert str(Status(Event("Processing", now), [op], [], "Completed successfully")) == "Completed successfully"


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
               None)) == "[Copy 45/100 files (45%)]...  Processing batch 2  (!Low disk space, Network unstable)"

    # Test that warnings are included with result
    assert str(Status(Event("Processing", now), [op], [Event("Low disk space", now)],
                      "Completed")) == "Completed  (!Low disk space)"

    # Test result with multiple warnings
    assert str(
        Status(None, [], [Event("Error 1", now), Event("Error 2", now)], "Failed")) == "Failed  (!Error 1, Error 2)"
