from runtools.runcore.status import StatusTracker, Operation
from runtools.runcore.util import parse_datetime, utc_now


def test_add_event():
    tracker = StatusTracker()
    tracker.event('e1')
    tracker.event('e2')

    assert tracker.to_status().last_event.text == 'e2'


def test_operation_updates():
    tracker = StatusTracker()
    tracker.operation('op1').update(1, 10, 'items')

    op1 = tracker.to_status().operations[0]
    assert op1.name == 'op1'
    assert op1.completed == 1
    assert op1.total == 10
    assert op1.unit == 'items'


def test_operation_incr_update():
    tracker = StatusTracker()
    tracker.operation('op1').update(1)
    tracker.operation('op1').update(1)

    op1 = tracker.to_status().operations[0]
    assert op1.name == 'op1'
    assert op1.completed == 2
    assert op1.total is None
    assert op1.unit == ''

    tracker.operation('op2').update(5, 10)
    tracker.operation('op2').update(4, 10)
    op1 = tracker.to_status().operations[1]
    assert op1.completed == 9
    assert op1.total == 10


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
    tracker = StatusTracker()
    assert str(tracker.to_status()) == ""

    # Test with just an event
    tracker = StatusTracker()
    tracker.event("Processing")
    assert str(tracker.to_status()) == "Processing"

    # Test with single operation
    tracker = StatusTracker()
    tracker.operation("Copy").update(45, 100, "files")
    assert str(tracker.to_status()) == "[Copy 45/100 files (45%)]"

    # Test with operation and event
    tracker = StatusTracker()
    tracker.operation("Copy").update(45, 100, "files")
    tracker.event("Processing batch 2")
    assert str(tracker.to_status()) == "[Copy 45/100 files (45%)]...  Processing batch 2"

    # Test with multiple operations and event
    tracker = StatusTracker()
    tracker.operation("Copy").update(45, 100, "files")
    tracker.operation("Validate").update(20, 50, "records")
    tracker.event("Processing batch 2")
    assert str(tracker.to_status()) == "[Copy 45/100 files (45%)] [Validate 20/50 records (40%)]...  Processing batch 2"

    # Test with finished operation (should not show in status)
    tracker = StatusTracker()
    op = tracker.operation("Copy")
    op.update(100, 100, "files")
    op.active = False
    tracker.event("Finalizing")
    assert str(tracker.to_status()) == "Finalizing"

    # Test with result (should override everything else)
    tracker = StatusTracker()
    tracker.operation("Copy").update(45, 100, "files")
    tracker.event("Processing")
    tracker.result("Completed successfully")
    assert str(tracker.to_status()) == "Completed successfully"
