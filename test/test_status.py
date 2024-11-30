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
   # TODO
    pass
