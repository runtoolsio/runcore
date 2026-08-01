"""Behaviour of ``ObservableNotification`` demand hooks — fired on first/last observer transitions only."""
from runtools.runcore.util.observer import ObservableNotification


def _hooked():
    transitions = []
    notification = ObservableNotification(
        on_first_observer=lambda: transitions.append('first'),
        on_last_observer=lambda: transitions.append('last'))
    return notification, transitions


def test_first_observer_fires_hook():
    notification, transitions = _hooked()

    notification.add_observer(lambda e: None)

    assert transitions == ['first']


def test_second_observer_does_not_refire_hook():
    notification, transitions = _hooked()

    notification.add_observer(lambda e: None)
    notification.add_observer(lambda e: None)

    assert transitions == ['first']


def test_last_observer_removal_fires_hook():
    notification, transitions = _hooked()
    observer = lambda e: None

    notification.add_observer(observer)
    notification.remove_observer(observer)

    assert transitions == ['first', 'last']


def test_removal_with_remaining_observers_does_not_fire_hook():
    notification, transitions = _hooked()
    first, second = (lambda e: None), (lambda e: None)
    notification.add_observer(first)
    notification.add_observer(second)

    notification.remove_observer(first)

    assert transitions == ['first']


def test_removing_unregistered_observer_does_not_fire_hook():
    notification, transitions = _hooked()

    notification.remove_observer(lambda e: None)

    assert transitions == []


def test_demand_retransition_fires_hooks_again():
    notification, transitions = _hooked()
    observer = lambda e: None

    notification.add_observer(observer)
    notification.remove_observer(observer)
    notification.add_observer(observer)

    assert transitions == ['first', 'last', 'first']


def test_hooks_are_optional():
    notification = ObservableNotification()
    observer = lambda e: None

    notification.add_observer(observer)
    notification.remove_observer(observer)

    assert notification.observers == []
