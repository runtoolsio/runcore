from _operator import itemgetter
from itertools import chain
from types import FunctionType, MethodType
from typing import List, Tuple, Any, Callable, Optional, TypeVar, Generic

DEFAULT_OBSERVER_PRIORITY = 100


class CallableNotification:

    def __init__(self, *, error_hook: Optional[Callable[[Callable, Tuple[Any], Exception], None]] = None,
                 force_reraise=False):
        self.error_hook: Optional[Callable[[Callable, Tuple[Any, ...], Exception], None]] = error_hook
        self.force_reraise = force_reraise
        self._prioritized_observers = []

    def __call__(self, *args):
        self.notify_all(*args)

    @property
    def observers(self) -> List[Callable[..., Any]]:
        return [o for _, o in self._prioritized_observers]

    @property
    def prioritized_observers(self) -> List[Tuple[int, Callable[..., Any]]]:
        return list(self._prioritized_observers)

    def add_observer(self, observer: Callable[..., Any], priority: int = DEFAULT_OBSERVER_PRIORITY) -> None:
        self._prioritized_observers = sorted(
            chain(self._prioritized_observers, [(priority, observer)]),
            key=itemgetter(0))

    def remove_observer(self, observer: Callable[..., Any]) -> None:
        self._prioritized_observers = [(priority, o) for priority, o in self._prioritized_observers if o != observer]

    def notify_all(self, *args):
        exceptions = []
        for _, observer in self._prioritized_observers:
            try:
                observer(*args)
            except Exception as e:
                if self.error_hook:
                    self.error_hook(observer, args, e)
                if not self.error_hook or self.force_reraise:
                    exceptions.append(e)

        if exceptions:
            raise ExceptionGroup("Observer exception(s) occurred", exceptions)


O = TypeVar("O")


class ObservableNotification(Generic[O]):

    def __init__(self, *,
                 event_filter: Optional[Callable[[Any], bool]] = None,
                 error_hook: Optional[Callable[[O, Tuple[Any], Exception], None]] = None,
                 force_reraise=False):
        self.event_filter = event_filter
        self.error_hook: Optional[Callable[[O, Tuple[Any], Exception], None]] = error_hook
        self.force_reraise = force_reraise
        self._prioritized_observers = []
        self._observer_proxy = _Proxy(self, force_reraise)

    @property
    def observer_proxy(self) -> O:
        return self._observer_proxy

    @property
    def observers(self) -> List[O]:
        return [o for _, o in self._prioritized_observers]

    @property
    def prioritized_observers(self) -> List[Tuple[int, O]]:
        return list(self._prioritized_observers)

    def add_observer(self, observer: O, priority: int = DEFAULT_OBSERVER_PRIORITY) -> None:
        self._prioritized_observers = sorted(
            chain(self._prioritized_observers, [(priority, observer)]),
            key=itemgetter(0))

    def remove_observer(self, observer: O) -> None:
        self._prioritized_observers = [(priority, o) for priority, o in self._prioritized_observers if o != observer]

    def observer_context(self, *observers: O, priority: int = DEFAULT_OBSERVER_PRIORITY) -> 'ObserverContext[O]':
        return ObserverContext(self, *observers, priority=priority)


class _Proxy(Generic[O]):

    def __init__(self, notification: ObservableNotification, force_reraise) -> None:
        self._notification = notification
        self._force_reraise = force_reraise

    def __getattribute__(self, name: str) -> object:
        def method(*args, **kwargs):
            notification = object.__getattribute__(self, "_notification")

            if notification.event_filter:
                event = args[0] if args else None
                if event and not notification.event_filter(event):
                    return

            exceptions = []
            for observer in notification.observers:
                try:
                    if isinstance(observer, (FunctionType, MethodType)) or (
                            callable(observer) and not hasattr(observer, name)):
                        observer(*args, **kwargs)
                    else:
                        getattr(observer, name)(*args, **kwargs)
                except Exception as e:
                    if notification.error_hook:
                        notification.error_hook(observer, args, e)
                    if not notification.error_hook or self._force_reraise:
                        exceptions.append(e)

            if exceptions:
                raise ExceptionGroup("Observer exception(s) occurred", exceptions)

        # Special handling for methods/attributes that are specific to the proxy object itself
        if name in ["_notification"] or name.startswith("__"):
            return object.__getattribute__(self, name)

        return method


class ObserverContext(Generic[O]):

    def __init__(self, notification: ObservableNotification[O], *observers: O, priority: int = DEFAULT_OBSERVER_PRIORITY):
        self._notification = notification
        self._observers = observers
        self._priority = priority

    def __enter__(self) -> O:
        for observer in self._observers:
            self._notification.add_observer(observer, self._priority)

    def __exit__(self, exc_type, exc_val, exc_tb):
        for observer in self._observers:
            self._notification.remove_observer(observer)
