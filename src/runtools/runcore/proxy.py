import bisect
from abc import abstractmethod
from threading import Lock
from typing import Callable, List, override

from runtools.runcore.job import (
    JobRun, JobInstance, InstanceNotifications, InstanceObservableNotifications,
    InstanceLifecycleEvent, InstanceOutputEvent, InstancePhaseEvent, InstanceStatusEvent,
    NotificationBinding,
)
from runtools.runcore.matching import MetadataCriterion
from runtools.runcore.output import Mode, Output, OutputLine
from runtools.runcore.run import StopReason


class JobInstanceProxyBase(JobInstance):
    """Base class for job instance proxies.

    A proxy represents an instance whose state and commands are mediated by a
    transport implementation. It keeps a local :class:`JobRun` snapshot and
    updates that snapshot from state events. Snapshot reads are local; commands
    and output reads are delegated to the concrete transport.

    State updates are monotonic: older snapshots are ignored according to
    :meth:`JobRun.is_newer_than`.

    Subclasses implement command delivery and remote output fetches.
    """

    def __init__(self, initial: JobRun, output_buffer_depth: int = 100):
        self._job_run = initial  # Replacement-only, never mutated in place => lock-free reads
        self._output = _ProxyOutput(self._fetch_output_tail, output_buffer_depth)
        self._notifications = InstanceObservableNotifications(
            instance_filter=(MetadataCriterion.exact_match(initial.instance_id)))
        self._notifications.add_observer_lifecycle(self._on_lifecycle_update)
        self._notifications.add_observer_phase(self._on_phase_update)
        self._notifications.add_observer_status(self._on_status_update)
        self._notifications.add_observer_output(self._on_output_update)

    @property
    def metadata(self):
        return self._job_run.metadata

    def _update_state(self, job_run: JobRun):
        """Apply a possibly-newer snapshot of this instance.

        Guarded by :meth:`JobRun.is_newer_than` — older snapshots (stale discovery
        results, replayed events) never regress the cached state.
        """
        if job_run.is_newer_than(self._job_run):
            self._job_run = job_run

    def bind_to(self, source: InstanceNotifications) -> NotificationBinding:
        """Subscribe this proxy to an upstream instance event source.

        The proxy is inert until bound — construction has no external effects.
        The caller (the directory) owns the returned binding and unbinds it when
        the instance leaves the view.
        """
        return self._notifications.bind_to(source)

    def _on_lifecycle_update(self, event: InstanceLifecycleEvent):
        self._update_state(event.job_run)

    def _on_phase_update(self, event: InstancePhaseEvent):
        self._update_state(event.job_run)

    def _on_status_update(self, event: InstanceStatusEvent):
        self._update_state(event.job_run)

    def _on_output_update(self, event: InstanceOutputEvent):
        self._output.add_line(event.output_line)

    def snap(self) -> JobRun:
        return self._job_run

    def find_phase_control(self, phase_filter):
        phase = self._job_run.find_first_phase(phase_filter)
        if not phase:
            return None
        return PhaseControlProxy(self._exec_phase_op, phase.phase_id)

    @abstractmethod
    def _exec_phase_op(self, phase_id: str, op_name: str, *op_args):
        """Execute an operation on a phase of the remote instance."""

    @abstractmethod
    def _fetch_output_tail(self, mode: Mode, max_lines: int) -> List[OutputLine]:
        """Fetch lines from the remote instance's tail buffer."""

    @property
    def output(self) -> Output:
        return self._output

    @abstractmethod
    def stop(self, stop_reason= StopReason.STOPPED):
        pass

    def run(self):
        raise NotImplementedError("Remote run is not supported")

    @property
    @override
    def notifications(self) -> InstanceNotifications:
        return self._notifications


class _ProxyOutput(Output):
    """Bounded buffer of output events received since proxy construction.

    TAIL reads covered by the buffer are served locally. Reads the buffer cannot
    prove complete are served directly from the remote instance; fetched lines are
    intentionally not merged into the event-fed buffer.

    The buffer is kept sorted by line ordinal and drops duplicate deliveries, so
    transport delivery order is not load-bearing — the output analog of the
    :meth:`JobRun.is_newer_than` guard on state.
    """

    def __init__(self, fetch_tail: Callable[[Mode, int], List[OutputLine]], max_lines: int):
        self._fetch_tail = fetch_tail
        self._max_lines = max_lines
        self._lines: List[OutputLine] = []
        self._lock = Lock()

    @property
    def locations(self):
        return ()

    def add_line(self, line: OutputLine):
        with self._lock:
            if not self._lines or line.ordinal > self._lines[-1].ordinal:
                self._lines.append(line)  # Fast path: in-order delivery
            else:
                index = bisect.bisect_left(self._lines, line.ordinal, key=lambda l: l.ordinal)
                if index < len(self._lines) and self._lines[index].ordinal == line.ordinal:
                    return  # Duplicate delivery
                self._lines.insert(index, line)
            del self._lines[:-self._max_lines]

    def tail(self, mode: Mode = Mode.TAIL, max_lines: int = 0):
        with self._lock:
            lines = list(self._lines)
        if mode == Mode.TAIL and 0 < max_lines <= len(lines):
            return lines[-max_lines:]
        return self._fetch_tail(mode, max_lines)


class PhaseControlProxy:
    """Proxy for controlling a phase in another process.

    Delegates operations to the owning instance proxy's transport-specific
    ``_exec_phase_op``.
    """

    def __init__(self, exec_phase_op: Callable, phase_id: str):
        self._exec_phase_op = exec_phase_op
        self._phase_id = phase_id

    def exec_op(self, op_name: str, *op_args):
        """Execute an operation on the phase.

        Args:
            op_name: Name of the operation to execute
            op_args: Arguments to pass to the operation

        Returns:
            Operation-specific return value

        Raises:
            PhaseNotFoundError: If the phase doesn't exist on the server
            TargetNotFoundError: If the instance or server is not found
            InstanceCallServerError: For server-side errors during execution
            InstanceCallClientError: For client-side errors during execution
        """
        return self._exec_phase_op(self._phase_id, op_name, *op_args)

    def __getattr__(self, name):
        """Dynamic method resolution to enable natural operation calling.

        This allows calling operations directly as methods on the phase control object.
        For example: phase_control.pause() instead of phase_control.exec_op('pause')

        Args:
            name: Name of the operation/attribute to access

        Returns:
            A callable function that delegates to exec_op if name doesn't exist as an attribute
        """
        # Only intercept methods that don't exist as actual attributes
        if name.startswith('_'):
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

        def method_proxy(*args):
            return self.exec_op(name, *args)

        return method_proxy
