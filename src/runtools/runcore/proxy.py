from abc import abstractmethod
from typing import Callable, override

from runtools.runcore.job import (
    JobRun, JobInstance, InstanceID, InstanceNotifications, InstanceObservableNotifications,
    InstanceLifecycleEvent, InstancePhaseEvent, InstanceStatusEvent,
)
from runtools.runcore.matching import MetadataCriterion
from runtools.runcore.output import Output, Mode
from runtools.runcore.run import StopReason, Stage
from runtools.runcore.transport import NodeRpcClient


class JobInstanceProxyBase(JobInstance):
    """Transport-neutral base for proxies to job instances running in another process.

    Covers the state half: initialized from a discovery snapshot and kept fresh by
    lifecycle/phase/status events from the env-wide notification hub — reads never do
    I/O. Updates are replacement-only, guarded by :meth:`JobRun.is_newer_than` so
    replayed or out-of-order events cannot regress state. Unbinds from the hub when
    the instance ends.

    Transport subclasses implement the command half: ``stop``, ``output`` and
    ``_exec_phase_op``.
    """

    def __init__(self, initial: JobRun, notifications: InstanceNotifications):
        self._job_run = initial  # Replacement-only, never mutated in place => lock-free reads
        self._source_notifications = notifications

        instance_filter = MetadataCriterion.exact_match(initial.instance_id)
        self._notifications = InstanceObservableNotifications(instance_filter=instance_filter)
        self._notifications.bind_to(notifications)
        self._notifications.add_observer_lifecycle(self._on_lifecycle_update)
        self._notifications.add_observer_phase(self._on_phase_update)
        self._notifications.add_observer_status(self._on_status_update)

    def _update_state(self, job_run: JobRun):
        if job_run.is_newer_than(self._job_run):
            self._job_run = job_run

    def _on_lifecycle_update(self, event: InstanceLifecycleEvent):
        self._update_state(event.job_run)
        if event.new_stage == Stage.ENDED:
            self._notifications.unbind_from(self._source_notifications)

    def _on_phase_update(self, event: InstancePhaseEvent):
        self._update_state(event.job_run)

    def _on_status_update(self, event: InstanceStatusEvent):
        self._update_state(event.job_run)

    @property
    def metadata(self):
        return self._job_run.metadata

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

    @property
    @abstractmethod
    def output(self) -> Output:
        pass

    @abstractmethod
    def stop(self, stop_reason= StopReason.STOPPED):
        pass

    def run(self):
        raise NotImplementedError("Remote run is not supported")

    @property
    def tracking(self):
        raise NotImplementedError("Remote status tracking not yet supported")

    @property
    @override
    def notifications(self) -> InstanceNotifications:
        return self._notifications


class UnixSocketJobInstanceProxy(JobInstanceProxyBase):
    """Proxy to a job instance reachable over the unix-socket transport."""

    def __init__(self, initial: JobRun, notifications: InstanceNotifications,
                 client: NodeRpcClient, server_address: str):
        super().__init__(initial, notifications)
        self._client = client
        self._server_address = server_address
        self._output = _ProxyOutput(client, server_address, initial.instance_id)

    @property
    def output(self) -> Output:
        return self._output

    def stop(self, stop_reason=StopReason.STOPPED):
        self._client.stop_instance(self._server_address, self.id, stop_reason)

    def _exec_phase_op(self, phase_id: str, op_name: str, *op_args):
        return self._client.exec_phase_op(self._server_address, self.id, phase_id, op_name, *op_args)


class _ProxyOutput(Output):

    def __init__(self, client: NodeRpcClient, server_address: str, instance_id: InstanceID):
        self._client = client
        self._server_address = server_address
        self._instance_id = instance_id

    @property
    def locations(self):
        return ()

    def tail(self, mode: Mode = Mode.TAIL, max_lines: int = 0):
        return self._client.get_output_tail(self._server_address, self._instance_id, max_lines)


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
