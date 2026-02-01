from typing import override

from runtools.runcore.client import TargetNotFoundError, InstanceCallError
from runtools.runcore.criteria import JobRunCriteria, MetadataCriterion
from runtools.runcore.err import RuntoolsException
from runtools.runcore.job import (
    JobRun, JobInstance, InstanceID, InstanceNotifications, InstanceObservableNotifications,
    InstanceLifecycleEvent,
)
from runtools.runcore.output import Output, Mode
from runtools.runcore.run import StopReason, Stage


class _ProxyOutput(Output):

    def __init__(self, client, server_address, instance_id):
        self._client = client
        self._server_address = server_address
        self._instance_id = instance_id

    @property
    def locations(self):
        return ()

    def tail(self, mode: Mode = Mode.TAIL, max_lines: int = 0):
        return self._client.get_output_tail(self._server_address, self._instance_id, max_lines)


class JobInstanceProxy(JobInstance):
    """Proxy to a job instance running in another process."""

    def __init__(self, client, server_address, instance_id, connector_notifications: InstanceNotifications):
        self._client = client
        self._server_address = server_address
        self._instance_id = instance_id
        self._connector_notifications = connector_notifications
        try:
            job_runs = client.get_active_runs(server_address, JobRunCriteria.instance_match(instance_id))
        except TargetNotFoundError:
            raise ProxyInstanceNotFoundError(server_address, instance_id)
        except InstanceCallError as e:
            raise ProxyInstanceUnavailableError(server_address, instance_id) from e
        if not job_runs:
            raise ProxyInstanceNotFoundError(server_address, instance_id)
        self._job_run: JobRun = job_runs[0]
        self._output = _ProxyOutput(client, server_address, instance_id)

        instance_filter = MetadataCriterion.exact_match(instance_id)
        self._notifications = InstanceObservableNotifications(instance_filter=instance_filter)
        self._notifications.bind_to(connector_notifications)
        self._notifications.add_observer_lifecycle(self._on_lifecycle_update)

    def _on_lifecycle_update(self, event: InstanceLifecycleEvent):
        if event.new_stage == Stage.ENDED:
            self._notifications.unbind_from(self._connector_notifications)

    @property
    def metadata(self):
        return self._job_run.metadata

    def find_phase_control(self, phase_filter):
        phase = self._job_run.find_first_phase(phase_filter)
        if not phase:
            return None
        return PhaseControlProxy(self._client, self._server_address, self._instance_id, phase.phase_id)

    def to_run(self):
        return self._job_run

    @property
    def output(self) -> Output:
        return self._output

    def run(self):
        pass

    def stop(self, stop_reason=StopReason.STOPPED):
        self._client.stop_instance(self._server_address, self._instance_id, stop_reason)

    @property
    @override
    def notifications(self) -> InstanceNotifications:
        return self._notifications


class PhaseControlProxy:
    """Proxy for controlling a phase in another process.

    This class provides a clean interface for executing operations on a phase
    through the LocalInstanceClient.
    """

    def __init__(self, client, server_address, instance_id, phase_id: str):
        self._phase_id = phase_id
        self._client = client
        self._server_address = server_address
        self._instance_id = instance_id

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
        return self._client.exec_phase_op(self._server_address, self._instance_id, self._phase_id, op_name, *op_args)

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


class ProxyInstanceError(RuntoolsException):
    pass


class ProxyInstanceNotFoundError(ProxyInstanceError):
    """Exception raised when a job instance cannot be found.

    Args:
        server_address: Address of the server where the instance was expected
        instance_id: ID of the job instance that could not be found
    """

    def __init__(self, server_address: str, instance_id: InstanceID):
        self.server_address = server_address
        self.instance_id = instance_id
        super().__init__(f"Job instance '{instance_id}' not found on server '{server_address}'")


class ProxyInstanceUnavailableError(ProxyInstanceError):
    """Exception raised when a job instance cannot be reached due to an error.

    Args:
        server_address: Address of the server where the instance was expected
        instance_id: ID of the job instance that could not be reached
    """

    def __init__(self, server_address: str, instance_id: InstanceID):
        self.server_address = server_address
        self.instance_id = instance_id
        super().__init__(f"Error reaching job instance '{instance_id}' on server '{server_address}'")
