from runtools.runcore.client import TargetNotFoundError, RemoteCallError
from runtools.runcore.criteria import JobRunCriteria
from runtools.runcore.err import RuntoolsException
from runtools.runcore.job import JobRun, JobInstance, InstanceID
from runtools.runcore.util.observer import DEFAULT_OBSERVER_PRIORITY


class JobInstanceRemote(JobInstance):

    def __init__(self, client, server_address, instance_id):
        self._client = client
        self._server_address = server_address
        self._instance_id = instance_id
        try:
            job_runs = client.get_active_runs(server_address, JobRunCriteria.instance_match(instance_id))
        except TargetNotFoundError:
            raise RemoteInstanceNotFoundError(server_address, instance_id)
        except RemoteCallError as e:
            raise RemoteInstanceUnavailableError(server_address, instance_id) from e
        if not job_runs:
            raise RemoteInstanceNotFoundError(server_address, instance_id)
        self._job_run: JobRun = job_runs[0]

    @property
    def metadata(self):
        return self._job_run.metadata

    def find_phase_control(self, phase_filter):
        phase = self._job_run.find_first_phase(phase_filter)
        if not phase:
            return None
        return PhaseControlRemote(self._client, self._server_address, self._instance_id, phase.phase_id)

    def snapshot(self):
        return self._job_run

    @property
    def output(self):
        pass

    def run(self):
        pass

    def stop(self):
        self._client.stop_instance(self._server_address, self._instance_id)

    def interrupted(self):
        pass

    def add_observer_lifecycle(self, observer, priority=DEFAULT_OBSERVER_PRIORITY, reply_last_event=False):
        pass

    def remove_observer_lifecycle(self, observer):
        pass

    def add_observer_transition(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        pass

    def remove_observer_transition(self, observer):
        pass


class PhaseControlRemote:
    """Remote proxy for controlling a specific phase of a job instance.

    This class provides a clean interface for executing operations on a remote
    phase through the RemoteCallClient.
    """

    def __init__(self, client, server_address, instance_id, phase_id: str):
        self._phase_id = phase_id
        self._client = client
        self._server_address = server_address
        self._instance_id = instance_id

    def exec_op(self, op_name: str, *op_args):
        """Execute an operation on the remote phase.

        Args:
            op_name: Name of the operation to execute
            op_args: Arguments to pass to the operation

        Returns:
            Operation-specific return value

        Raises:
            PhaseNotFoundError: If the phase doesn't exist on the remote server
            TargetNotFoundError: If the instance or server is not found
            RemoteCallServerError: For server-side errors during execution
            RemoteCallClientError: For client-side errors during execution
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


class RemoteInstanceError(RuntoolsException):
    pass


class RemoteInstanceNotFoundError(RemoteInstanceError):
    """Exception raised when a remote job instance cannot be found.

    Args:
        server_address: Address of the server where the instance was expected
        instance_id: ID of the job instance that could not be found
    """

    def __init__(self, server_address: str, instance_id: InstanceID):
        self.server_address = server_address
        self.instance_id = instance_id
        super().__init__(f"Job instance '{instance_id}' not found on server '{server_address}'")


class RemoteInstanceUnavailableError(RemoteInstanceError):
    """Exception raised when a remote job instance cannot be reached due to an error.

    Args:
        server_address: Address of the server where the instance was expected
        instance_id: ID of the job instance that could not be reached
    """

    def __init__(self, server_address: str, instance_id: InstanceID):
        self.server_address = server_address
        self.instance_id = instance_id
        super().__init__(f"Error reaching job instance '{instance_id}' on server '{server_address}'")
