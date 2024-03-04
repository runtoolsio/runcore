"""
This is the core component of runtools defining the main constructs like run, phase, job, instance etc.
"""

__version__ = "0.1.1"

from types import MappingProxyType

from runtools.runcore import db
from runtools.runcore.client import AggregatedResponse, APIClient, ApprovalResponse, StopResponse, OutputResponse, \
    SignalDispatchResponse
from runtools.runcore.common import RuntoolsException
from runtools.runcore.db import Persistence, SortCriteria
from runtools.runcore.job import JobRun
from runtools.runcore.listening import InstanceTransitionReceiver

_current_persistence = 'sqlite'
_persistence = {}


def configure(**kwargs):
    persistence_obj = kwargs.get('persistence', {"type": _current_persistence})
    if "type" not in persistence_obj:
        raise InvalidConfiguration("Field `type` is mandatory in `persistence` configuration object")
    configure_persistence(persistence_obj["type"], persistence_obj.get(_current_persistence, {}))


def configure_persistence(persistence_type, persistence_config=None):
    global _current_persistence
    _current_persistence = persistence_type
    _persistence[_current_persistence] = MappingProxyType(persistence_config or {})


class InvalidConfiguration(RuntoolsException):

    def __init__(self, message: str):
        super().__init__(message)


def persistence(persistence_type=None) -> Persistence:
    db_type = persistence_type or _current_persistence
    if not db_type:
        raise PersistenceDisabledError

    return db.load_database_module(db_type).create_database(_persistence[db_type])


def read_job_runs(run_match, sort=SortCriteria.ENDED, *, asc=True, limit=-1, offset=0, last=False):
    with persistence() as p:
        return p.read_job_runs(run_match, sort, asc=asc, limit=limit, offset=offset, last=last)


def read_job_stats(run_match=None):
    with persistence() as p:
        return p.read_job_stats(run_match)


class PersistenceDisabledError(Exception):
    """
    Raised when attempting an operation while persistence is disabled.
    Any code using persistence should always catch this exception.
    """

    def __init__(self):
        super().__init__("Cannot perform persistence operation because persistence is disabled.")


def api_client():
    return APIClient()


def get_active_runs(run_match=None) -> AggregatedResponse[JobRun]:
    """
    Retrieves instance information for all active job instances for the current user.

    Args:
        run_match (JobRunAggregatedCriteria, optional):
            A filter for instance matching. If provided, only instances that match will be included.

    Returns:
        A container holding the :class:`JobRun` objects that represent job instances.
        It also includes any errors that may have happened, each one related to a specific server API.

    Raises:
        PayloadTooLarge: If the payload size exceeds the maximum limit.
    """

    with api_client() as c:
        return c.get_active_runs(run_match)


def approve_pending_instances(run_match, phase_id=None) -> AggregatedResponse[ApprovalResponse]:
    """
    This function releases job instances that are pending in the provided group
    and optionally match the provided criteria.

    Args:
        run_match (InstanceMatchCriteria, mandatory):
            The operation will affect only instances matching these criteria or all instances if not provided.
        phase_id (str, optional):
            ID of the approval phase.

    Returns:
        A container holding :class:`ReleaseResponse` objects, each representing the result of the release operation
        for a respective job instance.
        It also includes any errors that may have happened, each one related to a specific server API.
    """

    with api_client() as c:
        return c.approve_pending_instances(run_match, phase_id)


def stop_instances(run_match) -> AggregatedResponse[StopResponse]:
    """
    This function stops job instances that match the provided criteria.

    Args:
        run_match (InstanceMatchCriteria, mandatory):
            The operation will affect only instances matching these criteria.

    Returns:
        A container holding :class:`StopResponse` objects, each representing the result of the stop operation
        for a respective job instance.
        It also includes any errors that may have happened, each one related to a specific server API.

    Note:
        The stop operation might not succeed if the instance doesn't correctly handle stop/terminate signals.
    """

    with api_client() as c:
        return c.stop_instances(run_match)


def fetch_output(run_match=None) -> AggregatedResponse[OutputResponse]:
    """
    This function requests the last lines of the output from job instances that optionally match the provided criteria.

    Args:
        run_match (InstanceMatchCriteria, optional):
            The operation will affect only instances matching these criteria.
            If not provided, the tail of all instances is read.

    Returns:
        A container holding :class:`TailResponse` objects, each containing last lines for a respective job instance.
        It also includes any errors that may have happened, each one related to a specific server API.
    """

    with api_client() as c:
        return c.fetch_output(run_match)


def signal_dispatch(instance_match) -> AggregatedResponse[SignalDispatchResponse]:
    with api_client() as c:
        return c.signal_dispatch(instance_match)


def instance_transition_receiver(instance_match=None, phases=(), run_states=()):
    return InstanceTransitionReceiver(instance_match, phases, run_states)
