"""
This is the core component of runtools defining the main constructs like run, phase, job, instance etc.
"""

__version__ = "0.1.1"

from types import MappingProxyType
from typing import List

from runtools.runcore import db
from runtools.runcore.client import RemoteCallClient, RemoteCallResult
from runtools.runcore.common import RuntoolsException
from runtools.runcore.db import Persistence, SortCriteria
from runtools.runcore.job import JobRun

_current_persistence = 'sqlite'
_persistence = {}


class InvalidConfiguration(RuntoolsException):

    def __init__(self, message: str):
        super().__init__(message)


def configure(**kwargs):
    persistence_obj = kwargs.get('persistence', {"type": _current_persistence})
    if "type" not in persistence_obj:
        raise InvalidConfiguration("Field `type` is mandatory in `persistence` configuration object")
    configure_persistence(persistence_obj["type"], persistence_obj.get(_current_persistence, {}))


def configure_persistence(persistence_type, persistence_config=None):
    global _current_persistence
    _current_persistence = persistence_type
    _persistence[_current_persistence] = MappingProxyType(persistence_config or {})


class PersistenceDisabledError(Exception):
    """
    Raised when attempting an operation while persistence is disabled.
    Any code using persistence should always catch this exception.
    """

    def __init__(self):
        super().__init__("Cannot perform persistence operation because persistence is disabled.")


def persistence(persistence_type=None) -> Persistence:
    db_type = persistence_type or _current_persistence
    if not db_type:
        raise PersistenceDisabledError

    return db.load_database_module(db_type).create(database=_persistence['database'], **_persistence[db_type])


def read_history_runs(run_match, sort=SortCriteria.ENDED, *, asc=True, limit=-1, offset=0, last=False):
    with persistence() as p:
        return p.read_history_runs(run_match, sort, asc=asc, limit=limit, offset=offset, last=last)


def read_history_stats(run_match=None):
    with persistence() as p:
        return p.read_history_stats(run_match)


def api_client():
    return RemoteCallClient()


def get_active_runs(run_match=None) -> List[RemoteCallResult[List[JobRun]]]:
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
        return c.collect_active_runs(run_match)



def signal_dispatch(instance_match, queue_id):
    with api_client() as c:
        return c.signal_dispatch(instance_match, queue_id)
