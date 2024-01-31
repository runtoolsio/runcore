"""
This is the core component of runtools defining the main constructs like run, phase, job, instance etc.
"""

__version__ = "0.1.1"

from types import MappingProxyType

from runtools.runcore import db
from runtools.runcore.common import RuntoolsException
from runtools.runcore.db import Persistence

_current_persistence = 'sqlite'
_persistence = {}


def configure(**kwargs):
    persistence_obj = kwargs.get('persistence', {"type": _current_persistence})
    if "type" not in persistence_obj:
        raise InvalidConfiguration("Field `type` is mandatory for `persistence` configuration object")
    configure_persistence(persistence_obj["type"], persistence_obj.get(_current_persistence, {}))


def configure_persistence(persistence_type, persistence_config=None):
    global _current_persistence
    _current_persistence = persistence_type
    _persistence[_current_persistence] = MappingProxyType(persistence_config or {})


def persistence(persistence_type=None) -> Persistence:
    db_type = persistence_type or _current_persistence
    if not db_type:
        raise PersistenceDisabledError

    return db.load_database_module(db_type).create_database(_persistence[db_type])


class InvalidConfiguration(RuntoolsException):

    def __init__(self, message: str):
        super().__init__(message)


class PersistenceDisabledError(Exception):
    """
    Raised when attempting an operation while persistence is disabled.
    Any code using persistence should always catch this exception.
    """

    def __init__(self):
        super().__init__("Cannot perform persistence operation because persistence is disabled.")
