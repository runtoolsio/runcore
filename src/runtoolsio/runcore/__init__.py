"""
This is the core component of runtools defining the main constructs like run, phase, job, instance etc.
"""

__version__ = "0.1.1"

from types import MappingProxyType

from runtoolsio.runcore import db

_PERSISTENCE_TYPE = 'sqlite'
_PERSISTENCE_CONFIG = MappingProxyType({})


def configure(**kwargs):
    global _PERSISTENCE_TYPE
    _PERSISTENCE_TYPE = kwargs.get('persistence_type', _PERSISTENCE_TYPE)

    global _PERSISTENCE_CONFIG
    _PERSISTENCE_CONFIG = kwargs.get('persistence_config', _PERSISTENCE_CONFIG)


def persistence():
    if not _PERSISTENCE_TYPE:
        raise PersistenceDisabledError

    return db.load_database_module(_PERSISTENCE_TYPE).create_persistence(_PERSISTENCE_CONFIG)


class PersistenceDisabledError(Exception):
    """
    Raised when attempting an operation while persistence is disabled.
    Any code using persistence should always catch this exception.
    """

    def __init__(self):
        super().__init__("Cannot perform persistence operation because persistence is disabled.")
