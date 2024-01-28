"""
This module consists of:

- The persistence contract:
    The contract defines the mandatory methods for a class implementing the persistence functionality:
        > read_instances(instance_match, sort, *, asc, limit, offset, last)
        > read_stats(instance_match)
        > count_instances(instance_match)
        > store_instances(*job_inst)
        > remove_instances(instance_match)
        > clean_up(max_records, max_age)
    An instance of this class is returned when the `create_persistence()` function of the implementing module is called.

- Persistence implementation lookup:
    This module identifies and loads the desired persistence contract implementation when:
        a) A `load_*` method is invoked.
        b) The global persistence is accessed.
    Modules providing a `create_persistence` function, which returns a class adhering to the contract,
    are discovered using the conventional package name pattern: `runtoolsio.runcore.db.{persistence_type}`.

- Global persistence:
    This module itself implements the persistence contract, meaning it provides the contract methods
    as module functions. The module loads and caches the implementation defined in the `cfg.persistence_type`
    configuration field when any of the contract functions are called for the first time.
    Subsequent uses of the methods delegates to the cached implementation until the `reset` function is invoked.
    After using the global persistence, it should be closed  by calling the `close` function.

"""

import sys
from enum import Enum
from typing import List

from runtoolsio.runcore import paths, db
from runtoolsio.runcore import util
from runtoolsio.runcore.job import JobStats, JobRuns
from runtoolsio.runcore.run import RunState

CURRENT_DB = 'sqlite'

_db_configs = {}
_db_modules = {}


def add_database_config(db_type, db_conf):
    """
    Loads the persistence specified by the parameter and creates a new instance of it.

    Args:
        db_type (str): Type of the persistence to be loaded
    """
    _db_configs[db_type] = db_conf

    global CURRENT_DB
    if not CURRENT_DB:
        CURRENT_DB = db_type


def _instance(db_type=None):
    db_module = _db_modules.get(db_type)
    if not db_module:
        db_module[db_type] = db_module = db.load_database_module(db_type)

    return db_module.create_database(_db_configs.get(db_type))


class SortCriteria(Enum):
    """
    Enum representing the criteria by which job instance rows can be sorted.

    Attributes:
    - CREATED: Sort by the timestamp when the job instance was created.
    - ENDED: Sort by the timestamp when the job instance ended or was completed.
    - TIME: Sort by the execution time of the job instance.
    """
    CREATED = 1
    ENDED = 2
    TIME = 3


def read_runs(instance_match=None, sort=SortCriteria.ENDED, *, asc=True, limit=-1, offset=-1, last=False) \
        -> JobRuns:
    """
    Fetches ended job instances based on specified criteria.
    Datasource: The database as defined by the configured persistence type.

    Args:
        instance_match (InstanceMatchCriteria, optional):
            Criteria to match specific job instances. None means fetch all. Defaults to None.
        sort (SortCriteria):
            Determines the field by which records are sorted. Defaults to `SortCriteria.ENDED`.
        asc (bool, optional):
            Determines if the sorting is in ascending order. Defaults to True.
        limit (int, optional):
            Maximum number of records to return. -1 means no limit. Defaults to -1.
        offset (int, optional):
            Number of records to skip before starting to return. -1 means no offset. Defaults to -1.
        last (bool, optional):
            If set to True, only the last record for each job is returned. Defaults to False.

    Returns:
        JobRuns: A collection of job instances that match the given criteria.
    """
    return _instance().read_job_runs(instance_match, sort, asc=asc, limit=limit, offset=offset, last=last)


def read_stats(instance_match=None) -> List[JobStats]:
    """
    Returns job statistics for each job based on specified criteria.
    Datasource: The database as defined by the configured persistence type.

    Args:
        instance_match (InstanceMatchCriteria, optional):
            Criteria to match records used to calculate the statistics. None means fetch all. Defaults to None.
    """
    return _instance().read_stats(instance_match)


def count_instances(instance_match):
    """
    Counts the total number of job instances based on the specified match criteria.
    Datasource: The database as defined by the configured persistence type.

    Args:
        instance_match (InstanceMatchCriteria): Criteria to filter job instances.

    Returns:
        int: Total count of job instances matching the specified criteria.
    """
    return sum(s.count for s in (_instance().read_stats(instance_match)))


def store_instances(*job_inst):
    """
    Stores the provided job instances to the configured persistence source.
    After storing, it also initiates a cleanup based on configured criteria.

    Args:
        *job_inst (JobInst): Variable number of job instances to be stored.
    """
    _instance().store_instances(*job_inst)
    clean_up_by_config()


def remove_runs(run_match):
    """
    Removes job instances based on the specified match criteria from the configured persistence source.

    Args:
        run_match (InstanceMatchCriteria): Criteria to filter job instances for removal.
    """
    _instance().remove_runs(run_match)


def clean_up_by_config():
    """
    Cleans up the job instances in the configured persistence source based on max records and max age
    as defined in the configuration. See `clean_up` function for more details.
    """
    persistence_max_age = None  # TODO
    persistence_max_records = None  # TODO
    try:
        max_age = util.parse_iso8601_duration(persistence_max_age) if persistence_max_age else None
    except ValueError:
        sys.stderr.write("Invalid max_age in " + str(paths.lookup_config_file()) + "\n")
        return
    _instance().clean_up(persistence_max_records, max_age)


def clean_up(max_records=-1, max_age=None):
    """
    Cleans up old records in the configured persistence source based on given parameters.
    The cleanup can be based on a maximum number of records to retain or/and the age of the records.

    Args:
        max_records (int, optional):
            The maximum number of records to retain. Records are deleted from the oldest one defined by `ended` field.
            A value of -1 indicates no limit. Defaults to -1.
        max_age (relativedelta, optional):
            The maximum age of the records to retain. Older records will be removed.
            If None, removal by age is not performed. Defaults to None.
    """
    _instance().clean_up(max_records, max_age)


def _sort_key(sort: SortCriteria):
    """TODO To remove?"""

    def key(j):
        if sort == SortCriteria.CREATED:
            return j.lifecycle.state_first_transition_at(RunState.CREATED)
        if sort == SortCriteria.ENDED:
            return j.lifecycle.ended_at
        if sort == SortCriteria.TIME:
            return j.lifecycle.total_executing_time
        raise ValueError(sort)

    return key


class _NoPersistence:

    def read_instances(self, instance_match=None, sort=SortCriteria.CREATED, *, asc, limit, offset, last=False):
        raise PersistenceDisabledError()

    def read_stats(self, instance_match=None):
        raise PersistenceDisabledError()

    def store_instances(self, *jobs_inst):
        raise PersistenceDisabledError()

    def remove_runs(self, instance_match):
        raise PersistenceDisabledError()

    def clean_up(self, max_records, max_age):
        raise PersistenceDisabledError()

    def close(self):
        pass


class PersistenceDisabledError(Exception):
    """
    Raised when attempting an operation while persistence is disabled.
    Any code using persistence should always catch this exception.
    """

    def __init__(self):
        super().__init__("Cannot perform persistence operation because persistence is disabled.")
