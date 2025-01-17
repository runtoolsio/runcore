"""
This module serves as a source of job definitions and offers:
 1. An API to read job definitions, with each represented as an instance of the `Job` class, from job repositories.
 2. A job repository interface.
 3. Default job repositories: active, history, and file.

To add a custom job repository, implement the `JobRepository` interface and pass its instance to the `add_repo` function
"""

import os
from abc import ABC, abstractmethod
from typing import List, Optional

import runtools.runcore
from runtools import runcore
from runtools.runcore import PersistenceDisabledError
from runtools.runcore import paths
from runtools.runcore import util
from runtools.runcore.job import Job


class JobRepository(ABC):

    @property
    @abstractmethod
    def id(self):
        pass

    @abstractmethod
    def read_jobs(self):
        pass

    def read_job(self, job_id):
        for job in self.read_jobs():
            if job.id == job_id:
                return job

        return None


class JobRepositoryFile(JobRepository):
    DEF_FILE_CONTENT = \
        {
            'example_job_id': {
                'job_type': 'BATCH',
                'properties': { 'prop_1': 'val_1', }
            }
        }

    def __init__(self, path=None):
        self.path = path

    @property
    def id(self):
        return 'file'

    def read_jobs(self) -> List[Job]:
        root = util.read_toml_file(self.path or paths.lookup_jobs_file())
        if not root:
            return []

        return [
            Job(job_id, job_type=job_config['job_type'], properties=job_config['properties'])
            for job_id, job_config in root.items()
        ]

    def reset(self, overwrite: bool):
        # TODO Create `runcore config create --jobs` command for this
        path = self.path or (paths.runtools_config_file_search_path(exclude_cwd=True)[0] / paths.JOBS_FILE)
        if not os.path.exists(path) or overwrite:
            pass
            # TODO Copy file from resources
            # util.write_toml_file(JobRepositoryFile.DEF_FILE_CONTENT, path)


class JobRepositoryActiveInstances(JobRepository):

    @property
    def id(self):
        return 'active'

    def read_jobs(self) -> List[Job]:
        return [*{Job(i.job_id) for i in runtools.runcore.get_active_runs().successful}]


class JobRepositoryHistory(JobRepository):

    @property
    def id(self):
        return 'history'

    def read_jobs(self) -> List[Job]:
        try:
            with runcore.persistence() as db:
                return [Job(s.job_id) for s in db.read_history_stats()]
        except PersistenceDisabledError:
            return []


def _init_repos():
    repos = [JobRepositoryActiveInstances(), JobRepositoryHistory(), JobRepositoryFile()]  # Keep the correct order
    return {repo.id: repo for repo in repos}


_job_repos = _init_repos()


def add_repo(repo):
    _job_repos[repo.id] = repo


def read_job(job_id) -> Optional[Job]:
    """
    Args:
        job_id (str): ID of the job to be searched.

    Returns:
        Optional[Job]: Searched job from registered repositories or None if not found.
    """
    for repo in reversed(_job_repos.values()):
        job = repo.read_job(job_id)
        if job:
            return job

    return None


def read_jobs() -> List[Job]:
    """
    Returns:
        List[Job]: Jobs from all registered repositories
    """
    jobs = {}
    for repo in _job_repos.values():
        for job in repo.read_jobs():
            jobs[job.id] = job

    return list(jobs.values())
