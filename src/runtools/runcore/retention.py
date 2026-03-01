from dataclasses import dataclass


@dataclass(frozen=True)
class RetentionPolicy:
    """Retention limits for finished job runs. -1 means unlimited."""
    max_runs_per_job: int = 100
    max_runs_per_env: int = 1000
