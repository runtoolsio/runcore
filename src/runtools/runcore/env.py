from pathlib import Path
from typing import Optional, Literal

from pydantic import BaseModel, Field

from runtools.runcore.db import PersistenceConfig

DEFAULT_ENVIRONMENT = 'main'

class EnvironmentTypes:
    LOCAL = 'local'


class EnvironmentConfig(BaseModel):
    type: str
    id: str = Field(default=DEFAULT_ENVIRONMENT, description="Environment identifier")
    persistence: Optional[PersistenceConfig] = Field(
        default=None,
        description="Persistence configuration, if None persistence is disabled"
    )


class LayoutConfig(BaseModel):
    root_dir: Optional[Path] = Field(
        default=None,
        description="Root directory for local environments, uses default if None"
    )


class LocalEnvironmentConfig(EnvironmentConfig):
    type: Literal["local"] = EnvironmentTypes.LOCAL
    layout: LayoutConfig = Field(
        default_factory=LayoutConfig,
        description="Layout configuration for local environment resources"
    )

    @classmethod
    def create_default(cls, env_id: str = DEFAULT_ENVIRONMENT):
        return cls(id=env_id, persistence=PersistenceConfig.default_sqlite())
