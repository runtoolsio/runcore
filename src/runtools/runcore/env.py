from pathlib import Path
from typing import Optional, Literal, Annotated, Union, Dict, Any

from pydantic import BaseModel, Field, Discriminator

from runtools.runcore import config, paths
from runtools.runcore.db import PersistenceConfig
from runtools.runcore.err import RuntoolsException
from runtools.runcore.util import files

DEFAULT_ENVIRONMENT = 'main'
CONFIG_FILE = 'env.toml'


class EnvironmentTypes:
    LOCAL = 'local'


class EnvironmentConfig(BaseModel):
    type: str
    id: str = Field(default=DEFAULT_ENVIRONMENT, description="Environment identifier")
    persistence: Optional[PersistenceConfig] = Field(
        default_factory=PersistenceConfig.default_sqlite,
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


EnvironmentConfigUnion = Annotated[
    Union[LocalEnvironmentConfig],
    Discriminator("type")
]


def env_config_from_dict(conf: Dict[str, Any]) -> EnvironmentConfigUnion:
    """
    Create the appropriate environment config type based on the 'type' field in the config dict.

    Args:
        conf: Dictionary containing environment configuration (e.g., from a TOML file)

    Returns:
        The appropriate environment configuration object

    Raises:
        ValidationError: If the provided config cannot be validated
    """
    # The type discriminator will automatically choose the right model
    return EnvironmentConfigUnion.model_validate(conf)


def load_packed_def_env_config():
    return _load_env_config(DEFAULT_ENVIRONMENT, [paths.package_config_path(config.__package__, CONFIG_FILE)])


def load_env_config(env_id: str = DEFAULT_ENVIRONMENT) -> Dict[str, Any]:
    """
    Load environment configuration for the specified environment ID.

    Args:
        env_id: The environment identifier to look for

    Returns:
        Dictionary containing the environment configuration

    Raises:
        ConfigFileNotFoundError: If no environment configuration files are found
        EnvironmentNotFoundError: If no configuration is found for the given env_id
    """
    return _load_env_config(env_id, paths.find_config_files("*env*.toml", raise_if_empty=True))


def _load_env_config(env_id, path_provider) -> Dict[str, Any]:
    for env_cfg_path in path_provider:
        env_cfg = files.read_toml_file(env_cfg_path)

        if "environment" not in env_cfg:
            continue

        # Handle both single environment and list of environments
        environments = env_cfg["environment"]
        if not isinstance(environments, list):
            environments = [environments]

        for env in environments:
            if env.get("id") == env_id:
                return env

    raise EnvironmentNotFoundError(f"No configuration found for environment '{env_id}'")


class EnvironmentNotFoundError(RuntoolsException):
    """Exception raised when an environment configuration cannot be found."""

    def __init__(self, message: str):
        super().__init__(message)
