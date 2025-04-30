from pathlib import Path
from typing import Optional, Literal, Annotated, Union, Dict, Any

from pydantic import BaseModel, Field, Discriminator, TypeAdapter

from runtools.runcore import config, paths
from runtools.runcore.db import PersistenceConfig
from runtools.runcore.err import RuntoolsException
from runtools.runcore.util import files

DEFAULT_ENVIRONMENT = 'main'
CONFIG_FILE = 'env.toml'


class EnvironmentTypes:
    ISOLATED = 'isolated'
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
    persistence: Optional[PersistenceConfig] = Field(
        default_factory=PersistenceConfig.default_sqlite,
        description="Persistence configuration, if None persistence is disabled"
    )


class IsolatedEnvironmentConfig(EnvironmentConfig):
    """Configuration for isolated environments used primarily in testing."""
    type: Literal["isolated"] = EnvironmentTypes.ISOLATED
    persistence: PersistenceConfig = Field(
        default_factory=PersistenceConfig.in_memory_sqlite,
        description="Persistence configuration, defaults to in-memory SQLite for testing"
    )


EnvironmentConfigUnion = Annotated[
    Union[LocalEnvironmentConfig, IsolatedEnvironmentConfig],
    Discriminator("type")
]


def get_env_config(env_id: str = DEFAULT_ENVIRONMENT, fallback_default: bool = False) -> EnvironmentConfigUnion:
    """
    Loads the environment configuration dictionary for the given env_id,
    handles potential fallback to the packed default, validates the dictionary,
    and returns the corresponding Pydantic configuration model.

    Args:
        env_id: The environment identifier to look for.
        fallback_default: If True and env_id is DEFAULT_ENVIRONMENT but not found,
                          fall back to packaged default configuration.

    Returns:
        The validated environment configuration object (LocalEnvironmentConfig, IsolatedEnvironmentConfig, etc.).

    Raises:
        ConfigFileNotFoundError: If no config files found and fallback doesn't apply.
        EnvironmentNotFoundError: If env_id not found and fallback doesn't apply.
        ValidationError: If the loaded configuration dictionary is invalid.
    """
    env_config_dict = load_env_config(env_id=env_id, fallback_default=fallback_default)
    return env_config_from_dict(env_config_dict)


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
    # Use TypeAdapter with the Annotated Union to handle the discriminator logic
    validator = TypeAdapter(EnvironmentConfigUnion)
    return validator.validate_python(conf)


def load_default_env_config() -> Dict[str, Any]:
    return _load_env_config(DEFAULT_ENVIRONMENT, [paths.package_config_path(config.__package__, CONFIG_FILE)])


def load_env_config(env_id: str = DEFAULT_ENVIRONMENT, fallback_default: bool = False) -> Dict[str, Any]:
    """
    Load environment configuration for the specified environment ID.

    Args:
        env_id: The environment identifier to look for
        fallback_default: If True and env_id is DEFAULT_ENVIRONMENT but not found, fall back to
                         packaged default configuration instead of raising an exception

    Returns:
        Dictionary containing the environment configuration

    Raises:
        ConfigFileNotFoundError: If no environment configuration files are found and fallback_default is False or env_id is not DEFAULT_ENVIRONMENT
        EnvironmentNotFoundError: If no configuration is found for the given env_id and fallback_default is False or env_id is not DEFAULT_ENVIRONMENT
    """
    try:
        return _load_env_config(env_id, paths.find_config_files("*env*.toml", raise_if_empty=True))
    except RuntoolsException as e:
        if fallback_default and env_id == DEFAULT_ENVIRONMENT:
            return load_default_env_config()
        raise e


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
