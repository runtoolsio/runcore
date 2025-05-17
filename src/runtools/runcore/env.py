from pathlib import Path
from typing import Optional, Literal, Annotated, Union, Dict, Any, Tuple

from pydantic import BaseModel, Field, Discriminator, TypeAdapter

from runtools.runcore import config, paths, util
from runtools.runcore.db import PersistenceConfig
from runtools.runcore.err import RuntoolsException
from runtools.runcore.paths import ConfigFileNotFoundError
from runtools.runcore.util import files

DEFAULT_LOCAL_ENVIRONMENT = 'local'
ENV_CONFIG_FILE = 'env.toml'


class EnvironmentTypes:
    ISOLATED = 'isolated'
    LOCAL = 'local'


class EnvironmentConfig(BaseModel):
    type: str
    id: str = Field(description="Environment identifier")
    persistence: Optional[PersistenceConfig] = Field(
        default=None,
        description="Persistence configuration; if None, persistence is disabled"
    )


class LayoutConfig(BaseModel):
    root_dir: Optional[Path] = Field(
        default=None,
        description="Root directory for local environments; uses default if None"
    )


class LocalEnvironmentConfig(EnvironmentConfig):
    type: Literal["local"] = EnvironmentTypes.LOCAL
    id: str = Field(default=DEFAULT_LOCAL_ENVIRONMENT, description="Environment identifier")
    layout: LayoutConfig = Field(
        default_factory=LayoutConfig,
        description="Layout configuration for local environment resources"
    )
    persistence: Optional[PersistenceConfig] = Field(
        default_factory=PersistenceConfig.default_sqlite,
        description="Persistence configuration; if None, persistence is disabled"
    )


class IsolatedEnvironmentConfig(EnvironmentConfig):
    """Configuration for isolated environments used primarily in testing."""
    type: Literal["isolated"] = EnvironmentTypes.ISOLATED
    id: str = Field(default_factory=lambda: "isolated_" + util.unique_timestamp_hex(), description="Environment identifier")
    persistence: PersistenceConfig = Field(
        default_factory=PersistenceConfig.in_memory_sqlite,
        description="Persistence configuration; defaults to in-memory SQLite for testing"
    )


EnvironmentConfigUnion = Annotated[
    Union[LocalEnvironmentConfig, IsolatedEnvironmentConfig],
    Discriminator("type")
]


def get_env_config(env_id: str) -> EnvironmentConfigUnion:
    """
    Load and validate a single environment configuration by its identifier.

    Args:
        env_id: The environment identifier to load.

    Returns:
        A validated EnvironmentConfig model instance.

    Raises:
        ConfigFileNotFoundError: If no configuration files are found.
        EnvironmentNotFoundError: If the specified env_id is not present.
        ValidationError: If the loaded configuration dict fails validation.
    """
    env_config_dict, _ = load_env_config(env_id)
    return env_config_from_dict(env_config_dict)


def get_env_configs(*env_ids: str) -> Dict[str, EnvironmentConfigUnion]:
    """
    Load and validate multiple environment configurations by their identifiers.

    Args:
        env_ids: The environment identifiers to load.

    Returns:
        A dict mapping each env_id to its validated EnvironmentConfig model instance.

    Raises:
        ConfigFileNotFoundError: If no configuration files are found.
        EnvironmentNotFoundError: If any of the specified env_ids are not present.
        ValidationError: If any loaded configuration dict fails validation.
    """
    id_to_raw = load_env_configs(*env_ids)
    return {
        eid: env_config_from_dict(raw_conf)
        for eid, (raw_conf, _path) in id_to_raw.items()
    }


def env_config_from_dict(conf: Dict[str, Any]) -> EnvironmentConfigUnion:
    """
    Create a Pydantic EnvironmentConfig model based on the 'type' discriminator in the dict.

    Args:
        conf: A dict containing environment configuration data.

    Returns:
        A validated EnvironmentConfig model (LocalEnvironmentConfig or IsolatedEnvironmentConfig).

    Raises:
        ValidationError: If the provided dict cannot be validated against models.
    """
    validator = TypeAdapter(EnvironmentConfigUnion)
    return validator.validate_python(conf)


def load_env_default_config(env_id: str) -> Tuple[Dict[str, Any], Path]:
    """
    Load the default (packaged) environment config for the given env_id.

    Args:
        env_id: The environment identifier to load.

    Returns:
        Tuple of (raw config dict, source file path).

    Raises:
        ConfigFileNotFoundError: If the packaged default config file is missing.
        EnvironmentNotFoundError: If the env_id is not found in the default file.
    """
    def_cfg = [paths.package_config_path(config.__package__, ENV_CONFIG_FILE)]
    return _load_env_config(def_cfg, env_id)[env_id]


def load_env_config(env_id: str) -> Tuple[Dict[str, Any], Path]:
    """
    Load a single environment config by its identifier from available config files.

    Args:
        env_id: The environment identifier to load.

    Returns:
        Tuple of (raw config dict, source file path).

    Raises:
        ConfigFileNotFoundError: If no config files are found among '*env*.toml'.
        EnvironmentNotFoundError: If env_id is not present in any file.
    """
    return load_env_configs(env_id)[env_id]


def load_env_configs(*env_id: str) -> Dict[str, Tuple[Dict[str, Any], Path]]:
    """
    Load one or more environment configs by their identifiers from files matching '*env*.toml'.

    Args:
        env_id: One or more environment identifiers to load.

    Returns:
        A dict mapping each requested env_id to a tuple of (raw config dict, source file path).

    Raises:
        ConfigFileNotFoundError: If no config files are found matching the pattern.
        EnvironmentNotFoundError: If any env_id is not present in the discovered files.
    """
    paths_provider = paths.find_config_files("*env*.toml", raise_if_empty=True)
    return _load_env_config(paths_provider, *env_id)


def _load_env_config(path_provider, *env_ids: str) -> Dict[str, Tuple[Dict[str, Any], Path]]:
    """
    Internal helper to load configs from given file paths.

    Args:
        path_provider: Iterable of Path objects pointing to toml files.
        env_ids: Environment identifiers to filter; if empty, loads all environments.

    Returns:
        A dict mapping env_id to (raw config dict, source file path).

    Raises:
        ConfigFileNotFoundError: If path_provider yields no files.
        EnvironmentNotFoundError: If no matching env_ids are found when specified.
    """
    config_paths = list(path_provider)
    if not config_paths:
        raise ConfigFileNotFoundError("No environment configuration files found")

    id_to_config: Dict[str, Tuple[Dict[str, Any], Path]] = {}
    for env_cfg_path in config_paths:
        env_cfg = files.read_toml_file(env_cfg_path)
        if "environment" not in env_cfg:
            continue
        environments = env_cfg["environment"]
        if not isinstance(environments, list):
            environments = [environments]
        for env in environments:
            eid = env.get("id")
            if not eid:
                continue
            if not env_ids or eid in env_ids:
                id_to_config[eid] = (env, env_cfg_path)

    if env_ids:
        missing = set(env_ids) - set(id_to_config)
        if missing:
            raise EnvironmentNotFoundError(
                f"No configuration found for environment(s) `{', '.join(missing)}` "
                f"in file(s): {', '.join(str(p) for p in config_paths)}"
            )
    elif not id_to_config:
        raise EnvironmentNotFoundError(
            f"No environment configurations found in file(s): {', '.join(str(p) for p in config_paths)}"
        )

    return id_to_config


class EnvironmentNotFoundError(RuntoolsException):
    """Exception raised when an environment configuration cannot be found."""
    def __init__(self, message: str):
        super().__init__(message)
