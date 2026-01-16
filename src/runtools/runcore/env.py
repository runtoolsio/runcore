import itertools
from pathlib import Path
from typing import Optional, Literal, Annotated, Union, Dict, Any, Set, Iterable

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
    default: bool = Field(
        default=False,
        description="Whether this environment should be treated as the default"
    )
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
    id: str = Field(default_factory=lambda: "isolated_" + util.unique_timestamp_hex(),
                    description="Environment identifier")
    persistence: PersistenceConfig = Field(
        default_factory=PersistenceConfig.in_memory_sqlite,
        description="Persistence configuration; defaults to in-memory SQLite for testing"
    )


EnvironmentConfigUnion = Annotated[
    Union[LocalEnvironmentConfig, IsolatedEnvironmentConfig],
    Discriminator("type")
]

ConfigDict = Dict[str, Any]


def get_env_config(env_id: Optional[str] = None) -> EnvironmentConfigUnion:
    """
    Load, merge, and validate a single environment configuration by its identifier.

    This function reads all applicable TOML configuration files (project, user, system, and packaged default),
    deep-merges the layers, and validates the result against the Pydantic `EnvironmentConfigUnion` model.

    If no `env_id` is provided, the default environment configuration is loaded.

    Args:
        env_id (str, optional): The environment identifier to load. If None, the default environment is used.

    Returns:
        EnvironmentConfigUnion: The validated configuration model for the resolved environment.

    Raises:
        ConfigFileNotFoundError: If no configuration files are found.
        EnvironmentNotFoundError: If the specified `env_id` is not present.
        ValidationError: If the loaded configuration dict fails validation.
    """
    return env_config_from_dict(load_env_config(env_id) if env_id else load_default_env_config())


def get_default_env_config() -> EnvironmentConfigUnion:
    """
    Load and validate the default environment config.

    Returns:
        EnvironmentConfigUnion: A validated Pydantic model (LocalEnvironmentConfig or IsolatedEnvironmentConfig)
        corresponding to the default environment.

    Raises:
        ConfigFileNotFoundError: If no TOML config files can be found.
        EnvironmentNotFoundError: If no environment entries are found at all.
        ValidationError: If the loaded configuration dict fails Pydantic validation.
    """
    return env_config_from_dict(load_default_env_config())


def get_env_configs(*env_ids: str) -> Dict[str, EnvironmentConfigUnion]:
    """
    Load, deep-merge, and validate multiple environment configurations.

    Reads from project-level, user-level, system-level, and packaged default TOML files,
    applying system<user<project overrides, filtered by any provided env_ids.

    Args:
        *env_ids (str): One or more environment IDs to load. If omitted, all discovered
            environments (plus packaged default if none found) are returned.

    Returns:
        Dict[str, EnvironmentConfigUnion]: A mapping of environment IDs to their validated models.

    Raises:
        ConfigFileNotFoundError: If no configuration files are found.
        EnvironmentNotFoundError: If any of the specified env_ids are not present.
        ValidationError: If any loaded configuration dict fails validation.
    """
    return {eid: env_config_from_dict(conf_dict) for eid, conf_dict in load_env_configs(*env_ids).items()}


def env_config_from_dict(conf: ConfigDict) -> EnvironmentConfigUnion:
    """
    Instantiate the correct Pydantic model based on the 'type' discriminator.

    Args:
        conf (Dict[str, Any]): A raw configuration dictionary containing at least a 'type' key.

    Returns:
        EnvironmentConfigUnion: Either a LocalEnvironmentConfig or IsolatedEnvironmentConfig.

    Raises:
        ValidationError: If the provided dict does not conform to the expected schema.
    """
    validator = TypeAdapter(EnvironmentConfigUnion)
    return validator.validate_python(conf)


def load_env_config(env_id: str) -> ConfigDict:
    """
    Load raw, merged environment configuration for a single env_id (no validation).

    Args:
        env_id (str): The environment identifier to load.

    Returns:
        ConfigDict: The merged configuration dict for the given env_id.

    Raises:
        ConfigFileNotFoundError: If no config files are found matching '*env*.toml'.
        EnvironmentNotFoundError: If env_id is not present in any discovered file.
    """
    return load_env_configs(env_id)[env_id]


def load_default_env_config() -> ConfigDict:
    """
    Load the merged raw dict for whichever environment is marked default.

    Falls back to DEFAULT_LOCAL_ENVIRONMENT if no config.default==True is found.

    Returns:
        ConfigDict: The merged configuration dictionary for the default environment.

    Raises:
        ConfigFileNotFoundError: If no TOML config files can be found.
        EnvironmentNotFoundError:
            - If no environment entries are found at all.
        MultipleDefaultEnvironmentsError:
            - If more than one environment is marked default.
    """
    all_cfgs = load_env_configs()  # may raise ConfigFileNotFoundError / EnvironmentNotFoundError

    default_envs = [eid for eid, conf in all_cfgs.items() if conf.get("default", False)]
    if len(default_envs) > 1:
        raise MultipleDefaultEnvironmentsError(default_envs)
    if len(default_envs) == 1:
        return all_cfgs[default_envs[0]]

    # fallback: the packaged default-local TOML should always register DEFAULT_LOCAL_ENVIRONMENT
    if DEFAULT_LOCAL_ENVIRONMENT in all_cfgs:
        return all_cfgs[DEFAULT_LOCAL_ENVIRONMENT]

    # if this ever trips, it's a bug in our packaging or path logic
    assert False, (
        "Packaged default-local environment missing! "
        f"Expected to find '{DEFAULT_LOCAL_ENVIRONMENT}' in {list(all_cfgs)!r}"
    )


def load_env_configs(*env_ids: str) -> Dict[str, ConfigDict]:
    """
    Collect and deep-merge raw TOML configuration for one or more env IDs.

    Layers (increasing priority):
      1. Project-level env.toml files (find_config_files)
      2. Packaged default ENV_CONFIG_FILE

    Args:
        *env_ids (str): Environment IDs to load. If empty, loads all discovered environments.

    Returns:
        Dict[str, ConfigDict]: Mapping from each requested env_id to its raw merged dict.

    Raises:
        ConfigFileNotFoundError: If no config files are found.
        EnvironmentNotFoundError: If any specified env_ids are missing.
    """
    paths_provider = paths.find_config_files("*env*.toml")
    chain = itertools.chain(paths_provider, [paths.package_config_path(config.__package__, ENV_CONFIG_FILE)])
    return _load_env_config(chain, *env_ids)


def _deep_merge(a: ConfigDict, b: ConfigDict) -> ConfigDict:
    """
    Recursively merge dict `b` into dict `a`, returning a new dict.
    Keys in `b` override or extend those in `a`.
    """
    result = a.copy()
    for k, v in b.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def _load_env_config(path_provider, *env_ids: str) -> Dict[str, ConfigDict]:
    """
    Internal helper to deep-merge environment configs from a sequence of TOML files.

    Reads TOML files in reversed priority (lowest first, highest last), extracts all
    `environment` blocks, and deep-merges repeated IDs so higher-priority layers override.

    Args:
        path_provider (Iterable[Path]): Paths to TOML files, highest-priority first.
        *env_ids (str): Environment identifiers to filter; if empty, loads all environments.

    Returns:
        Dict[str, ConfigDict]: Mapping from env_id to its merged raw config dict.

    Raises:
        ConfigFileNotFoundError: If no files are provided.
        EnvironmentNotFoundError: If specified env_ids are not found.
    """
    config_paths = list(path_provider)
    if not config_paths:
        raise ConfigFileNotFoundError("No environment configuration files found")

    id_to_config: Dict[str, ConfigDict] = {}
    for env_cfg_path in reversed(config_paths):
        env_cfg = files.read_toml_file(env_cfg_path)
        if "environment" not in env_cfg:
            continue
        environments = env_cfg["environment"]
        if not isinstance(environments, list):
            environments = [environments]
        for env in environments:
            eid = env.get("id")
            if not eid or (env_ids and eid not in env_ids):
                continue
            if eid in id_to_config:
                id_to_config[eid] = _deep_merge(id_to_config[eid], env)
            else:
                id_to_config[eid] = env

    if env_ids:
        missing = set(env_ids) - set(id_to_config)
        if missing:
            raise EnvironmentNotFoundError(
                f"No configuration found for environment(s) `{', '.join(missing)}` "
                f"in file(s): {', '.join(str(p) for p in config_paths)}",
                missing
            )
    elif not id_to_config:
        raise EnvironmentNotFoundError(
            f"No environment configurations found in file(s): {', '.join(str(p) for p in config_paths)}"
        )

    return id_to_config


class EnvironmentNotFoundError(RuntoolsException):
    """Exception raised when an environment configuration cannot be found."""

    def __init__(self, message: str, env_ids: Optional[Iterable[str]] = None):
        super().__init__(message)
        self.env_ids: Set[str] = set(env_ids) if env_ids else set()


class MultipleDefaultEnvironmentsError(RuntoolsException):
    """
    Raised when more than one environment is marked as default.
    """

    def __init__(self, default_ids: Iterable[str]):
        message = f"Multiple environments marked as default: {list(default_ids)!r}"
        super().__init__(message)
        self.default_ids = set(default_ids)
