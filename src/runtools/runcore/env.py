import logging
from enum import StrEnum
from pathlib import Path
from typing import Optional, Literal, Annotated, Union, Dict, Set, Iterable, List

from pydantic import BaseModel, ConfigDict, Field, Discriminator

from runtools.runcore import paths
from runtools.runcore.db import load_database_module
from runtools.runcore.err import RuntoolsException
from runtools.runcore.output import OutputConfig
from runtools.runcore.util import files

log = logging.getLogger(__name__)

BUILTIN_LOCAL = 'local'
_SQLITE_DRIVER = 'sqlite'


class TransportType(StrEnum):
    IN_PROCESS = 'in_process'
    UNIX_SOCKET = 'unix_socket'
    DB_POLLING = 'db_polling'


class InProcessTransportConfig(BaseModel):
    """No transport boundary — node and clients live in the same process."""
    model_config = ConfigDict(frozen=True)

    type: Literal["in_process"] = TransportType.IN_PROCESS


class UnixSocketTransportConfig(BaseModel):
    """Process boundary over Unix domain sockets."""
    model_config = ConfigDict(frozen=True)

    type: Literal["unix_socket"] = TransportType.UNIX_SOCKET
    root_dir: Optional[Path] = Field(
        default=None,
        description="Transport root: holds component dirs, socket files, and liveness lock files. "
                    "Uses default if None."
    )


class DbPollingTransportConfig(BaseModel):
    """Shared-database transport (polling): clients observe runs by polling the environment database
    rather than contacting producing nodes. Backend-agnostic — the DB driver is chosen by the
    EnvironmentEntry, independently of this transport."""
    model_config = ConfigDict(frozen=True)

    type: Literal["db_polling"] = TransportType.DB_POLLING


TransportConfig = Annotated[
    Union[InProcessTransportConfig, UnixSocketTransportConfig, DbPollingTransportConfig],
    Discriminator("type")
]


class RetentionConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    keep_days: Optional[int] = Field(
        default=None,
        description="Default retention for `env prune`: keep finished runs newer than N days "
                    "(0 = remove all). None means no default — prune requires an explicit --keep-days.")


class EnvironmentConfig(BaseModel):
    """Runtime configuration of a named environment.

    Transport selects process topology, communication mechanism, and coordination primitives.
    Persistence is not part of this config — the DB must be opened (via EnvironmentEntry)
    before this config can be loaded from it.
    """
    model_config = ConfigDict(frozen=True)

    id: str = Field(description="Environment identifier")
    transport: TransportConfig = Field(description="Transport selecting topology and coordination")
    output: OutputConfig = Field(default_factory=OutputConfig, description="Output configuration")
    retention: RetentionConfig = Field(default_factory=RetentionConfig,
                                       description="Retention configuration for finished runs")
    plugins: Dict[str, dict] = Field(default_factory=dict, description="Plugin name to config mapping")

    @classmethod
    def default_local(cls, env_id: str) -> "EnvironmentConfig":
        """Out-of-the-box preset: unix_socket transport, default output and retention."""
        return cls(id=env_id, transport=UnixSocketTransportConfig())


# --- Environment entry ---

class EnvironmentEntry(BaseModel):
    """Describes how to reach an environment's database.

    Can be constructed from the registry, from the built-in local convention,
    or programmatically by the user.
    """
    id: str
    driver: str
    location: Optional[str] = Field(default=None, description="Driver-specific location of the backing store")

    @property
    def is_builtin_local(self) -> bool:
        return self.id == BUILTIN_LOCAL and self.location is None


def resolve_env_ref(ref: 'EnvironmentEntry | str | None') -> EnvironmentEntry:
    """Resolve an environment reference to an EnvironmentEntry.

    EnvironmentEntry passes through, str is looked up, None defaults to built-in local.
    """
    if isinstance(ref, EnvironmentEntry):
        return ref
    return lookup(ref or BUILTIN_LOCAL)


def lookup(env_id: str) -> EnvironmentEntry:
    """Look up an environment entry by ID.

    Built-in 'local' is always available (deterministic path, no registry needed).
    Other environments are looked up from the registry.

    Raises:
        EnvironmentNotFoundError: If env_id is not built-in local and not in the registry.
    """
    if env_id == BUILTIN_LOCAL:
        return EnvironmentEntry(id=BUILTIN_LOCAL, driver=_SQLITE_DRIVER)
    registry = _load_registry()
    if env_id not in registry:
        raise EnvironmentNotFoundError(f"Environment '{env_id}' not found in registry", {env_id})
    entry_data = registry[env_id]
    return EnvironmentEntry(id=env_id, **entry_data)


# --- Registry ---

def _load_registry() -> Dict[str, dict]:
    """Read the environment registry file. Returns empty dict if file doesn't exist.

    Raises:
        InvalidEnvironmentRegistryError: If the registry contains a reserved environment ID.
    """
    reg_path = paths.registry_path()
    if not reg_path.exists():
        return {}
    data = files.read_toml_file(reg_path)
    environments = data.get("environments", {})
    if BUILTIN_LOCAL in environments:
        raise InvalidEnvironmentRegistryError(
            f"'{BUILTIN_LOCAL}' is a reserved built-in environment and cannot be defined in the registry")
    return environments


def _save_registry(registry: Dict[str, dict]):
    """Write the registry back to environments.toml."""
    reg_path = paths.registry_path()
    reg_path.parent.mkdir(parents=True, exist_ok=True)
    files.write_toml_file(reg_path, {"environments": registry})


def load_registry() -> Dict[str, EnvironmentEntry]:
    """Read all registered environments. Does not include built-in local."""
    raw = _load_registry()
    return {eid: EnvironmentEntry(id=eid, **entry) for eid, entry in raw.items()}


def available_environments() -> list[EnvironmentEntry]:
    """Return all available environments: built-in local + registered."""
    entries = [EnvironmentEntry(id=BUILTIN_LOCAL, driver=_SQLITE_DRIVER)]
    entries.extend(EnvironmentEntry(id=eid, **data) for eid, data in _load_registry().items())
    return entries


def resolve_env_id(env_id: Optional[str] = None) -> str:
    """Non-interactive environment resolver (used by taro).

    Available environments = built-in local + registered environments.

    - If env_id given: return it.
    - If exactly 1 available: return it.
    - If 0 available: raise EnvironmentNotFoundError.
    - If multiple available: raise AmbiguousEnvironmentError.
    """
    if env_id:
        return env_id
    available = available_environments()
    if len(available) == 1:
        return available[0].id
    if not available:  # Currently unreachable (local always included), but needed once env disabling is implemented
        raise EnvironmentNotFoundError("No environments available. Use -e local or create one with `taro env create`.")
    raise AmbiguousEnvironmentError([e.id for e in available])


# --- DB layer ---

def _create_env_db(entry: EnvironmentEntry):
    """Create an EnvironmentDatabase instance from an entry (unopened).

    Raises:
        EnvironmentNotFoundError: If the backing store does not exist.
    """
    driver = load_database_module(entry.driver)
    if not driver.exists(entry):
        raise EnvironmentNotFoundError(
            f"Database for environment '{entry.id}' not found", {entry.id})
    return driver.create(entry)


def ensure_environment(entry: EnvironmentEntry):
    """Ensure the built-in local environment's backing store exists, creating it with defaults if needed.

    Only applicable to the built-in local environment. Named environments must be created explicitly.
    """
    if not entry.is_builtin_local:
        return
    driver = load_database_module(entry.driver)
    if not driver.exists(entry):
        driver.create_environment(entry, EnvironmentConfig.default_local(entry.id))


def _open_environment(entry: EnvironmentEntry):
    """Open an environment's database and load its config. Returns (env_db, config) tuple.

    The DB is returned already opened. The caller owns the DB lifecycle (must close it).
    On failure during config loading, the DB is closed before raising.
    """
    env_db = _create_env_db(entry)
    env_db.open()
    try:
        config_dict = env_db.load_config(entry.id)
        config = EnvironmentConfig.model_validate(config_dict)
    except BaseException:
        env_db.close()
        raise
    return env_db, config


def load_env_config(entry: EnvironmentEntry) -> EnvironmentConfig:
    """Load environment config from DB. Returns defaults for missing keys."""
    with _create_env_db(entry) as db:
        config_dict = db.load_config(entry.id)
        return EnvironmentConfig.model_validate(config_dict)


def save_env_config(entry: EnvironmentEntry, config: EnvironmentConfig):
    """Save environment config to the DB."""
    with _create_env_db(entry) as db:
        db.save_config(entry.id, config.model_dump(mode='json', exclude={'id'}))


# --- Lifecycle ---

def create_environment(entry: EnvironmentEntry, config: EnvironmentConfig):
    """Create a new environment: provision backing store, seed config, and register.

    The name 'local' is reserved for the built-in environment.

    Raises:
        EnvironmentAlreadyExistsError: If environment already registered or name is reserved.
    """
    if entry.id == BUILTIN_LOCAL:
        raise EnvironmentAlreadyExistsError(entry.id)

    registry = _load_registry()
    if entry.id in registry:
        raise EnvironmentAlreadyExistsError(entry.id)

    load_database_module(entry.driver).create_environment(entry, config)

    entry_data = {"driver": entry.driver}
    if entry.location:
        entry_data["location"] = entry.location
    registry[entry.id] = entry_data
    _save_registry(registry)
    log.debug("Environment created", extra={"env": entry.id})


def delete_environment(env_id: str, *, delete_db: bool = True):
    """Remove a registry-managed environment: delete backing store + remove registry entry.

    The built-in 'local' environment cannot be deleted.

    Args:
        env_id: Environment identifier.
        delete_db: Whether to delete the DB file. Defaults to True.

    Raises:
        EnvironmentNotFoundError: If the environment is not registered.
    """
    if env_id == BUILTIN_LOCAL:
        raise EnvironmentNotFoundError(f"Built-in '{BUILTIN_LOCAL}' environment cannot be deleted")

    registry = _load_registry()
    if env_id not in registry:
        raise EnvironmentNotFoundError(f"Environment '{env_id}' not found in registry", {env_id})

    entry_data = registry.pop(env_id)

    if delete_db:
        entry = EnvironmentEntry(id=env_id, **entry_data)
        load_database_module(entry.driver).delete(entry)

    _save_registry(registry)
    log.debug("Environment deleted", extra={"env": env_id})


# --- Errors ---

class InvalidEnvironmentRegistryError(RuntoolsException):
    """Raised when the environment registry file contains invalid entries."""
    pass


class EnvironmentNotFoundError(RuntoolsException):
    """Exception raised when an environment configuration cannot be found."""

    def __init__(self, message: str, env_ids: Optional[Iterable[str]] = None):
        super().__init__(message)
        self.env_ids: Set[str] = set(env_ids) if env_ids else set()


class AmbiguousEnvironmentError(RuntoolsException):
    """Raised when multiple environments are registered and no env_id was specified."""

    def __init__(self, available: List[str]):
        message = f"Multiple environments available: {available}. Specify one with -e/--env."
        super().__init__(message)
        self.available = available


class EnvironmentAlreadyExistsError(RuntoolsException):
    """Raised when trying to create an environment that already exists."""

    def __init__(self, env_id: str):
        super().__init__(f"Environment '{env_id}' already exists")
        self.env_id = env_id
