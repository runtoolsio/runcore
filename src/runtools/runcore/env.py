import logging
from contextlib import contextmanager
from enum import StrEnum
from pathlib import Path
from typing import Optional, Dict, Set, Iterable, Iterator, List, TypeVar, assert_never

from pydantic import BaseModel, ConfigDict, Field

from runtools.runcore import paths
from runtools.runcore.db import EnvironmentDatabase
from runtools.runcore.err import RuntoolsException
from runtools.runcore.output import OutputConfig
from runtools.runcore.util import files

log = logging.getLogger(__name__)

BUILTIN_LOCAL = 'local'


class EnvironmentKind(StrEnum):
    """Curated backend bundle selecting storage, live transport, and coordination together."""
    LOCAL = 'local'        # sqlite + unix-socket directory + file locks
    POSTGRES = 'postgres'  # postgres + polling directory + advisory locks


class RetentionConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    keep_days: Optional[int] = Field(
        default=None,
        description="Default retention for `env prune`: keep finished runs newer than N days "
                    "(0 = remove all). None means no default — prune requires an explicit --keep-days.")


class _EnvironmentConfigBase(BaseModel):
    """Settings shared by all environment kinds.

    Identity and kind live on the EnvironmentEntry — the config is pure behaviour,
    discriminated externally by ``entry.kind`` at load. Persistence is not part of this
    config — the DB must be opened (via EnvironmentEntry) before this config can be
    loaded from it.
    """
    model_config = ConfigDict(frozen=True)

    output: OutputConfig = Field(default_factory=OutputConfig, description="Output configuration")
    retention: RetentionConfig = Field(default_factory=RetentionConfig,
                                       description="Retention configuration for finished runs")
    plugins: Dict[str, dict] = Field(default_factory=dict, description="Plugin name to config mapping")


class LocalEnvironmentConfig(_EnvironmentConfigBase):
    """Configuration of a ``local`` environment (sqlite + unix-socket directory)."""

    root_dir: Optional[Path] = Field(
        default=None,
        description="Transport root: holds component dirs, socket files, and liveness lock files. "
                    "Uses default if None."
    )


class PostgresEnvironmentConfig(_EnvironmentConfigBase):
    """Configuration of a ``postgres`` environment (postgres + polling directory)."""


EnvironmentConfig = LocalEnvironmentConfig | PostgresEnvironmentConfig

_CONFIG_TYPES = {
    EnvironmentKind.LOCAL: LocalEnvironmentConfig,
    EnvironmentKind.POSTGRES: PostgresEnvironmentConfig,
}


# --- Environment entry ---

class EnvironmentEntry(BaseModel):
    """Describes how to reach an environment's database.

    Can be constructed from the registry, from the built-in local convention,
    or programmatically by the user.
    """
    id: str
    kind: EnvironmentKind = EnvironmentKind.LOCAL
    location: Optional[str] = Field(default=None, description="Kind-specific location of the backing store")

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
        return EnvironmentEntry(id=BUILTIN_LOCAL, kind=EnvironmentKind.LOCAL)
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
    entries = [EnvironmentEntry(id=BUILTIN_LOCAL, kind=EnvironmentKind.LOCAL)]
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

def _db_module(kind: EnvironmentKind):
    """Resolve the database backend module for an environment kind.

    Each module exposes the driver contract (``create()``, ``create_environment()``, ``exists()``,
    ``delete()``). Imported lazily per branch so an unused backend's optional deps (e.g. ``psycopg``
    for postgres) are never imported.
    """
    match kind:
        case EnvironmentKind.LOCAL:
            from runtools.runcore.db import sqlite
            return sqlite
        case EnvironmentKind.POSTGRES:
            from runtools.runcore.db import postgres
            return postgres
        case _:
            assert_never(kind)  # new kind added but not wired here


def _create_env_db(entry: EnvironmentEntry) -> EnvironmentDatabase:
    """Create an EnvironmentDatabase instance from an entry (unopened).

    Raises:
        EnvironmentNotFoundError: If the backing store does not exist.
    """
    driver = _db_module(entry.kind)
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
    driver = _db_module(entry.kind)
    if not driver.exists(entry):
        driver.create_environment(entry, LocalEnvironmentConfig())


def load_env_config(entry: EnvironmentEntry) -> EnvironmentConfig:
    """Load environment config from DB. Returns defaults for missing keys."""
    with _create_env_db(entry) as db:
        config_dict = db.load_config(entry.id)
        return _CONFIG_TYPES[entry.kind].model_validate(config_dict)


def save_env_config(entry: EnvironmentEntry, config: EnvironmentConfig):
    """Save environment config to the DB."""
    with _create_env_db(entry) as db:
        db.save_config(entry.id, config.model_dump(mode='json'))


_C = TypeVar('_C', bound=_EnvironmentConfigBase)


@contextmanager
def open_configured_db(env_db: EnvironmentDatabase, env_id: str,
                                config_type: type[_C]) -> Iterator[_C]:
    """Open the environment's store and yield its validated config.

    The shared open+load boilerplate of the per-kind connect functions — internal assembly
    code, like ``connector.compose``. On success the store stays open (ownership passes to
    the composed connector/node); any failure inside the block, config validation included,
    closes it before re-raising.
    """
    env_db.open()
    try:
        yield config_type.model_validate(env_db.load_config(env_id))
    except BaseException:
        env_db.close()
        raise


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

    _db_module(entry.kind).create_environment(entry, config)

    entry_data = {"kind": entry.kind.value}
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
        _db_module(entry.kind).delete(entry)

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
