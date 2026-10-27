"""Environment kind wiring — every kind must be covered by the env-module dispatch tables.

Guard tests: adding a new ``EnvironmentKind`` without wiring these tables should fail here,
not at a user's first connect.
"""
import pytest

from runtools.runcore.env import (
    EnvironmentAlreadyExistsError, EnvironmentEntry, EnvironmentKind, EnvironmentNotFoundError, LocalEnvironmentConfig,
    _CONFIG_TYPES, _db_module, add_environment, create_environment, load_env_config, lookup, save_env_config,
)

DRIVER_CONTRACT = ("create", "create_environment", "exists", "delete")


@pytest.mark.parametrize("kind", EnvironmentKind)
def test_each_kind_resolves_a_storage_driver(kind):
    driver = _db_module(kind)
    for fn in DRIVER_CONTRACT:
        assert callable(getattr(driver, fn))


@pytest.mark.parametrize("kind", EnvironmentKind)
def test_each_kind_has_a_config_type(kind):
    assert kind in _CONFIG_TYPES


def test_negative_tail_cap_rejected():
    """A negative cap would silently drop every published line (and break postgres prunes) —
    invalid config must fail at load, not at the first tail read; 0 = publishing disabled."""
    from pydantic import ValidationError
    from runtools.runcore.output import OutputConfig

    with pytest.raises(ValidationError):
        OutputConfig(tail_cap=-1)
    assert OutputConfig(tail_cap=0).tail_cap == 0


# --- Lifecycle: create provisions a new store; add registers an existing one ---

@pytest.fixture
def machines(tmp_path, monkeypatch):
    """Two machines sharing one environment store: separate registries, the same database location."""
    db = tmp_path / 'shared.db'

    def use(machine):
        monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path / machine))
        return EnvironmentEntry(id='work', kind=EnvironmentKind.LOCAL, location=str(db))

    return use


def test_create_refuses_an_existing_store_instead_of_reseeding_its_config(machines):
    create_environment(machines('a'), LocalEnvironmentConfig())

    with pytest.raises(EnvironmentAlreadyExistsError):
        create_environment(machines('b'), LocalEnvironmentConfig())


def test_add_registers_an_existing_store_and_keeps_its_config(machines):
    entry = machines('a')
    create_environment(entry, LocalEnvironmentConfig())
    config = load_env_config(entry)
    retention = config.retention.model_copy(update={'keep_days': 7})
    save_env_config(entry, config.model_copy(update={'retention': retention}))

    add_environment(machines('b'))

    assert lookup('work').location == entry.location
    assert load_env_config(lookup('work')).retention.keep_days == 7


def test_add_requires_an_existing_store(machines):
    with pytest.raises(EnvironmentNotFoundError):
        add_environment(machines('a'))
