"""Environment kind wiring — every kind must be covered by the env-module dispatch tables.

Guard tests: adding a new ``EnvironmentKind`` without wiring these tables should fail here,
not at a user's first connect.
"""
import pytest

from runtools.runcore.env import EnvironmentKind, _CONFIG_TYPES, _db_module

DRIVER_CONTRACT = ("create", "create_environment", "exists", "delete")


@pytest.mark.parametrize("kind", EnvironmentKind)
def test_each_kind_resolves_a_storage_driver(kind):
    driver = _db_module(kind)
    for fn in DRIVER_CONTRACT:
        assert callable(getattr(driver, fn))


@pytest.mark.parametrize("kind", EnvironmentKind)
def test_each_kind_has_a_config_type(kind):
    assert kind in _CONFIG_TYPES
