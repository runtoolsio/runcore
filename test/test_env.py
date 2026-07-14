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


def test_negative_tail_cap_rejected():
    """A negative cap would silently drop every published line (and break postgres prunes) —
    invalid config must fail at load, not at the first tail read; 0 = publishing disabled."""
    from pydantic import ValidationError
    from runtools.runcore.output import OutputConfig

    with pytest.raises(ValidationError):
        OutputConfig(tail_cap=-1)
    assert OutputConfig(tail_cap=0).tail_cap == 0
