"""Lock providers — in-memory claim semantics (must mirror the file/advisory backends)."""
import pytest

from runtools.runcore.err import InvalidStateError
from runtools.runcore.util.lock import Lock, LockProvider, MemoryLockProvider


def test_memory_provider_conforms_to_lock_contract():
    provider = MemoryLockProvider()
    assert isinstance(provider, LockProvider)
    assert isinstance(provider.lock('l1'), Lock)


def test_try_acquire_refuses_held_id_even_within_same_thread():
    provider = MemoryLockProvider()
    held = provider.lock('group')
    assert held.try_acquire() is True
    try:
        # Non-reentrant: a same-thread re-claim must fail like it would on file/advisory locks
        assert provider.lock('group').try_acquire() is False
    finally:
        held.release()

    with provider.lock('group'):  # released -> claimable again
        pass


def test_lock_instance_state_guards():
    provider = MemoryLockProvider()
    lock = provider.lock('guarded')
    with pytest.raises(InvalidStateError):
        lock.release()

    assert lock.try_acquire() is True
    with pytest.raises(InvalidStateError):
        lock.try_acquire()
    with pytest.raises(InvalidStateError):
        lock.acquire()
    lock.release()


def test_distinct_ids_do_not_contend():
    provider = MemoryLockProvider()
    with provider.lock('a'):
        other = provider.lock('b')
        assert other.try_acquire() is True
        other.release()
