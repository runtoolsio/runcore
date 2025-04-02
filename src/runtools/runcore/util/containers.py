import functools
from typing import Iterable



def iterates(func):
    @functools.wraps(func)
    def catcher(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except StopIteration:
            pass

    return catcher


def to_list(e):
    if e is None:
        return []
    return list(e) if isinstance(e, Iterable) else [e]


def to_tuple(e):
    if e is None:
        return ()
    if not isinstance(e, (list, tuple, set)):
        return (e,)

    return tuple(e)


def update_nested_dict(original, updates):
    """
    Recursively update a nested dictionary.

    :param original: The original dictionary to be updated.
    :param updates: The dictionary containing updates.
    :return: Updated dictionary.
    """
    for key, value in updates.items():
        if isinstance(value, dict) and key in original:
            original[key] = update_nested_dict(original.get(key, {}), value)
        else:
            original[key] = value
    return original
