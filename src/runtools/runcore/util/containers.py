import functools
import itertools
from collections.abc import MutableMapping
from typing import Iterable


def sequence_view(seq, *, sort_key, asc, limit, offset=0, filter_=None):
    sorted_seq = sorted(seq, key=sort_key, reverse=not asc)
    if filter_:
        sorted_seq = filter(filter_, sorted_seq)
    return itertools.islice(sorted_seq, offset if offset > 0 else 0, (limit + offset) if limit > 0 else None)


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


def ensure_tuple_copy(e):
    if e is None:
        return ()
    if not isinstance(e, (list, tuple, set)):
        features = (e,)

    return tuple(e)


def flatten_dict(dictionary, parent_key='', separator='_'):
    """

    Author: https://stackoverflow.com/a/6027615/5282992
    """
    items = []
    for key, value in dictionary.items():
        new_key = parent_key + separator + key if parent_key else key
        if isinstance(value, MutableMapping):
            items.extend(flatten_dict(value, new_key, separator=separator).items())
        else:
            items.append((new_key, value))

    return dict(items)


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


def get_next_item(dct, key, default=None):
    keys = list(dct.keys())
    try:
        index = keys.index(key)
        return dct[keys[index + 1]] if index + 1 < len(keys) else None
    except ValueError:
        # Key not found in the dictionary
        return default
