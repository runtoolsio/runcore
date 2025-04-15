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


def set_nested_value(data, key_path, value):
    """
    Set a nested value inside a dict/list structure by parsing a key path.
    For example, the key_path "persistence[0].field" means:
        data["persistence"][0]["field"] = value
    """
    # Split the key path on dots to get each token.
    tokens = key_path.split('.')
    current = data
    for i, token in enumerate(tokens):
        # Check if the token contains list index syntax.
        if '[' in token and ']' in token:
            # Split token into dictionary part and index part.
            dict_key, index_part = token.split('[', 1)
            index = int(index_part[:-1])  # remove trailing ']'

            # If the dictionary key doesn't exist or isn't a list, create a new list.
            if dict_key not in current or not isinstance(current[dict_key], list):
                current[dict_key] = []

            # Ensure the list is long enough.
            while len(current[dict_key]) <= index:
                current[dict_key].append({})

            # If this is the last token, update the element directly.
            if i == len(tokens) - 1:
                current[dict_key][index] = value
            else:
                # Otherwise, descend into the indexed element.
                current = current[dict_key][index]
        else:
            # For regular dictionary keys without list indices.
            if i == len(tokens) - 1:
                current[token] = value
            else:
                # If this intermediate key doesn't exist or isn't a dict, create one.
                if token not in current or not isinstance(current[token], dict):
                    current[token] = {}
                current = current[token]


def update_nested_dict(original, updates):
    """
    Recursively update a nested structure (consisting of dictionaries and lists).

    The updates dictionary can have keys that indicate deep paths using a dot
    notation and/or list-index syntax. For example:

        updates = {
            "persistence[0].field": "new_value",
            "nested.key.subkey": 42,
            "another.key": "hello"
        }

    This function will update 'original' accordingly.
    """
    for key, value in updates.items():
        # If the key contains a dot or list index, parse it as a path.
        if '.' in key or ('[' in key and ']' in key):
            set_nested_value(original, key, value)
        else:
            # Otherwise, if both the original and the update for this key are dicts, merge recursively.
            if isinstance(value, dict) and isinstance(original.get(key), dict):
                original[key] = update_nested_dict(original.get(key, {}), value)
            else:
                original[key] = value
    return original
