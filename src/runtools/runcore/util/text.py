import re
from enum import Enum
from fnmatch import fnmatch
from operator import eq
from typing import Dict


def split_params(params, kv_sep="=", subkey_sep=".") -> Dict:
    """
    Converts a sequence of values in format "key{kv_sep}value" to a nested dict.
    Nested keys are separated by {subkey_sep}.
    """
    def nested_dict_assign(dct, keys, val):
        """
        Assigns a value to a nested dictionary, creating intermediate dicts as needed.
        """
        for k in keys[:-1]:
            dct = dct.setdefault(k, {})
        dct[keys[-1]] = val

    result = {}
    if not params:
        return result

    for param in params:
        if kv_sep not in param:
            raise ValueError(f"Parameter must be in format: key{kv_sep}value")

        key, value = param.split(kv_sep)
        nested_keys = key.split(subkey_sep)
        nested_dict_assign(result, nested_keys, value)

    return result


def truncate(text, max_len, truncated_suffix=''):
    text_length = len(text)
    suffix_length = len(truncated_suffix)

    if suffix_length > max_len:
        raise ValueError(f"Truncated suffix length {suffix_length} is larger than max length {max_len}")

    if text_length > max_len:
        return text[:(max_len - suffix_length)] + truncated_suffix

    return text


def partial_match(string, pattern):
    return bool(re.search(pattern, string))


def _always_true(*_):
    return True


def _always_false(*_):
    return False


class MatchingStrategy(Enum):
    """
    Define functions for string match testing where the first parameter is the tested string and the second parameter
    is the pattern.
    """
    EXACT = (eq,)
    FN_MATCH = (fnmatch,)
    PARTIAL = (partial_match,)
    ALWAYS_TRUE = (_always_true,)
    ALWAYS_FALSE = (_always_false,)

    def __call__(self, *args, **kwargs):
        return self.value[0](*args, **kwargs)


def convert_if_number(val):
    if isinstance(val, (int, float)) or not val:
        return val

    if '.' in (dec := val.replace(',', '.')):
        try:
            return float(dec)
        except ValueError:
            pass

    try:
        return int(val)
    except ValueError:
        pass

    return val
