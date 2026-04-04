import os
import tomllib
from pathlib import Path
from shutil import copy
from typing import Dict, Any

from runtools.runcore.paths import package_config_path, ConfigFileAlreadyExists, runtools_config_file_search_path, \
    expand_user


def copy_resource(src: Path, dst: Path, overwrite=False):
    if not dst.parent.is_dir():
        os.makedirs(dst.parent)

    if not dst.exists() or overwrite:
        copy(src, dst)
    else:
        raise FileExistsError('File already exists: ' + str(dst))


def _copy_config(package, filename, dest, overwrite: bool):
    cfg_to_copy = package_config_path(package, filename)
    try:
        copy_resource(cfg_to_copy, dest, overwrite)
        return dest
    except FileExistsError as e:
        raise ConfigFileAlreadyExists(str(e)) from e


def copy_config_to_search_path(package, filename, overwrite: bool):
    # Copy to first dir in search path (excluding the current working directory)
    dest = runtools_config_file_search_path(exclude_cwd=True)[0] / filename
    return _copy_config(package, filename, dest, overwrite)


def copy_config_to_path(package, filename, copy_to, overwrite: bool):
    dest = copy_to / filename
    return _copy_config(package, filename, dest, overwrite)


def read_toml_file(file_path) -> Dict[str, Any]:
    """
    Reads a TOML file and returns its contents as a dictionary.

    Args:
        file_path (str|Path): The path to the TOML file.

    Returns:
        A dictionary representing the TOML data.

    Raises:
        FileNotFoundError: If the file_path does not exist.
        tomllib.TOMLDecodeError: If the file is not valid TOML.
    """
    with open(file_path, 'rb') as file:
        return tomllib.load(file)


def write_toml_file(file_path, data: Dict[str, Any]):
    """Write a dictionary as TOML to a file.

    Args:
        file_path (str|Path): The path to write to.
        data: The dictionary to serialize as TOML.
    """
    with open(file_path, 'w') as f:
        f.write(format_toml(data))
        f.write('\n')


def _format_toml_value(value) -> str:
    """Format a single value as a TOML literal."""
    if value is None:
        return '""'  # TOML has no null; use empty string as placeholder
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return f'"{value}"'
    if isinstance(value, dict):
        return _format_inline_table(value)
    if isinstance(value, list):
        return f"[{', '.join(_format_toml_value(v) for v in value)}]"
    return f'"{value}"'


def _format_inline_table(d: dict) -> str:
    """Format a dict as a TOML inline table: {key = value, ...}."""
    pairs = []
    for k, v in d.items():
        if v is None:
            continue
        pairs.append(f"{k} = {_format_toml_value(v)}")
    return "{" + ", ".join(pairs) + "}"


def format_toml(data: Dict[str, Any], _prefix: str = "") -> str:
    """
    Format a dictionary as a TOML string.

    Handles nested dicts as TOML sections and scalar values as key-value pairs.
    None values are omitted (TOML has no null type).

    Args:
        data: The dictionary to format.
        _prefix: Internal use for building section paths.

    Returns:
        A TOML-formatted string.
    """
    lines = []
    tables = []
    array_tables = []

    for key, value in data.items():
        if value is None:
            continue
        if isinstance(value, dict):
            tables.append((key, value))
        elif isinstance(value, list) and value and isinstance(value[0], dict):
            array_tables.append((key, value))
        elif isinstance(value, bool):
            lines.append(f"{key} = {str(value).lower()}")
        elif isinstance(value, (int, float)):
            lines.append(f"{key} = {value}")
        elif isinstance(value, str):
            lines.append(f'{key} = "{value}"')
        elif isinstance(value, list):
            lines.append(f"{key} = [{', '.join(_format_toml_value(v) for v in value)}]")
        else:
            lines.append(f'{key} = "{value}"')

    for key, table in tables:
        section = f"{_prefix}.{key}" if _prefix else key
        section_content = format_toml(table, section)
        if section_content.strip():
            lines.append(f"\n[{section}]")
            lines.append(section_content.rstrip())

    for key, items in array_tables:
        section = f"{_prefix}.{key}" if _prefix else key
        for item in items:
            lines.append(f"\n[[{section}]]")
            for k, v in item.items():
                if v is None:
                    continue
                lines.append(f"{k} = {_format_toml_value(v)}")

    return "\n".join(lines)


def print_file(path_):
    path_ = expand_user(path_)
    print('Showing file: ' + str(path_))
    with open(path_, 'r') as file:
        print(file.read())
