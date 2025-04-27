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


def print_file(path_):
    path_ = expand_user(path_)
    print('Showing file: ' + str(path_))
    with open(path_, 'r') as file:
        print(file.read())
