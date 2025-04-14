"""
Followed conventions:
 - https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html
 - https://refspecs.linuxfoundation.org/FHS_3.0/fhs/ch03s15.html

Discussion:
 - https://askubuntu.com/questions/14535/whats-the-local-folder-for-in-my-home-directory

 TODO Read XDG env variables: https://wiki.archlinux.org/index.php/XDG_Base_Directory
"""

import getpass
import os
import re
from importlib.resources import path
from pathlib import Path
from typing import Generator, List, Callable, Union

from runtools.runcore import util
from runtools.runcore.err import RuntoolsException

CONFIG_DIR = 'runtools'
JOBS_FILE = 'jobs.toml'
_LOG_FILE = 'runcore.log'


def _is_root():
    return os.geteuid() == 0


def package_config_path(package, filename) -> Path:
    """
    Get the path to the config file using importlib.resources.
    """
    try:
        with path(package, filename) as config_path:
            return config_path
    except FileNotFoundError:
        raise ConfigFileNotFoundError(filename + ' config file not found')


def lookup_jobs_file():
    return lookup_file_in_config_path(JOBS_FILE)


def lookup_file_in_config_path(file) -> Path:
    """Returns config found in the search path
    :return: config file path
    :raise FileNotFoundError: when config lookup failed
    """
    search_path = runtools_config_file_search_path()
    for config_dir in search_path:
        config = config_dir / file
        if config.exists():
            return config

    raise ConfigFileNotFoundError(file, search_path)


def runtools_config_file_search_path(*, exclude_cwd=False) -> List[Path]:
    search_path = config_file_search_path(exclude_cwd=exclude_cwd)

    if exclude_cwd:
        return [path / CONFIG_DIR for path in search_path]
    else:
        return [search_path[0]] + [path / CONFIG_DIR for path in search_path[1:]]


def config_file_search_path(*, exclude_cwd=False) -> List[Path]:
    """Sorted list of directories in which the program should look for configuration files:

    1. Current working directory unless `exclude_cwd` is True
    2. ${XDG_CONFIG_HOME} or defaults to ${HOME}/.config
    3. ${XDG_CONFIG_DIRS} or defaults to /etc/xdg
    4. /etc

    Related discussion: https://stackoverflow.com/questions/1024114
    :return: list of directories for configuration file lookup
    """
    search_path = []
    if not exclude_cwd:
        search_path.append(Path.cwd())

    search_path.append(xdg_config_home())
    search_path += xdg_config_dirs()
    search_path.append(Path('/etc'))

    return search_path


def xdg_config_home() -> Path:
    if os.environ.get('XDG_CONFIG_HOME'):
        return Path(os.environ['XDG_CONFIG_HOME'])
    else:
        return Path.home() / '.config'


def xdg_config_dirs() -> List[Path]:
    if os.environ.get('XDG_CONFIG_DIRS'):
        return [Path(path) for path in re.split(r":", os.environ['XDG_CONFIG_DIRS'])]
    else:
        return [Path('/etc/xdg')]


def log_file_path(create: bool) -> Path:
    """
    1. Root user: /var/log/runcore/{log-file}
    2. Non-root user: ${XDG_CACHE_HOME}/runcore/{log-file} or default to ${HOME}/.cache/runcore

    :param create: create path directories if not exist
    :return: log file path
    """

    if _is_root():
        path = Path('/var/log')
    else:
        if os.environ.get('XDG_CACHE_HOME'):
            path = Path(os.environ['XDG_CACHE_HOME'])
        else:
            home = Path.home()
            path = home / '.cache'

    if create:
        os.makedirs(path / 'runcore', exist_ok=True)

    return path / 'runcore' / 'runcore.log'


"""
XDG_RUNTIME_DIR most likely not suitable for api files as it can gets deleted even when nohup/screen is used:
  https://xdg.freedesktop.narkive.com/kxtUbAAM/xdg-runtime-dir-when-is-a-user-logged-out

https://unix.stackexchange.com/questions/88083/idiomatic-location-for-file-based-sockets-on-debian-systems
goldilocks's answer:
They are commonly found in /tmp or a subdirectory thereof.
You will want to use a subdirectory if you want to restrict access via permissions, since /tmp is world readable.
Note that /run, and all of the other directories mentioned here except /tmp, are only writable by root.
For a system process, this is fine, but if the application may be run by a non-privileged user,
you either want to use /tmp or create a permanent directory somewhere and set permissions on that,
or use a location in the user's $HOME.

More discussion:
https://unix.stackexchange.com/questions/313036/is-a-subdirectory-of-tmp-a-suitable-place-for-unix-sockets
"""


def ensure_dirs(path_):
    path_.mkdir(mode=0o700, exist_ok=True, parents=True)
    return path_


def runtime_dir() -> Path:
    if _is_root():
        return Path(f"/run/runtools")
    else:
        return Path(f"/tmp/runtools_{getpass.getuser()}")  # Alternative may be: ${HOME}/.cache/runtools


def runtime_env_dir():
    return runtime_dir() / 'env'


def list_subdir(root_path: Path, *, pattern=None) -> Generator[Path, None, None]:
    """
    Get a generator of subdirectories in a root path, optionally matching a pattern.

    Args:
        root_path: The root directory to search for subdirectories
        pattern: Optional regex pattern to filter subdirectories

    Returns:
        Generator yielding Path objects for matching subdirectories
    """
    for entry in root_path.iterdir():
        if entry.is_dir() and (pattern is None or re.search(pattern, entry.name)):
            yield entry


def find_files_in_subdir(root_path: Path, file_names: Union[str, List[str]], *, subdir_pattern=None) -> Generator[
    Path, None, None]:
    """
    Get a generator of files with specific names from multiple subdirectories.

    Args:
        root_path: The root directory containing subdirectories to search
        file_names: Either a single file name (str) or a list of file names to look for in each subdirectory
        subdir_pattern: Optional regex pattern to filter subdirectories

    Returns:
        Generator yielding Path objects for matching files
    """
    # Convert single string to list for uniform handling
    if isinstance(file_names, str):
        file_names = [file_names]

    for subdir in list_subdir(root_path, pattern=subdir_pattern):
        for file_name in file_names:
            file_path = subdir / file_name
            if file_path.exists():
                yield file_path


def files_in_subdir_provider(root_path: Path, file_names: Union[str, List[str]], *, subdir_pattern=None) \
        -> Callable[[], Generator[Path, None, None]]:
    """
    Returns a callable function that generates file paths for a specific file name
    across multiple subdirectories.

    Args:
        root_path: The root directory containing subdirectories to search
        file_names: The file name or names to look for in each subdirectory
        subdir_pattern: Optional regex pattern to filter subdirectories

    Returns:
        A callable function that generates Path objects when called
    """

    def provider():
        return find_files_in_subdir(root_path, file_names, subdir_pattern=subdir_pattern)

    return provider


def lock_dir(create: bool) -> Path:
    """
    1. Root user: /run/lock/runcore
    2. Non-root user: /tmp/taro_${USER}

    :param create: create path directories if not exist
    :return: directory path for file locks
    :raises FileNotFoundError: when path cannot be created (only if create == True)
    """

    if _is_root():
        path = Path('/run/lock/runcore')
    else:
        path = Path(f"/tmp/taro_{getpass.getuser()}")

    if create:
        path.mkdir(mode=0o700, exist_ok=True)

    return path


def lock_path(lock_name: str, create: bool) -> Path:
    """
    1. Root user: /run/lock/runcore/{lock-name}
    2. Non-root user: /tmp/taro_${USER}/{lock-name}

    :param lock_name: socket file name
    :param create: create path directories if not exist
    :return: path of a file to be used as a lock
    :raises FileNotFoundError: when path cannot be created (only if create == True)
    """

    return lock_dir(create) / lock_name


def get_data_dir(*, create: bool = False) -> Path:
    """
    Determines the appropriate data directory for persistent application data based on user privileges.

    Follows XDG standards for data storage locations.

    Args:
        create: If True, ensures the directory exists

    Returns:
        Path to the application data directory
    """
    if _is_root():
        data_path = Path('/var/lib/runtools')
    elif os.environ.get('XDG_DATA_HOME'):
        data_path = Path(os.environ['XDG_DATA_HOME']) / 'runtools'
    else:
        home = Path.home()
        data_path = home / '.local' / 'share' / 'runtools'

    if create:
        data_path.mkdir(parents=True, exist_ok=True)

    return data_path


def sqlite_db_path(env_id, *, create: bool = False) -> Path:
    """
    Determines the appropriate path for an environment's SQLite database.

    Args:
        env_id: Environment identifier
        create: If True, ensures the directory exists

    Returns:
        Path to the environment's database file
    """
    data_path = get_data_dir(create=create)
    return data_path / f"{env_id}.db"


def copy_config_to_search_path(package, filename, overwrite: bool):
    cfg_to_copy = package_config_path(package, filename)
    # Copy to first dir in search path
    # TODO Specify where to copy the file - do not use XDG search path
    copy_to = runtools_config_file_search_path(exclude_cwd=True)[0] / filename
    try:
        util.copy_resource(cfg_to_copy, copy_to, overwrite)
        return copy_to
    except FileExistsError as e:
        raise ConfigFileAlreadyExists(str(e)) from e


class ConfigFileAlreadyExists(RuntoolsException, FileExistsError):
    pass


class ConfigFileNotFoundError(RuntoolsException, FileNotFoundError):

    def __init__(self, file, search_path=()):
        self.file = file
        self.search_path = search_path

        if search_path:
            message = f"Config file `{file}` not found in the search path: {', '.join([str(dir_) for dir_ in search_path])}"
        else:
            message = f"Config file `{file}` not found"

        super().__init__(message)
