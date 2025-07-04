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

from runtools.runcore.err import RuntoolsException

CONFIG_DIR = 'runtools'
JOBS_FILE = 'jobs.toml'


def expand_user(file):
    if not isinstance(file, str) or not file.startswith('~'):
        return file

    return os.path.expanduser(file)


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


def find_config_files(pattern: str, *, exclude_cwd: bool = False, raise_if_empty: bool = False) \
        -> Generator[Path, None, None]:
    """
    Find files matching a pattern in the config search path.

    Args:
        pattern: Glob pattern for matching files (e.g., "*.toml")
        exclude_cwd: Whether to exclude the current working directory from the search
        raise_if_empty: Whether to raise ConfigFileNotFoundError if no files are found

    Returns:
        Generator yielding Path objects for matching files

    Raises:
        ConfigFileNotFoundError: If raise_if_empty is True and no files are found
    """
    search_path = runtools_config_file_search_path(exclude_cwd=exclude_cwd)
    found_any = False

    for dir_path in search_path:
        if dir_path.exists() and dir_path.is_dir():
            for file_path in dir_path.glob(pattern):
                if file_path.is_file():
                    found_any = True
                    yield file_path

    if raise_if_empty and not found_any:
        raise ConfigFileNotFoundError(pattern, search_path)


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
        return [path_ / CONFIG_DIR for path_ in search_path]
    else:
        return [search_path[0]] + [path_ / CONFIG_DIR for path_ in search_path[1:]]


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
        return [Path(p) for p in re.split(r":", os.environ['XDG_CONFIG_DIRS'])]
    else:
        return [Path('/etc/xdg')]


def log_file_dir(create: bool = False) -> Path:
    """
    Returns the directory for general runtools application logs.

    Behavior:
    - If running as root: use system-wide log directory `/var/log/runtools`
    - If non-root user:
        - Use `${XDG_CACHE_HOME}/runtools` if XDG_CACHE_HOME is set
        - Otherwise fallback to `${HOME}/.cache/runtools`

    :param create: Whether to create the directory if it does not exist
    :return: Path to the application log directory
    """
    if _is_root():
        # Root user: use standard system log location
        path_ = Path("/var/log/runtools")
    else:
        # Non-root: prefer XDG-compliant user cache directory
        cache_base = os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")
        path_ = Path(cache_base) / "runtools"

    if create:
        path_.mkdir(parents=True, exist_ok=True)

    return path_


def state_dir(create: bool = False) -> Path:
    """Returns the base directory for storing persistent local state data.

    Root behavior:
        /var/lib/runtools

    Non-root behavior:
        $XDG_STATE_HOME/runtools or fallback to ~/.local/state/runtools

    Args:
        create: If True, create the directory if it doesn't exist.

    Returns:
        Path to the base state directory.
    """
    if _is_root():
        path_ = Path("/var/lib/runtools")
    else:
        base = Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local" / "state"))
        path_ = base / "runtools"

    if create:
        path_.mkdir(parents=True, exist_ok=True)

    return path_


def job_log_dir(env: str, job_id: str, create: bool = False) -> Path:
    """Returns the path to a job log dir under a specific environment.

    Uses:
    - log_file_dir() for root (system-wide logs)
    - state_dir() for non-root (persistent local state)

    Path format: {base}/{env}/output/{job_id}

    Args:
        env: Name of the environment.
        job_id: Identifier of the job.
        create: If True, create parent directories as needed.

    Returns:
        Path to the job log dir.
    """
    base_dir = log_file_dir(create=create) if _is_root() else state_dir(create=create)
    log_file = base_dir / env / "output" / job_id

    if create:
        log_file.parent.mkdir(parents=True, exist_ok=True)

    return log_file


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


class ConfigFileAlreadyExists(RuntoolsException, FileExistsError):
    pass


class ConfigFileNotFoundError(RuntoolsException, FileNotFoundError):

    def __init__(self, file, search_path=()):
        self.file = file
        self.search_path = search_path

        if search_path:
            message = f"Config file `{file}` not found in search paths: {', '.join([str(dir_) for dir_ in search_path])}"
        else:
            message = f"Config file `{file}` not found"

        super().__init__(message)
