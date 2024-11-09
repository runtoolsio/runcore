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
from typing import Generator, List, Callable

from runtools.runcore import util
from runtools.runcore.common import RuntoolsException

CONFIG_DIR = 'runcore'
CONFIG_FILE = 'runcore.toml'
JOBS_FILE = 'jobs.toml'
_HOSTINFO_FILE = 'hostinfo'
_LOG_FILE = 'runcore.log'


def _is_root():
    return os.geteuid() == 0


def default_config_file_path() -> Path:
    return package_config_path('runtools.runcore.config', CONFIG_FILE)


def package_config_path(package, filename) -> Path:
    """
    Get the path to the config file using importlib.resources.
    """
    try:
        with path(package, filename) as config_path:
            return config_path
    except FileNotFoundError:
        raise ConfigFileNotFoundError(filename + ' config file not found')


def lookup_config_file():
    return lookup_file_in_config_path(CONFIG_FILE)


def lookup_jobs_file():
    return lookup_file_in_config_path(JOBS_FILE)


def lookup_hostinfo_file():
    return lookup_file_in_config_path(_HOSTINFO_FILE)


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


def socket_dir(create: bool) -> Path:
    """
    1. Root user: /run/runcore
    2. Non-root user: /tmp/taro_${USER} (An alternative may be: ${HOME}/.cache/runcore)

    TODO taro_${USER} should be unique to prevent denial of service attempts:

    :param create: create path directories if not exist
    :return: directory path for unix domain sockets
    :raises FileNotFoundError: when path cannot be created (only if create == True)
    """

    if _is_root():
        path = Path('/run/runcore')
    else:
        path = Path(f"/tmp/taro_{getpass.getuser()}")

    if create:
        path.mkdir(mode=0o700, exist_ok=True)

    return path


def socket_path(socket_name: str, create: bool) -> Path:
    """
    1. Root user: /run/runcore/{socket-name}
    2. Non-root user: /tmp/taro_${USER}/{socket-name} (An alternative may be: ${HOME}/.cache/runcore/{socket-name})

    :param socket_name: socket file name
    :param create: create path directories if not exist
    :return: unix domain socket path
    :raises FileNotFoundError: when path cannot be created (only if create == True)
    """

    return socket_dir(create) / socket_name


def socket_files(file_extension: str) -> Generator[Path, None, None]:
    s_dir = socket_dir(False)
    if s_dir.exists():
        for entry in s_dir.iterdir():
            if entry.is_socket() and file_extension == entry.suffix:
                yield entry


def socket_files_provider(file_extension: str) -> Callable[[], Generator[Path, None, None]]:
    def provider():
        return socket_files(file_extension)

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


def sqlite_db_path(create: bool) -> Path:
    """
    1. Root user: /var/lib/runcore/{db-file}
    2. Non-root user: ${XDG_DATA_HOME}/runcore/{db-file} or default to ${HOME}/.local/share/runcore

    :param create: create path directories if not exist
    :return: db file path
    """

    if _is_root():
        db_path = Path('/var/lib/runcore')

    elif os.environ.get('XDG_DATA_HOME'):
        db_path = Path(os.environ['XDG_DATA_HOME']) / 'runcore'
    else:
        home = Path.home()
        db_path = home / '.local' / 'share' / 'runcore'

    if create:
        db_path.mkdir(parents=True, exist_ok=True)

    return db_path / 'jobs.db'


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
