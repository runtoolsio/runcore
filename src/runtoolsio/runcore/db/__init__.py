import importlib
import pkgutil

from runtoolsio import runcore


def load_database_module(db_type):
    """
    Loads the persistence specified by the parameter and creates a new instance of it.

    Args:
        db_type (str): Type of the persistence to be loaded
    """
    for finder, name, is_pkg in pkgutil.iter_modules(runcore.db.__path__, runcore.db.__name__ + "."):
        if name == runcore.db.__name__ + "." + db_type:
            return importlib.import_module(name)

    raise DatabaseNotFoundError(runcore.db.__name__ + "." + db_type)


class DatabaseNotFoundError(Exception):

    def __init__(self, module_):
        super().__init__(f'Cannot find database module {module_}. Ensure this module is installed '
                         f'or check that the provided persistence type value is correct.')
