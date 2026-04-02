"""
Job Instance Plugin Module
===========================

Plugins extend job instance lifecycle with additional functionality (notifications, metrics, etc.).
They are auto-discovered from the ``runtools.plugins`` namespace package and configured per environment.

Plugin API:
    Subclass ``Plugin`` and implement ``on_instance_added()`` / ``on_instance_removed()``.
    Plugins receive a config dict on construction from the environment's ``plugins`` config section.

Discovery:
    Plugins are registered automatically when their module is imported. ``load_modules()`` discovers
    modules in the ``runtools.plugins`` namespace subpackage.
    See: https://packaging.python.org/en/latest/guides/creating-and-discovering-plugins/#using-namespace-packages

Lifecycle:
    ``on_open()``             — environment node opened
    ``on_instance_added()``   — new job instance created (subscribe to events here)
    ``on_instance_removed()`` — job instance removed (cleanup)
    ``on_close()``            — environment node closing (release resources)
"""


import importlib
import logging
import pkgutil
from types import ModuleType
from typing import Dict, Type

from runtools.runcore.job import Feature

log = logging.getLogger(__name__)


class Plugin(Feature):
    _name2subclass: Dict[str, Type] = {}
    _name2plugin: Dict[str, 'Plugin'] = {}

    def __init_subclass__(cls, *, plugin_name=None, **kwargs):
        """
        All plugins are registered using subclass registration:
        https://www.python.org/dev/peps/pep-0487/#subclass-registration
        """
        res_name = plugin_name or cls.__module__.split('.')[-1]
        cls._name2subclass[res_name] = cls
        log.debug("event=[plugin_registered] name=[%s] class=[%s]", res_name, cls)

    @classmethod
    def fetch_plugins(cls, plugin_configs: Dict[str, dict], *, cached=False) -> Dict[str, 'Plugin']:
        """Instantiate registered plugins with their configuration.

        Args:
            plugin_configs: Mapping of plugin name to plugin-specific config dict.
            cached: If True, reuse previously cached plugin instances (ignores new config for cached entries).

        Returns:
            Dict mapping plugin name to instantiated Plugin.
        """
        if not plugin_configs:
            raise ValueError("Plugins not specified")

        if cached:
            initialized = {name: cls._name2plugin[name] for name in plugin_configs if name in cls._name2plugin}
        else:
            initialized = {}

        for name, config in ((n, c) for n, c in plugin_configs.items() if n not in initialized):
            try:
                plugin_cls = Plugin._name2subclass[name]
            except KeyError:
                log.warning("event=[plugin_not_found] name=[%s]", name)
                continue
            try:
                plugin = plugin_cls(config)
                initialized[name] = plugin
                log.debug("event=[plugin_created] name=[%s] plugin=[%s]", name, plugin)
                if cached:
                    cls._name2plugin[name] = plugin
            except PluginDisabledError as e:
                log.warning("event=[plugin_disabled] name=[%s] detail=[%s]", name, e)
            except Exception as e:
                log.warning("event=[plugin_instantiation_failed] name=[%s] detail=[%s]", name, e)

        return initialized

    @classmethod
    def close_all(cls):
        for name, plugin in cls._name2plugin.items():
            try:
                plugin.on_close()
            except Exception as e:
                log.warning("event=[plugin_closing_failed] name=[%s] plugin=[%s] detail=[%s]", name, plugin, e)

    def on_close(self):
        """Releases resources held by the plugin. Called when the environment node closes."""
        pass


class PluginDisabledError(Exception):
    """
    This exception can be thrown from plugin's init method to signalise that there is a condition preventing
    the plugin to work. It can be an initialization error, missing configuration, etc.
    """

    def __init__(self, message: str):
        super().__init__(message)


def load_modules(modules, *, package=None) -> Dict[str, ModuleType]:
    """
    Utility function to ensure all plugins are registered before use.
    Users of the plugins API should call this before utilizing any plugin.

    Args:
        modules (List[str]): Modules where plugins are defined.
        package (ModuleType, optional): Base package for plugins. Defaults to `runtools.plugins` namespace sub-package.
    """

    if not modules:
        raise ValueError("Modules for discovery not specified")

    if package is None:
        import runtools.plugins
        package = runtools.plugins

    discovered_modules = [name for _, name, __ in pkgutil.iter_modules(package.__path__, package.__name__ + ".")]
    log.debug("event=[plugin_modules_discovered] names=[%s]", ",".join(discovered_modules))

    name2module = {}
    for name in modules:
        full_name = f"{package.__name__}.{name}"
        if full_name not in discovered_modules:
            log.warning("event=[plugin_module_not_found] module=[%s]", name)
            continue

        try:
            module = importlib.import_module(full_name)
            name2module[name] = module
            log.debug("event=[plugin_module_imported] name=[%s] module=[%s]", module, module)
        except BaseException as e:
            log.exception("event=[plugin_module_invalid] reason=[import_failed] name=[%s] detail=[%s]", name, e)

    return name2module
