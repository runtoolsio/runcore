"""
Job Instance Plugin Module
===========================

Plugins extend job instance lifecycle with additional functionality (notifications, metrics, etc.).
They are discovered via ``importlib.metadata`` entry points and configured per environment.

Plugin API:
    Subclass ``Plugin`` and implement ``on_instance_added()`` / ``on_instance_removed()``.
    Plugins receive a config dict on construction from the environment's ``plugins`` config section.

Discovery:
    Plugins register themselves via entry points in their ``pyproject.toml``::

        [project.entry-points."runtools.plugins"]
        sns = "taro_sns:SnsPlugin"

    At runtime, ``fetch_plugins()`` loads entry points by name from the ``runtools.plugins`` group.

Lifecycle:
    ``on_open()``             — environment node opened
    ``on_instance_added()``   — new job instance created (subscribe to events here)
    ``on_instance_removed()`` — job instance removed (cleanup)
    ``on_close()``            — environment node closing (release resources)
"""

import logging
from importlib.metadata import entry_points
from typing import Dict, Type

from runtools.runcore.job import Feature

log = logging.getLogger(__name__)

ENTRY_POINT_GROUP = "runtools.plugins"


class Plugin(Feature):
    _name2plugin: Dict[str, 'Plugin'] = {}
    # Fallback registry for plugins not using entry points (e.g., tests)
    _name2subclass: Dict[str, Type] = {}

    def __init_subclass__(cls, *, plugin_name=None, **kwargs):
        super().__init_subclass__(**kwargs)
        if plugin_name:
            cls.name = plugin_name
            cls._name2subclass[plugin_name] = cls

    @classmethod
    def _resolve_class(cls, name: str) -> Type['Plugin'] | None:
        """Resolve a plugin class by name — entry points first, then fallback registry."""
        # Try entry points
        eps = entry_points(group=ENTRY_POINT_GROUP)
        for ep in eps:
            if ep.name == name:
                try:
                    plugin_cls = ep.load()
                except Exception as e:
                    log.warning("Plugin load failed name=%s detail=%s", name, e)
                    return None
                log.debug("Plugin loaded from entry point name=%s class=%s", name, plugin_cls)
                return plugin_cls

        # Fallback to subclass registration
        if name in cls._name2subclass:
            log.debug("Plugin loaded from registry name=%s", name)
            return cls._name2subclass[name]

        return None

    @classmethod
    def fetch_plugins(cls, plugin_configs: Dict[str, dict], *, cached=False) -> Dict[str, 'Plugin']:
        """Instantiate plugins with their configuration.

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
            plugin_cls = cls._resolve_class(name)
            if plugin_cls is None:
                log.warning("Plugin not found name=%s", name)
                continue
            try:
                plugin = plugin_cls(config)
                initialized[name] = plugin
                log.debug("Plugin created name=%s plugin=%s", name, plugin)
                if cached:
                    cls._name2plugin[name] = plugin
            except PluginDisabledError as e:
                log.warning("Plugin disabled name=%s detail=%s", name, e)
            except Exception as e:
                log.warning("Plugin instantiation failed name=%s detail=%s", name, e)

        return initialized

    @classmethod
    def create_all(cls, plugin_configs: Dict[str, dict]) -> tuple['Plugin', ...]:
        """Discover and instantiate plugins. Convenience wrapper for node creation.

        Args:
            plugin_configs: Mapping of plugin name to plugin-specific config dict.
        """
        return tuple(cls.fetch_plugins(plugin_configs).values())

    @classmethod
    def close_all(cls):
        for name, plugin in cls._name2plugin.items():
            try:
                plugin.on_close()
            except Exception as e:
                log.warning("Plugin close failed name=%s detail=%s", name, e)

    def on_close(self):
        """Releases resources held by the plugin. Called when the environment node closes."""
        pass


class PluginDisabledError(Exception):
    """Raised from a plugin's __init__ to signal it cannot be activated (missing config, etc.)."""

    def __init__(self, message: str):
        super().__init__(message)
