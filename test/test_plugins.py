from typing import Optional

from plugins import test_plugin
from runtools.runcore.plugins import Plugin


def test_fetch_plugins():
    """Plugins registered via plugin_name= are discoverable."""
    name2plugin = Plugin.fetch_plugins({'plugin2': {}, 'test_plugin': {}})
    assert len(name2plugin) == 2
    assert isinstance(name2plugin['plugin2'], Plugin2)
    assert isinstance(name2plugin['test_plugin'], test_plugin.TestPlugin)


def test_fetch_plugin_twice():
    """Without caching, each fetch creates a new instance."""
    name2plugin_first = Plugin.fetch_plugins({'plugin3': {}})
    name2plugin_second = Plugin.fetch_plugins({'plugin3': {}})

    assert name2plugin_first != name2plugin_second


def test_fetch_plugin_cached():
    """With caching, the same instance is returned."""
    name2plugin_first = Plugin.fetch_plugins({'plugin3': {}}, cached=True)
    name2plugin_second = Plugin.fetch_plugins({'plugin3': {}}, cached=True)

    assert name2plugin_first == name2plugin_second


def test_non_existing_plugin_ignored():
    """Unknown plugin names are skipped with a warning."""
    name2plugin = Plugin.fetch_plugins({'plugin2': {}, 'plugin4': {}})
    assert len(name2plugin) == 1
    assert isinstance(name2plugin['plugin2'], Plugin2)


def test_create_invalid_plugins():
    """Plugins that raise on init are skipped."""
    Plugin2.error_on_init = Exception('Must be caught')
    name2plugin = Plugin.fetch_plugins({'plugin2': {}, 'plugin3': {}})
    assert len(name2plugin) == 1
    assert isinstance(name2plugin['plugin3'], Plugin3)


def test_config_passed_to_plugin():
    """Plugin receives its config dict on construction."""
    name2plugin = Plugin.fetch_plugins({'test_plugin': {'key': 'value'}})
    plugin = name2plugin['test_plugin']
    assert plugin.config == {'key': 'value'}


class Plugin2(Plugin, plugin_name='plugin2'):
    error_on_init: Optional[Exception] = None

    def __init__(self, config=None):
        error_to_raise = Plugin2.error_on_init
        Plugin2.error_on_init = None
        if error_to_raise:
            raise error_to_raise

    def on_instance_added(self, job_instance):
        pass

    def on_instance_removed(self, job_instance):
        pass


class Plugin3(Plugin, plugin_name='plugin3'):

    def __init__(self, config=None):
        pass

    def on_instance_added(self, job_instance):
        pass

    def on_instance_removed(self, job_instance):
        pass
