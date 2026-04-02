# For plugin testing in test_plugin.py and test_app_exec_plugin.py
from typing import List, Optional
from weakref import ref

from runtools.runcore.job import JobInstance
from runtools.runcore.plugins import Plugin


class TestPlugin(Plugin, plugin_name='test_plugin'):

    instance_ref: Optional["ref"] = None
    error_on_new_job_instance: Optional[BaseException] = None

    def __init__(self, config=None):
        TestPlugin.instance_ref = ref(self)
        self.config = config or {}
        self.job_instances: List[JobInstance] = []

    def on_instance_added(self, job_instance):
        self.job_instances.append(job_instance)
        error_to_raise = TestPlugin.error_on_new_job_instance
        TestPlugin.error_on_new_job_instance = None
        if error_to_raise:
            raise error_to_raise

    def on_instance_removed(self, job_instance):
        pass
