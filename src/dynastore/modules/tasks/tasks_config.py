# dynastore/modules/tasks/tasks_config.py
import os
from pydantic import Field
from dynastore.modules.db_config.platform_config_service import PluginConfig, register_config

TASKS_PLUGIN_CONFIG_ID = "tasks"


@register_config(TASKS_PLUGIN_CONFIG_ID)
class TasksPluginConfig(PluginConfig):
    """Configuration for the Background Tasks module."""

    queue_poll_interval: float = Field(
        default_factory=lambda: float(os.environ.get("DYNASTORE_QUEUE_POLL_INTERVAL", "30.0")),
        description="Fallback polling interval (in seconds) for the task queue listener when real-time push notifications are unavailable.",
        ge=0.1
    )
