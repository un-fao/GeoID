# dynastore/modules/tasks/tasks_config.py
import os
from typing import ClassVar, Optional
from pydantic import Field
from dynastore.modules.db_config.platform_config_service import PluginConfig

class TasksPluginConfig(PluginConfig):
    """Configuration for the Background Tasks module."""

    queue_poll_interval: float = Field(
        default_factory=lambda: float(os.environ.get("DYNASTORE_QUEUE_POLL_INTERVAL", "30.0")),
        description="Fallback polling interval (in seconds) for the task queue listener when real-time push notifications are unavailable.",
        ge=0.1
    )

    hard_retry_cap: int = Field(
        default=5,
        ge=1,
        description=(
            "Platform-wide circuit breaker on per-task retries. The dispatcher "
            "stops claiming, the reaper writes DEAD_LETTER, and fail_task "
            "refuses further retries once a row reaches retry_count >= "
            "hard_retry_cap, regardless of the row's individual max_retries. "
            "Defends against re-enqueue loops where a misbehaving runner "
            "creates new rows or fails to mark the row terminal. The pg_cron "
            "reaper SQL is rebuilt at startup; live changes only take effect "
            "on the next service restart."
        ),
    )
