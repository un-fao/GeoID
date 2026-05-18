# dynastore/modules/tasks/tasks_config.py
import os
from typing import ClassVar, Dict, List, Tuple
from pydantic import Field, model_validator
from dynastore.models.mutability import Mutable
from dynastore.modules.db_config.plugin_config import PluginConfig

class TasksPluginConfig(PluginConfig):
    """Configuration for the Background Tasks module."""
    _address: ClassVar[Tuple[str, ...]] = ("platform", "tasks")


    queue_poll_interval: Mutable[float] = Field(
        default_factory=lambda: float(os.environ.get("DYNASTORE_QUEUE_POLL_INTERVAL", "30.0")),
        description="Fallback polling interval (in seconds) for the task queue listener when real-time push notifications are unavailable.",
        ge=0.1
    )

    hard_retry_cap: Mutable[int] = Field(
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

    capability_publisher_ttl_seconds: Mutable[float] = Field(
        default=60.0,
        ge=10.0,
        le=600.0,
        description=(
            "TTL (seconds) for capability liveness sentinel keys written to "
            "the shared cache by every pod that can service a capability "
            "(e.g. an Indexer registered in this process). Read by the "
            "reactive reaper (#502): when the last pod with a capability "
            "dies, no one refreshes the key, the TTL expires, and "
            "unclaimable task rows are DLQed on the next dispatcher pass. "
            "Pair with capability_publisher_refresh_seconds <= ttl/2 so "
            "one missed tick is absorbed."
        ),
    )

    capability_publisher_refresh_seconds: Mutable[float] = Field(
        default=30.0,
        ge=5.0,
        description=(
            "How often each pod refreshes its capability sentinel keys. "
            "Must be <= capability_publisher_ttl_seconds / 2 to tolerate a "
            "single missed tick without false-positive DLQs."
        ),
    )

    proactive_sweep_interval_seconds: Mutable[float] = Field(
        default=60.0,
        ge=5.0,
        le=3600.0,
        description=(
            "Interval (seconds) between proactive sweeps that DLQ "
            "PENDING/retry=0 rows whose required capability has no live "
            "worker (issue #524). Reactive path stays as a safety net. "
            "Lower → tighter latency before stuck-PENDING rows leave "
            "PENDING; higher → fewer wakeups when the deployment is "
            "healthy. Read at startup; live changes apply on next pod "
            "restart (same model as hard_retry_cap)."
        ),
    )

    proactive_sweep_min_age_seconds: Mutable[float] = Field(
        default=300.0,
        ge=30.0,
        description=(
            "Minimum age (seconds) a row must be PENDING before the "
            "proactive sweep is allowed to look at it. Guards against "
            "false-positive DLQs during a publisher cold-start window: "
            "must be >= 2 * capability_publisher_ttl_seconds so any "
            "newly-deployed pod has at least one full refresh cycle to "
            "advertise its capability before a sweep can DLQ rows targeting it."
        ),
    )

    @model_validator(mode="after")
    def _enforce_refresh_le_half_ttl(self) -> "TasksPluginConfig":
        if self.capability_publisher_refresh_seconds > self.capability_publisher_ttl_seconds / 2:
            raise ValueError(
                "capability_publisher_refresh_seconds "
                f"({self.capability_publisher_refresh_seconds}s) must be "
                "<= capability_publisher_ttl_seconds / 2 "
                f"({self.capability_publisher_ttl_seconds / 2}s). A refresh "
                "interval larger than half the TTL means one missed tick "
                "expires the sentinel and the reactive reaper false-DLQs "
                "live capabilities."
            )
        min_age_floor = 2.0 * self.capability_publisher_ttl_seconds
        if self.proactive_sweep_min_age_seconds < min_age_floor:
            raise ValueError(
                "proactive_sweep_min_age_seconds "
                f"({self.proactive_sweep_min_age_seconds}s) must be "
                f">= 2 * capability_publisher_ttl_seconds ({min_age_floor}s) "
                "so a freshly-deployed pod has at least one full publisher "
                "cycle to advertise its capability before the proactive "
                "sweeper can DLQ rows targeting it."
            )
        return self


class TaskRoutingConfig(PluginConfig):
    """Service-affinity routing for the global task queue.

    Sibling to ``ItemsRoutingConfig`` but for the tasks tier. Maps
    ``task_type`` → ordered list of service names allowed to claim it.
    Empty/missing entry → any service whose runner can handle the task may
    claim (legacy behaviour).

    Each process compares the configured service names against its own
    ``service_name`` resolved from ``${DYNASTORE_CONFIG_ROOT}/instance.json``
    (see ``modules/db_config/instance.py``).  Per-deployment defaults are
    seeded by dropping a JSON file into ``${DYNASTORE_CONFIG_ROOT}/defaults/``;
    runtime changes go through the standard ``PUT /configs/plugins/task_routing_config``
    admin route and trigger the apply-handler that re-narrows the dispatcher's
    CapabilityMap.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "tasks")


    routing: Mutable[Dict[str, List[str]]] = Field(
        default_factory=dict,
        description=(
            "task_type → list of logical service names. Missing key or empty "
            "list = any capable service may claim. Set per deployment via the "
            "JSON files mounted into ${DYNASTORE_CONFIG_ROOT}/defaults/."
        ),
    )

    routing_disabled: Mutable[bool] = Field(
        default=False,
        description=(
            "Operator kill-switch — when true, the routing filter is a no-op "
            "and any service claims anything its CapabilityMap accepts. Use "
            "for emergency triage."
        ),
    )

    event_consumer_services: Mutable[List[str]] = Field(
        default_factory=list,
        description=(
            "Logical service names that run the catalog event consumer "
            "(durable 16-shard outbox loop in CatalogModule). Empty list = "
            "no service consumes — events accumulate in the outbox until a "
            "service is added. Default-empty is intentional: an unconfigured "
            "deployment fails noisy (queue depth visible in monitoring) "
            "rather than fails silent (connection storm everywhere). Set per "
            "deployment via the JSON files mounted into "
            "${DYNASTORE_CONFIG_ROOT}/defaults/."
        ),
    )
