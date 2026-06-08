#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""Leader-elected maintenance supervisor.

Drives deadline-insensitive periodic jobs off ``platform.maintenance_schedule``
(jobs 4–9 from the #1911 spec), replacing the pg_cron registrations for
events DLQ/reaper/alert, tenant-logs prune, system-logs prune, and IAM prune.

Architecture contract
---------------------
- One background loop per process; a pg session-level advisory lock (held
  on a dedicated ``managed_transaction`` connection — never a pool checkout
  held across work) ensures exactly one pod fleet-wide performs the jobs.
- Tick behaviour: reclaim stale jobs → fetch due jobs → for each due job:
  mark_running, run with a bounded per-job statement_timeout, mark_done.
  A job raising an exception records status='error' and lets others proceed
  (per-job isolation, resilience matrix).
- Bounded-batch DELETEs: all prune jobs delete in batches of at most
  ``_PRUNE_BATCH`` rows, looping until 0 rows affected, so the first prune
  on a large table never creates a long transaction or WAL spike.
- No @cached anywhere in this module: maintenance_schedule reads are the
  mutable source of truth and prune predicates are time-dependent.
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Optional

from dynastore.modules.catalog.db_init.maintenance_schedule import (
    MaintenanceScheduleRepository,
)
from dynastore.modules.db_config.locking_tools import check_extension_exists
from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    ResultHandler,
    managed_transaction,
)
from dynastore.tools.protocol_helpers import get_engine

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Advisory lock key — must not collide with SoftDeleteReaper (0x5D3A7E1F_C2B84961)
# or any other leader-elected loop.
# ---------------------------------------------------------------------------

_SUPERVISOR_ADVISORY_LOCK_KEY = 0x4D41494E_54454E41  # "MAINTENA" in ASCII hex

# ---------------------------------------------------------------------------
# Job names — must match the strings passed to repo.upsert_job at startup.
# ---------------------------------------------------------------------------

JOB_EVENTS_DLQ_PRUNE = "events_dlq_prune"
JOB_EVENTS_STUCK_REAPER = "events_stuck_reaper"
JOB_EVENTS_PENDING_ALERT = "events_pending_alert"
JOB_TENANT_LOGS_PRUNE = "tenant_logs_prune"
JOB_SYSTEM_LOGS_PRUNE = "system_logs_prune"
JOB_IAM_PRUNE = "iam_prune"

# pg_cron job names this supervisor supersedes. On a non-fresh deploy these may
# already be scheduled in cron.job from a prior boot; we unschedule them once so
# they cannot double-run alongside the supervisor. Per-tenant tenant-logs jobs
# follow the ``monthly_cleanup_logs_<schema>`` shape and are matched by prefix.
_SUPERSEDED_CRON_JOBS = (
    "events_events_retention",
    "events_events_pending_alert",
    "events_events_reaper",
    "monthly_cleanup_system_logs",
    "prune_expired_iam",
)
_SUPERSEDED_TENANT_LOG_PREFIX = "monthly_cleanup_logs_"

# Cadences (seconds)
_CADENCE_DLQ_PRUNE = 86400        # daily
_CADENCE_STUCK_REAPER = 300       # 5 minutes
_CADENCE_PENDING_ALERT = 86400    # daily
_CADENCE_TENANT_LOGS = 2592000    # monthly (30 days)
_CADENCE_SYSTEM_LOGS = 2592000    # monthly (30 days)
_CADENCE_IAM_PRUNE = 86400        # daily

# Bounded-batch DELETE size — no single DELETE removes more than this many rows.
_PRUNE_BATCH = 1000

# Stale-after threshold for reclaim (seconds): a job running for more than
# this long is assumed to belong to a dead leader.
_STALE_AFTER_SECONDS = 3600  # 1 hour

# Per-job statement timeout — a hung job resigns rather than wedging the supervisor.
_JOB_STATEMENT_TIMEOUT_MS = 60_000  # 60 seconds

# Events schema — mirrors events_module._EVENTS_SCHEMA
_EVENTS_SCHEMA = os.getenv("DYNASTORE_EVENTS_SCHEMA", "events")

# IAM schema — always "iam"
_IAM_SCHEMA = "iam"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _set_statement_timeout(conn: Any, timeout_ms: int) -> None:
    """Apply a bounded per-statement timeout on *conn* for the current job."""
    await DQLQuery(
        f"SET LOCAL statement_timeout = {timeout_ms}",
        result_handler=ResultHandler.NONE,
    ).execute(conn)


async def _bounded_batch_delete(
    conn: Any,
    sql_template: str,
    batch_size: int = _PRUNE_BATCH,
    **params: Any,
) -> int:
    """Loop ``DELETE … WHERE … LIMIT batch_size`` until 0 rows affected.

    The caller must already be inside a ``managed_transaction``; this helper
    does not open its own transaction so it inherits the per-job
    ``statement_timeout``.  Returns total rows deleted.
    """
    total = 0
    while True:
        rows = await DQLQuery(
            sql_template,
            result_handler=ResultHandler.ROWCOUNT,
        ).execute(conn, batch_size=batch_size, **params)
        if not rows:
            break
        total += rows
    return total


# ---------------------------------------------------------------------------
# Individual job implementations
# ---------------------------------------------------------------------------


async def _run_events_dlq_prune(conn: Any, dead_letter_days: int) -> int:
    """Delete DEAD_LETTER events older than *dead_letter_days* days."""
    sql = (
        f"DELETE FROM {_EVENTS_SCHEMA}.events "
        f"WHERE ctid IN ("
        f"  SELECT ctid FROM {_EVENTS_SCHEMA}.events "
        f"  WHERE status = 'DEAD_LETTER' "
        f"    AND created_at < NOW() - (:dead_letter_days * INTERVAL '1 day') "
        f"  LIMIT :batch_size"
        f")"
    )
    return await _bounded_batch_delete(conn, sql, dead_letter_days=dead_letter_days)


async def _run_events_stuck_reaper(
    conn: Any, timeout_minutes: int, max_retries: int
) -> int:
    """Reap PROCESSING events older than *timeout_minutes* minutes.

    Ported verbatim from the pg_cron CTE in events_module._register_events_reaper.
    Rows whose retry_count + 1 >= max_retries transition to DEAD_LETTER;
    others reset to PENDING.
    """
    sql = (
        f"WITH expired AS ("
        f"  SELECT shard, event_id, retry_count"
        f"  FROM {_EVENTS_SCHEMA}.events"
        f"  WHERE status = 'PROCESSING'"
        f"    AND processed_at < NOW() - (:timeout_minutes * INTERVAL '1 minute')"
        f"  FOR UPDATE SKIP LOCKED"
        f")"
        f"UPDATE {_EVENTS_SCHEMA}.events e"
        f"  SET status = CASE"
        f"    WHEN expired.retry_count + 1 >= :max_retries THEN 'DEAD_LETTER'"
        f"    ELSE 'PENDING'"
        f"  END,"
        f"  retry_count = expired.retry_count + 1,"
        f"  error_message = 'reaped stale PROCESSING'"
        f"  FROM expired"
        f"  WHERE e.shard = expired.shard AND e.event_id = expired.event_id"
    )
    result = await DQLQuery(sql, result_handler=ResultHandler.ROWCOUNT).execute(
        conn,
        timeout_minutes=timeout_minutes,
        max_retries=max_retries,
    )
    return result or 0


async def _run_events_pending_alert(conn: Any, dead_letter_days: int) -> int:
    """Emit a WARNING log per shard for stale PENDING events.

    Ported verbatim from events_module._register_events_retention alert_cmd.
    No DB state is changed; last_rows = total stale pending count.
    """
    sql = (
        f"SELECT shard,"
        f"       count(*) AS n,"
        f"       EXTRACT(EPOCH FROM NOW()-min(created_at))::bigint AS oldest_age_sec"
        f"  FROM {_EVENTS_SCHEMA}.events"
        f" WHERE status = 'PENDING'"
        f"   AND created_at < NOW() - (:dead_letter_days * INTERVAL '1 day')"
        f" GROUP BY shard"
    )
    rows = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(
        conn, dead_letter_days=dead_letter_days
    )
    total = 0
    for row in (rows or []):
        n = row["n"]
        total += n
        logger.warning(
            "events.pending_stale shard=%s count=%s oldest_age_sec=%s",
            row["shard"], n, row["oldest_age_sec"],
        )
    return total


async def _list_active_catalog_schemas(conn: Any) -> list[str]:
    """Return physical_schema for all non-deleted catalogs."""
    rows = await DQLQuery(
        "SELECT physical_schema FROM catalog.catalogs WHERE deleted_at IS NULL ORDER BY physical_schema",
        result_handler=ResultHandler.ALL,
    ).execute(conn)
    return [r[0] for r in rows] if rows else []


async def _run_tenant_logs_prune(conn: Any) -> int:
    """Delete tenant log rows older than 1 year across all active catalogs."""
    schemas = await _list_active_catalog_schemas(conn)
    total = 0
    for schema in schemas:
        # Use parameterised ctid loop; schema is interpolated (identifier, not value).
        sql = (
            f'DELETE FROM "{schema}".logs '
            f'WHERE ctid IN ('
            f'  SELECT ctid FROM "{schema}".logs '
            f'  WHERE "timestamp" < NOW() - INTERVAL \'1 year\' '
            f'  LIMIT :batch_size'
            f')'
        )
        total += await _bounded_batch_delete(conn, sql)
    return total


async def _run_system_logs_prune(conn: Any) -> int:
    """Delete system_logs rows older than 1 year."""
    sql = (
        'DELETE FROM "catalog"."system_logs" '
        'WHERE ctid IN ('
        '  SELECT ctid FROM "catalog"."system_logs" '
        '  WHERE "timestamp" < NOW() - INTERVAL \'1 year\' '
        '  LIMIT :batch_size'
        ')'
    )
    return await _bounded_batch_delete(conn, sql)


async def _run_iam_prune(conn: Any) -> int:
    """Delete expired IAM tokens, grants, and usage counters.

    Ported verbatim from the PL/pgSQL body in postgres_iam_storage._prune_function_ddl.
    Uses bounded-batch DELETE on each table; returns total rows deleted.
    """
    schema = _IAM_SCHEMA
    total = 0

    # Expired refresh tokens
    sql = (
        f'DELETE FROM "{schema}".refresh_tokens '
        f'WHERE ctid IN ('
        f'  SELECT ctid FROM "{schema}".refresh_tokens '
        f'  WHERE expires_at < NOW() LIMIT :batch_size'
        f')'
    )
    total += await _bounded_batch_delete(conn, sql)

    # Expired OAuth2 authorisation codes
    sql = (
        f'DELETE FROM "{schema}".oauth_codes '
        f'WHERE ctid IN ('
        f'  SELECT ctid FROM "{schema}".oauth_codes '
        f'  WHERE expires_at < NOW() LIMIT :batch_size'
        f')'
    )
    total += await _bounded_batch_delete(conn, sql)

    # Expired OAuth2 access/bearer tokens
    sql = (
        f'DELETE FROM "{schema}".oauth_tokens '
        f'WHERE ctid IN ('
        f'  SELECT ctid FROM "{schema}".oauth_tokens '
        f'  WHERE expires_at < NOW() LIMIT :batch_size'
        f')'
    )
    total += await _bounded_batch_delete(conn, sql)

    # Expired grants (valid_until IS NOT NULL AND valid_until < NOW())
    sql = (
        f'DELETE FROM "{schema}".grants '
        f'WHERE ctid IN ('
        f'  SELECT ctid FROM "{schema}".grants '
        f'  WHERE valid_until IS NOT NULL AND valid_until < NOW() LIMIT :batch_size'
        f')'
    )
    total += await _bounded_batch_delete(conn, sql)

    # Expired usage counter buckets (rate-limit windows)
    sql = (
        f'DELETE FROM "{schema}".usage_counters '
        f'WHERE ctid IN ('
        f'  SELECT ctid FROM "{schema}".usage_counters '
        f'  WHERE expires_at IS NOT NULL AND expires_at < NOW() LIMIT :batch_size'
        f')'
    )
    total += await _bounded_batch_delete(conn, sql)

    # Orphan lifetime usage counters (parent policy gone)
    sql = (
        f'DELETE FROM "{schema}".usage_counters u '
        f'WHERE u.expires_at IS NULL '
        f'  AND NOT EXISTS ('
        f'    SELECT 1 FROM "{schema}".policies p WHERE p.id = u.policy_id'
        f'  )'
    )
    rows = await DQLQuery(sql, result_handler=ResultHandler.ROWCOUNT).execute(conn)
    total += rows or 0

    return total


# ---------------------------------------------------------------------------
# Job dispatch table
# ---------------------------------------------------------------------------

# Map job_name → coroutine factory(conn) → int (rows affected)
# Each factory receives the open connection inside managed_transaction.
# Config params are captured at upsert time; the supervisor reads them once
# per startup when registering jobs.

async def _dispatch_job(job_name: str, conn: Any, config: dict[str, Any]) -> int:
    """Dispatch a single maintenance job by name.

    Returns the number of rows affected (0 if not applicable).
    Raises on unexpected errors so the caller can record status='error'.
    """
    if job_name == JOB_EVENTS_DLQ_PRUNE:
        return await _run_events_dlq_prune(conn, config["dead_letter_days"])
    if job_name == JOB_EVENTS_STUCK_REAPER:
        return await _run_events_stuck_reaper(
            conn, config["timeout_minutes"], config["max_retries"]
        )
    if job_name == JOB_EVENTS_PENDING_ALERT:
        return await _run_events_pending_alert(conn, config["dead_letter_days"])
    if job_name == JOB_TENANT_LOGS_PRUNE:
        return await _run_tenant_logs_prune(conn)
    if job_name == JOB_SYSTEM_LOGS_PRUNE:
        return await _run_system_logs_prune(conn)
    if job_name == JOB_IAM_PRUNE:
        return await _run_iam_prune(conn)
    raise ValueError(f"maintenance_supervisor: unknown job_name {job_name!r}")


# ---------------------------------------------------------------------------
# MaintenanceSupervisor
# ---------------------------------------------------------------------------


class MaintenanceSupervisor:
    """Leader-elected supervisor that drives periodic maintenance jobs.

    Call ``start(shutdown_event)`` from CatalogModule.lifespan to schedule the
    background loop.  The supervisor holds a session-level pg advisory lock on
    a dedicated ``managed_transaction`` connection; exactly one pod fleet-wide
    wins the lock per cadence.

    Jobs are registered via ``repo.upsert_job`` at startup; the supervisor
    reads ``platform.maintenance_schedule`` on every tick (no caching — it is
    the mutable source of truth).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        """Initialise with resolved job config values.

        *config* must contain:
          ``dead_letter_days`` (int)   — from EventsModule.accumulation_policy
          ``timeout_minutes`` (int)    — EVENT_PROCESSING_TIMEOUT_MINUTES env var
          ``max_retries`` (int)        — from EventsModule.accumulation_policy
        """
        self._config = config
        self._task: Optional[asyncio.Task[Any]] = None

    def start(self, shutdown_event: asyncio.Event) -> None:
        """Schedule the supervisor loop as an asyncio background task."""
        self._task = asyncio.create_task(
            self._loop(shutdown_event),
            name="maintenance_supervisor",
        )

    async def stop(self) -> None:
        """Cancel and await the background task."""
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def _loop(self, shutdown_event: asyncio.Event) -> None:
        """Leader-elected outer loop — exits when shutdown_event is set."""
        from dynastore.tools.async_utils import run_leader_loop

        @asynccontextmanager
        async def _acquire_leadership():
            engine = get_engine()
            if engine is None:
                yield False
                return
            try:
                async with managed_transaction(engine) as conn:
                    row = await DQLQuery(
                        "SELECT pg_try_advisory_lock(:key)",
                        result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
                    ).execute(conn, key=_SUPERVISOR_ADVISORY_LOCK_KEY)
                    acquired = bool(row)
                    if acquired:
                        yield True
                        await DQLQuery(
                            "SELECT pg_advisory_unlock(:key)",
                            result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
                        ).execute(conn, key=_SUPERVISOR_ADVISORY_LOCK_KEY)
                    else:
                        yield False
            except Exception:
                yield False

        async def _on_leader() -> None:
            await self.run_once()
            # Cadence sleep: re-evaluate leadership every 60 s (inner cadence).
            # The outer run_leader_loop cadence_seconds controls the outer retry.
            await asyncio.sleep(60.0)

        await run_leader_loop(
            acquire_leadership=_acquire_leadership,
            on_leader=_on_leader,
            name="MaintenanceSupervisor",
            cadence_seconds=60.0,
            is_shutdown=shutdown_event.is_set,
        )

    async def run_once(self) -> None:
        """One full supervisor tick: reclaim stale, then dispatch due jobs.

        Each job runs in its own transaction with a bounded statement_timeout.
        A job failure records status='error' and does not block other jobs.
        """
        engine = get_engine()
        if engine is None:
            logger.warning("maintenance_supervisor: no DB engine — skipping tick.")
            return

        repo = MaintenanceScheduleRepository()
        now = datetime.now(tz=timezone.utc)

        # Step 1: reclaim stale jobs (crashed leader mid-run).
        try:
            async with managed_transaction(engine) as conn:
                reclaimed = await repo.reclaim_stale_jobs(
                    conn, now=now, stale_after_seconds=_STALE_AFTER_SECONDS
                )
            if reclaimed:
                logger.warning(
                    "maintenance_supervisor: reclaimed %d stale job(s).", reclaimed
                )
        except Exception as exc:
            logger.warning(
                "maintenance_supervisor: reclaim_stale_jobs failed: %s — "
                "continuing tick.", exc,
            )

        # Step 2: fetch due jobs.
        try:
            async with managed_transaction(engine) as conn:
                due_jobs = await repo.get_due_jobs(conn, now=now)
        except Exception as exc:
            logger.warning(
                "maintenance_supervisor: get_due_jobs failed: %s — aborting tick.", exc
            )
            return

        if not due_jobs:
            logger.debug("maintenance_supervisor: no due jobs this tick.")
            return

        # Step 3: run each due job in its own transaction.
        for job in due_jobs:
            job_name: str = job["job_name"]
            await self._run_job(engine, repo, job_name, now)

    async def _run_job(
        self,
        engine: Any,
        repo: MaintenanceScheduleRepository,
        job_name: str,
        tick_now: datetime,
    ) -> None:
        """Execute one maintenance job with full lifecycle tracking."""
        async with managed_transaction(engine) as conn:
            await repo.mark_running(conn, job_name, now=tick_now)

        rows: Optional[int] = None
        status = "ok"
        error: Optional[str] = None

        try:
            async with managed_transaction(engine) as conn:
                await _set_statement_timeout(conn, _JOB_STATEMENT_TIMEOUT_MS)
                rows = await _dispatch_job(job_name, conn, self._config)
            logger.info(
                "maintenance_supervisor: job %r done — rows=%s.", job_name, rows
            )
        except Exception as exc:
            status = "error"
            error = str(exc)
            logger.exception(
                "maintenance_supervisor: job %r failed: %s", job_name, exc
            )

        finished_at = datetime.now(tz=timezone.utc)
        try:
            async with managed_transaction(engine) as conn:
                await repo.mark_done(
                    conn,
                    job_name,
                    status=status,
                    error=error,
                    rows=rows,
                    finished_at=finished_at,
                )
        except Exception as exc:
            logger.warning(
                "maintenance_supervisor: mark_done for %r failed: %s — "
                "running_since may be stale until next reclaim tick.",
                job_name, exc,
            )


# ---------------------------------------------------------------------------
# Startup: register job cadences into platform.maintenance_schedule
# ---------------------------------------------------------------------------


async def unschedule_superseded_cron_jobs(engine: Any) -> int:
    """Unschedule any pre-existing pg_cron jobs this supervisor now owns.

    Clean-cut safety for a non-fresh deploy: the events/logs/IAM ``pg_cron``
    registrations are gone from the code, but a database that was provisioned
    before this change may still have those jobs scheduled in ``cron.job`` —
    they would then run *alongside* the supervisor (double-run; the stuck-event
    reaper would double-increment ``retry_count``). This unschedules them once.

    No-op when ``pg_cron`` is absent (fresh / on-prem). Idempotent: after the
    first run there are no matching rows, so subsequent boots delete nothing.
    Returns the number of cron jobs unscheduled.
    """
    async with managed_transaction(engine) as conn:
        if not await check_extension_exists(conn, "pg_cron"):
            return 0
        rows = await DQLQuery(
            "SELECT cron.unschedule(jobid) FROM cron.job "
            "WHERE jobname = ANY(:names) OR jobname LIKE :tenant_prefix",
            result_handler=ResultHandler.ROWCOUNT,
        ).execute(
            conn,
            names=list(_SUPERSEDED_CRON_JOBS),
            tenant_prefix=f"{_SUPERSEDED_TENANT_LOG_PREFIX}%",
        )
    count = rows or 0
    if count:
        logger.info(
            "maintenance_supervisor: unscheduled %d superseded pg_cron job(s) "
            "(events/logs/IAM now driven by the supervisor).",
            count,
        )
    return count


async def register_supervisor_jobs(engine: Any) -> None:
    """Upsert all supervisor-owned job rows into ``platform.maintenance_schedule``.

    Idempotent: uses ON CONFLICT … DO UPDATE.  Designed to run once at
    CatalogModule startup before the supervisor loop is started.
    """
    repo = MaintenanceScheduleRepository()
    jobs = [
        (JOB_EVENTS_DLQ_PRUNE, _CADENCE_DLQ_PRUNE),
        (JOB_EVENTS_STUCK_REAPER, _CADENCE_STUCK_REAPER),
        (JOB_EVENTS_PENDING_ALERT, _CADENCE_PENDING_ALERT),
        (JOB_TENANT_LOGS_PRUNE, _CADENCE_TENANT_LOGS),
        (JOB_SYSTEM_LOGS_PRUNE, _CADENCE_SYSTEM_LOGS),
        (JOB_IAM_PRUNE, _CADENCE_IAM_PRUNE),
    ]
    async with managed_transaction(engine) as conn:
        for job_name, cadence in jobs:
            await repo.upsert_job(conn, job_name, interval_seconds=cadence)
    logger.info(
        "maintenance_supervisor: registered %d job cadences in platform.maintenance_schedule.",
        len(jobs),
    )


def build_supervisor_config() -> dict[str, Any]:
    """Read config values needed by the supervisor from the same sources the old cron code used.

    Mirrors events_module.PostgresEventsDriver.accumulation_policy and the
    EVENT_PROCESSING_TIMEOUT_MINUTES env var — exact same sources, no drift.
    """
    dead_letter_days = int(os.getenv("GLOBAL_EVENT_RETENTION_DAYS", "30"))
    timeout_minutes = int(os.getenv("EVENT_PROCESSING_TIMEOUT_MINUTES", "15"))
    max_retries = 3  # mirrors events_module._MAX_RETRIES
    return {
        "dead_letter_days": dead_letter_days,
        "timeout_minutes": timeout_minutes,
        "max_retries": max_retries,
    }
