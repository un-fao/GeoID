#    Copyright 2025 FAO
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

"""
tasks/maintenance.py

Administrative tools for the DynaStore task queue.

These tools are wired into the existing retention-policy infrastructure and
can be called from admin endpoints or scheduled pg_cron jobs.
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Any, Union

from sqlalchemy.ext.asyncio import AsyncEngine

from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    DDLQuery,
    ResultHandler,
    managed_transaction,
)
from dynastore.modules.tasks.models import Task, TaskStatusEnum

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Task statistics
# ---------------------------------------------------------------------------


async def get_task_statistics(engine: AsyncEngine, schema: str) -> Dict[str, Any]:
    """
    Returns a summary of task counts by status for monitoring / health-checks.
    """
    sql = f"""
        SELECT status, COUNT(*) AS cnt
        FROM "{schema}".tasks
        GROUP BY status;
    """
    async with managed_transaction(engine) as conn:
        rows = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(conn) or []
    return {r["status"]: r["cnt"] for r in rows}


# ---------------------------------------------------------------------------
# Dead-letter management
# ---------------------------------------------------------------------------


async def list_dead_letter_tasks(engine: AsyncEngine, schema: str) -> List[Dict[str, Any]]:
    """
    Returns all tasks in DEAD_LETTER state for operator review.
    """
    sql = f"""
        SELECT task_id, task_type, owner_id, retry_count, max_retries,
               timestamp, finished_at, error_message, inputs
        FROM "{schema}".tasks
        WHERE status = 'DEAD_LETTER'
        ORDER BY timestamp ASC;
    """
    async with managed_transaction(engine) as conn:
        return await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(conn) or []


async def requeue_dead_letter_task(
    engine: AsyncEngine, task_id: str, schema: str, reset_retries: bool = True
) -> bool:
    """
    Resets a DEAD_LETTER task back to PENDING for another attempt.
    Only an operator with awareness of why it failed should call this.

    Args:
        reset_retries: If True, resets retry_count to 0 (full fresh start).
                       If False, keeps the count (will fail again on next exhaustion).
    Returns:
        True if the task was found and requeued, False otherwise.
    """
    retry_clause = "retry_count = 0," if reset_retries else ""
    sql = f"""
        UPDATE "{schema}".tasks
        SET status       = 'PENDING',
            {retry_clause}
            locked_until = NULL,
            finished_at  = NULL,
            error_message = NULL
        WHERE task_id = :task_id
          AND status  = 'DEAD_LETTER'
        RETURNING task_id;
    """
    async with managed_transaction(engine) as conn:
        row = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
            conn, task_id=task_id
        )
    if row:
        logger.info(f"Maintenance: Task {task_id} re-queued from DEAD_LETTER.")
        return True
    logger.warning(f"Maintenance: Task {task_id} not found in DEAD_LETTER state.")
    return False


# ---------------------------------------------------------------------------
# Completed task purge
# ---------------------------------------------------------------------------


async def purge_completed_tasks(
    engine: AsyncEngine,
    schema: str,
    older_than: timedelta = timedelta(days=30),
) -> int:
    """
    Deletes COMPLETED and FAILED tasks older than the given age.
    Returns the number of rows deleted.
    """
    cutoff = _now() - older_than
    sql = f"""
        DELETE FROM "{schema}".tasks
        WHERE status IN ('COMPLETED', 'FAILED')
          AND finished_at < :cutoff
        RETURNING task_id;
    """
    async with managed_transaction(engine) as conn:
        rows = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(
            conn, cutoff=cutoff
        ) or []
    count = len(rows)
    logger.info(f"Maintenance: Purged {count} completed/failed task(s) from schema '{schema}'.")
    return count


# ---------------------------------------------------------------------------
# Stale ACTIVE task cleanup (used by Janitor internally, exposed for tooling)
# ---------------------------------------------------------------------------


async def find_stale_active_tasks(
    engine_or_conn: Union[AsyncEngine, Any],
    schema: str,
    stale_threshold: timedelta = timedelta(minutes=10),
) -> List[Dict[str, Any]]:
    """
    Returns ACTIVE tasks whose heartbeat is stale or whose lock has expired.
    The Janitor calls this to decide whether to reset or dead-letter a task.

    Args:
        engine_or_conn: Either an AsyncEngine (creates own transaction) or an
            already-open connection (runs inside the caller's transaction — used
            by the Janitor so the pg_try_advisory_xact_lock stays active).
    """
    cutoff = _now() - stale_threshold
    sql = f"""
        SELECT task_id, task_type, owner_id, retry_count, max_retries,
               timestamp, locked_until, last_heartbeat_at, inputs
        FROM "{schema}".tasks
        WHERE status = 'ACTIVE'
          AND (
              locked_until < NOW()
              OR (last_heartbeat_at IS NOT NULL AND last_heartbeat_at < :cutoff)
              OR (last_heartbeat_at IS NULL AND locked_until < NOW())
          );
    """
    query = DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS)

    # If a raw connection is passed, use it directly (keeps advisory lock active)
    if hasattr(engine_or_conn, 'execute'):
        return await query.execute(engine_or_conn, cutoff=cutoff) or []

    async with managed_transaction(engine_or_conn) as conn:
        return await query.execute(conn, cutoff=cutoff) or []
