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
tasks/queue.py — Queue signal system for DynaStore task dispatch.

Leader-election model
─────────────────────
One instance per service type (by ``NAME`` env var) wins a session-level
PostgreSQL advisory lock and becomes the LISTEN leader.  It maintains a
single asyncpg LISTEN socket on ``new_task_queued`` and
``dynastore_events_channel``.

All other instances are followers. They emit a periodic signal-bus event
every ``poll_interval`` seconds to wake the dispatcher for a claim sweep.

Both paths emit ``signal_bus`` events so the Dispatcher loop is identical
regardless of role.

Benefits
────────
• Only ONE persistent LISTEN connection per service type, not per replica.
• No ``platform.task_signals`` table needed — the global ``tasks.tasks``
  table is the single source of truth.
• On leader death the session-level advisory lock is released automatically
  (connection closed) and the next follower that reconnects wins.
"""

import asyncio
import logging
import os
from typing import Optional, Tuple

from sqlalchemy.ext.asyncio import AsyncEngine

from dynastore.tools.async_utils import signal_bus

logger = logging.getLogger(__name__)

# Public channel names — used by dispatcher and other modules.
NEW_TASK_QUEUED = "new_task_queued"
TASK_STATUS_CHANGED = "task_status_changed"
EVENTS_CHANNEL = "dynastore_events_channel"


def _listener_lock_key() -> int:
    """
    Derive a stable 63-bit advisory lock key from the NAME env var.
    Different service types (api, ingestor, provisioner) each get their
    own leader election. Instances with the same NAME compete for the same
    lock — exactly one wins.
    """
    name = os.getenv("NAME", "dynastore")
    return hash(f"dynastore.queue_listener.{name}") & 0x7FFFFFFFFFFFFFFF


# ---------------------------------------------------------------------------
# Leader path: holds LISTEN socket
# ---------------------------------------------------------------------------

async def _run_as_leader(
    driver_conn,
    shutdown_event: asyncio.Event,
    poll_timeout: float,
) -> None:
    """
    Runs on the instance that won the advisory lock.

    Listens on ``new_task_queued`` and ``dynastore_events_channel``.
    For each notification, emits a signal-bus event to wake the dispatcher
    and/or event consumer.

    A periodic timeout fires a signal so the dispatcher can do its janitor
    sweep even when no notifications arrive.
    """
    queue: asyncio.Queue[Tuple[str, Optional[str]]] = asyncio.Queue()

    def _on_new_task(connection, pid, channel, payload):
        queue.put_nowait((NEW_TASK_QUEUED, payload))

    def _on_status_change(connection, pid, channel, payload):
        queue.put_nowait((TASK_STATUS_CHANGED, payload))

    def _on_event(connection, pid, channel, payload):
        queue.put_nowait((EVENTS_CHANNEL, payload))

    await driver_conn.add_listener(NEW_TASK_QUEUED, _on_new_task)
    await driver_conn.add_listener(TASK_STATUS_CHANGED, _on_status_change)
    await driver_conn.add_listener(EVENTS_CHANNEL, _on_event)
    logger.info(
        "QueueListener[leader]: LISTEN active on "
        "new_task_queued + task_status_changed + dynastore_events_channel."
    )

    try:
        while not shutdown_event.is_set():
            try:
                channel_name, payload = await asyncio.wait_for(
                    queue.get(), timeout=poll_timeout
                )
            except asyncio.TimeoutError:
                # Periodic wakeup for janitor / claim sweep
                await signal_bus.emit(NEW_TASK_QUEUED)
                continue

            if channel_name == NEW_TASK_QUEUED:
                # Payload is the task_type from the trigger.
                # Filter against capability map — only wake if we can handle it.
                from dynastore.modules.tasks.runners import capability_map
                if payload and payload not in capability_map.all_types:
                    continue
                await signal_bus.emit(NEW_TASK_QUEUED)

            elif channel_name == TASK_STATUS_CHANGED:
                await signal_bus.emit(TASK_STATUS_CHANGED, identifier=payload)

            elif channel_name == EVENTS_CHANNEL:
                await signal_bus.emit(EVENTS_CHANNEL, identifier=payload)

    finally:
        try:
            await driver_conn.remove_listener(NEW_TASK_QUEUED, _on_new_task)
            await driver_conn.remove_listener(TASK_STATUS_CHANGED, _on_status_change)
            await driver_conn.remove_listener(EVENTS_CHANNEL, _on_event)
        except Exception:
            pass
    logger.info("QueueListener[leader]: LISTEN stopped.")


# ---------------------------------------------------------------------------
# Follower path: periodic poll
# ---------------------------------------------------------------------------

async def _run_as_follower(
    shutdown_event: asyncio.Event,
    poll_interval: float,
) -> None:
    """
    Runs on instances that did not win the advisory lock.

    Emits a periodic signal-bus event to wake the dispatcher for a claim
    sweep. The dispatcher uses ``claim_next()`` with SKIP LOCKED against
    the global table, so even without LISTEN the follower eventually picks
    up tasks.
    """
    logger.info(f"QueueListener[follower]: polling every {poll_interval}s.")

    while not shutdown_event.is_set():
        await asyncio.sleep(poll_interval)
        await signal_bus.emit(NEW_TASK_QUEUED)

    logger.info("QueueListener[follower]: stopped.")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def start_queue_listener(
    engine: AsyncEngine,
    shutdown_event: asyncio.Event,
    channel: str = NEW_TASK_QUEUED,
    poll_timeout: float = 30.0,
) -> None:
    """
    Starts the queue listener with automatic leader election.

    Attempts to acquire a session-level PostgreSQL advisory lock keyed by
    the ``NAME`` env var.
    - Winner  → _run_as_leader  (LISTEN socket).
    - Losers  → _run_as_follower (periodic claim sweep signal).

    On leader death the session lock is released automatically when the
    underlying connection closes, and the next follower to reconnect wins.
    """
    from dynastore.modules.db_config.query_executor import is_async_resource

    lock_key = _listener_lock_key()
    logger.info(f"QueueListener: Starting (lock_key={lock_key:#x}) …")

    # How often followers poll (configurable, default to poll_timeout)
    follower_poll_interval = poll_timeout

    while not shutdown_event.is_set():
        try:
            if not is_async_resource(engine):
                logger.info("QueueListener: Sync engine — follower poll mode only.")
                await _run_as_follower(shutdown_event, poll_interval=follower_poll_interval)
                continue

            async with engine.connect() as conn:
                raw = await conn.get_raw_connection()
                driver_conn = getattr(raw, "driver_connection", None)

                if not (driver_conn and hasattr(driver_conn, "add_listener")):
                    logger.info(
                        f"QueueListener: asyncpg not available "
                        f"(driver={type(driver_conn).__name__}) — follower poll mode."
                    )
                    break

                # Attempt session-level leader election.
                # pg_try_advisory_lock returns TRUE to exactly ONE session.
                lock_acquired = await driver_conn.fetchval(
                    "SELECT pg_try_advisory_lock($1)", lock_key
                )

                if lock_acquired:
                    logger.info("QueueListener: Won leader election.")
                    await _run_as_leader(driver_conn, shutdown_event, poll_timeout)
                    # Connection context exits → session lock auto-released.
                else:
                    logger.info("QueueListener: Another instance is the leader.")

            if shutdown_event.is_set():
                break

            # Either follower or leader crashed — run follower loop,
            # re-attempt leadership on next outer iteration.
            await _run_as_follower(shutdown_event, poll_interval=follower_poll_interval)

        except asyncio.CancelledError:
            logger.info("QueueListener: Cancelled.")
            break
        except Exception as e:
            if shutdown_event.is_set():
                break
            logger.error(
                f"QueueListener: Error — reconnecting in 5 s: {e}", exc_info=True
            )
            await asyncio.sleep(5.0)

    logger.info("QueueListener: Stopped.")
