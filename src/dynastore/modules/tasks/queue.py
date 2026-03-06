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
tasks/queue.py

Abstract Queue Listener for the DynaStore task dispatch system.

Maintains a dedicated long-lived database connection and emits a signal-bus
event whenever a new task is enqueued.  All datastore-specific terminology is
abstracted: callers see only the signal name ``new_task_queued``.

On SIGTERM / shutdown_event the listener exits cleanly.  Because it holds no
in-memory state, any instance can resume from where another left off.
"""

import asyncio
import logging
from typing import Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from dynastore.tools.async_utils import signal_bus

logger = logging.getLogger(__name__)

# The signal-bus channel name used across the task dispatch system.
NEW_TASK_QUEUED = "new_task_queued"

# For multi-tenant support, we track which schemas have pending tasks to avoid 
# searching all catalog schemas on every wakeup.
_DIRTY_SCHEMAS: set[str] = set()

def mark_schema_dirty(schema: str):
    """Marks a schema as having new tasks."""
    _DIRTY_SCHEMAS.add(schema)

def get_and_clear_dirty_schemas() -> list[str]:
    """Returns the list of dirty schemas and clears the internal set."""
    global _DIRTY_SCHEMAS
    schemas = list(_DIRTY_SCHEMAS)
    _DIRTY_SCHEMAS.clear()
    return schemas

# How long to wait for a push notification before emitting a wakeup anyway.
# This acts as a Janitor-fallback: the Dispatcher wakes up and lets the
# Janitor check for stale ACTIVE tasks even when no new task arrives.
_POLL_TIMEOUT_SECONDS = 30.0


async def start_queue_listener(
    engine: AsyncEngine,
    shutdown_event: asyncio.Event,
    channel: str = NEW_TASK_QUEUED,
    poll_timeout: float = _POLL_TIMEOUT_SECONDS,
) -> None:
    """
    Listens for push-notifications from the database and emits a signal-bus
    event on each arrival.  Completely stateless — safe to restart at any time.

    Args:
        engine:         The async database engine (must support async driver notifications).
        shutdown_event: Set this event to stop the listener gracefully.
        channel:        The notification channel name (abstracts LISTEN/NOTIFY).
        poll_timeout:   Seconds to wait before emitting a periodic wakeup signal.
                        Ensures the Janitor runs even with no new tasks.
    """
    logger.info(f"QueueListener: Starting on channel '{channel}' …")

    while not shutdown_event.is_set():
        try:
            from dynastore.modules.db_config.query_executor import is_async_resource

            if is_async_resource(engine):
                async with engine.connect() as conn:
                    # Access the underlying driver connection
                    raw = await conn.get_raw_connection()
                    driver_conn = getattr(raw, "driver_connection", None)

                    # Check if it's specifically asyncpg for push notifications
                    if driver_conn and hasattr(driver_conn, "add_listener"):
                        queue: asyncio.Queue[Optional[str]] = asyncio.Queue()

                        def _on_notification(connection, pid, channel, payload):
                            queue.put_nowait(payload)

                        await driver_conn.add_listener(channel, _on_notification)
                        logger.info(
                            f"QueueListener: Listening on '{channel}' via asyncpg push notifications."
                        )

                        while not shutdown_event.is_set():
                            try:
                                payload = await asyncio.wait_for(queue.get(), timeout=poll_timeout)
                                # Wake up specific tenant dispatcher if payload provided
                                if payload:
                                    mark_schema_dirty(payload)
                                    await signal_bus.emit(NEW_TASK_QUEUED, identifier=payload)
                                else:
                                    await signal_bus.emit(NEW_TASK_QUEUED)
                            except asyncio.TimeoutError:
                                # Periodic poll wakeup
                                await signal_bus.emit(NEW_TASK_QUEUED)

                        try:
                            await driver_conn.remove_listener(channel, _on_notification)
                        except Exception:
                            pass
                    else:
                        logger.info(
                            f"QueueListener: Push notifications not supported by driver {type(driver_conn)}. Falling back to polling every {poll_timeout}s."
                        )
                        while not shutdown_event.is_set():
                            # Global wakeup
                            await signal_bus.emit(NEW_TASK_QUEUED)
                            await asyncio.sleep(poll_timeout)
            else:
                # Sync engine fallback - always poll since we can't 'async with' a sync connection
                logger.info(
                    f"QueueListener: Sync engine detected. Polling every {poll_timeout}s."
                )
                while not shutdown_event.is_set():
                    await asyncio.sleep(poll_timeout)
                    await signal_bus.emit(NEW_TASK_QUEUED)

        except asyncio.CancelledError:
            logger.info("QueueListener: Cancelled — shutting down.")
            break
        except Exception as e:
            if shutdown_event.is_set():
                break
            logger.error(
                f"QueueListener: Connection error — will reconnect in 5s: {e}",
                exc_info=True,
            )
            await asyncio.sleep(5.0)

    logger.info("QueueListener: Stopped.")
