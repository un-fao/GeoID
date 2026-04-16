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

"""Router config cache — distributed invalidation (L2 Valkey pub/sub) + per-request memoisation (L4).

M6 adds two layers on top of the existing in-process ``@cached`` L1 router cache:

**L2 — Valkey pub/sub invalidation**:
    On every ``set_config`` / ``delete_config`` call the writer publishes a JSON
    message on the ``dynastore:config:invalidate`` channel.  A background asyncio
    task per worker subscribes to the channel and calls ``invalidate_router_cache``
    locally so stale L1 entries are evicted within one publish–subscribe round-trip
    (typically <100 ms on a co-located Valkey instance).

    Falls back gracefully when ``VALKEY_URL`` is not set: invalidation is
    process-local only (same as pre-M6 behaviour).

**L4 — per-request context var**:
    ``get_request_driver_cache()`` returns a mutable dict bound to the current
    asyncio task via :class:`contextvars.ContextVar`.  Callers that call
    ``get_driver`` multiple times with identical arguments within a single request
    skip the ``@cached`` layer entirely and read from the per-request dict.

    FastAPI middleware (or a dependency) should call ``init_request_driver_cache()``
    at request start and ``clear_request_driver_cache(token)`` at response end.
    The dict is GC-eligible as soon as the context var token is reset.

Usage::

    # In lifespan (start background subscriber):
    await RouterCacheInvalidator.start()
    # In lifespan teardown:
    await RouterCacheInvalidator.stop()

    # In config write hooks (after every set_config / delete_config):
    publish_router_invalidation(catalog_id, collection_id)

    # In FastAPI middleware / dependency:
    token = init_request_driver_cache()
    try:
        yield
    finally:
        clear_request_driver_cache(token)

    # In router (inside get_driver / resolve_drivers):
    cache = get_request_driver_cache()
    key = (operation, catalog_id, collection_id, hint)
    if key not in cache:
        cache[key] = await _resolve(...)
    return cache[key]
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from contextvars import ContextVar, Token
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pub/sub channel name
# ---------------------------------------------------------------------------

_INVALIDATE_CHANNEL = "dynastore:config:invalidate"

# ---------------------------------------------------------------------------
# L4 — per-request context var
# ---------------------------------------------------------------------------

# Maps (operation, catalog_id, collection_id, hint) -> List[ResolvedDriver]
_REQUEST_DRIVER_CACHE: ContextVar[Optional[Dict[Tuple, Any]]] = ContextVar(
    "request_driver_cache", default=None
)


def init_request_driver_cache() -> Token:
    """Initialise a fresh per-request driver cache for the current task context.

    Returns the ``Token`` needed to reset the context var; pass it to
    :func:`clear_request_driver_cache` in the response teardown path.
    """
    return _REQUEST_DRIVER_CACHE.set({})


def clear_request_driver_cache(token: Token) -> None:
    """Reset the per-request cache to its pre-request state."""
    _REQUEST_DRIVER_CACHE.reset(token)


def get_request_driver_cache() -> Dict[Tuple, Any]:
    """Return the per-request driver cache dict, or an empty sentinel if not initialised.

    The returned dict is mutable; callers may store resolved drivers in it.
    If middleware has not initialised the cache, returns a standalone empty dict
    (hits are still correct, but are not shared across calls in the same request).
    """
    cache = _REQUEST_DRIVER_CACHE.get()
    if cache is None:
        return {}
    return cache


# ---------------------------------------------------------------------------
# L2 — Valkey pub/sub invalidation
# ---------------------------------------------------------------------------


class RouterCacheInvalidator:
    """Background subscriber that evicts stale L1 routing cache entries.

    One instance per worker process.  Starts a long-lived asyncio task that
    subscribes to ``dynastore:config:invalidate`` and calls
    :func:`~dynastore.modules.storage.router.invalidate_router_cache` whenever
    a config write is published.

    Falls back silently when Valkey is unavailable.
    """

    _task: Optional[asyncio.Task] = None
    _stop_event: asyncio.Event = asyncio.Event()

    @classmethod
    async def start(cls) -> None:
        """Start the background subscriber.  No-op if Valkey is unavailable."""
        valkey_url = os.getenv("VALKEY_URL")
        if not valkey_url:
            logger.debug("RouterCacheInvalidator: VALKEY_URL not set — pub/sub disabled.")
            return

        cls._stop_event = asyncio.Event()
        cls._task = asyncio.create_task(
            cls._subscribe_loop(valkey_url),
            name="router-cache-invalidator",
        )
        logger.info("RouterCacheInvalidator: subscriber task started.")

    @classmethod
    async def stop(cls) -> None:
        """Signal the subscriber task to exit and wait for it."""
        cls._stop_event.set()
        if cls._task and not cls._task.done():
            try:
                cls._task.cancel()
                await asyncio.wait_for(cls._task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
        cls._task = None
        logger.debug("RouterCacheInvalidator: subscriber task stopped.")

    @classmethod
    async def _subscribe_loop(cls, valkey_url: str) -> None:
        """Long-running loop: subscribe to invalidation channel and process messages."""
        from dynastore.modules.storage.router import invalidate_router_cache

        while not cls._stop_event.is_set():
            try:
                import valkey.asyncio as avalkey  # type: ignore[import-untyped]
                client = avalkey.Valkey.from_url(valkey_url, decode_responses=True)
                pubsub = client.pubsub()
                await pubsub.subscribe(_INVALIDATE_CHANNEL)
                logger.debug(
                    "RouterCacheInvalidator: subscribed to %s", _INVALIDATE_CHANNEL
                )
                try:
                    async for message in pubsub.listen():
                        if cls._stop_event.is_set():
                            break
                        if message.get("type") != "message":
                            continue
                        try:
                            payload = json.loads(message["data"])
                            catalog_id: Optional[str] = payload.get("catalog_id")
                            collection_id: Optional[str] = payload.get("collection_id")
                            invalidate_router_cache(catalog_id, collection_id)
                            logger.debug(
                                "RouterCacheInvalidator: evicted router cache for "
                                "catalog=%s collection=%s",
                                catalog_id,
                                collection_id,
                            )
                        except Exception:
                            logger.exception(
                                "RouterCacheInvalidator: error processing invalidation message"
                            )
                finally:
                    await pubsub.unsubscribe(_INVALIDATE_CHANNEL)
                    await client.aclose()

            except asyncio.CancelledError:
                break
            except Exception:
                if not cls._stop_event.is_set():
                    logger.warning(
                        "RouterCacheInvalidator: connection lost, reconnecting in 2s…",
                        exc_info=True,
                    )
                    await asyncio.sleep(2)


def publish_router_invalidation(
    catalog_id: Optional[str],
    collection_id: Optional[str],
) -> None:
    """Fire-and-forget publish of a routing invalidation event.

    Always calls :func:`~dynastore.modules.storage.router.invalidate_router_cache`
    locally (L1 eviction) and, when Valkey is available, schedules a pub/sub
    publish so other workers evict their L1 entries as well.

    Safe to call from both sync and async contexts; scheduling is deferred to the
    running event loop.
    """
    from dynastore.modules.storage.router import invalidate_router_cache

    # Evict local L1 immediately (same-worker coherence — zero latency)
    invalidate_router_cache(catalog_id, collection_id)

    # Publish to Valkey for cross-worker coherence (best-effort)
    valkey_url = os.getenv("VALKEY_URL")
    if not valkey_url:
        return

    payload = json.dumps({"catalog_id": catalog_id, "collection_id": collection_id})

    async def _publish() -> None:
        try:
            import valkey.asyncio as avalkey  # type: ignore[import-untyped]
            client = avalkey.Valkey.from_url(valkey_url, decode_responses=True)
            try:
                await client.publish(_INVALIDATE_CHANNEL, payload)
            finally:
                await client.aclose()
        except Exception:
            logger.debug(
                "RouterCacheInvalidator: failed to publish invalidation to Valkey",
                exc_info=True,
            )

    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_publish(), name="router-invalidation-publish")
    except RuntimeError:
        # No running loop (sync context) — skip Valkey publish; local eviction above is sufficient
        pass
