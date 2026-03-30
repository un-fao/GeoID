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

"""
EventStorageProtocol — abstract storage backend for the DynaStore event bus.

Replaces the concrete EventStorageSPI class that was embedded in event_service.py.

Implementations:
  - PostgreSQL (default): ``EventsModule`` in dynastore.modules.events
    supports_notify=True  → consumer wakes on pg_notify, no polling loop
  - Any other backend:    set supports_notify=False
    → consumer falls back to timed asyncio.sleep between polls

The consumer loop is backend-agnostic:

    events = await storage.consume_batch(scope, batch_size)
    if not events:
        await storage.wait_for_events(timeout=poll_interval)
    # Postgres: woke on pg_notify;  others: woke after timeout
"""

import asyncio
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Protocol, runtime_checkable

if TYPE_CHECKING:
    from dynastore.modules.events.models import EventSubscription, EventSubscriptionCreate
    from dynastore.modules.db_config.query_executor import DbResource


@runtime_checkable
class EventStorageProtocol(Protocol):
    """
    Protocol for durable event storage backends.

    Abstracts the physical storage (PostgreSQL, GCP Pub/Sub, Redis, etc.) for
    the DynaStore event bus.  Consumers depend only on this protocol, never on
    the concrete implementation.
    """

    # ------------------------------------------------------------------
    # Backend capability flag
    # ------------------------------------------------------------------

    supports_notify: bool
    """
    True if the backend supports push-style readiness notification
    (e.g. PostgreSQL LISTEN/NOTIFY).

    When True, ``wait_for_events`` blocks until a notification arrives or the
    timeout expires.  When False, ``wait_for_events`` simply sleeps for the
    full timeout, effectively implementing a plain polling interval.
    """

    # ------------------------------------------------------------------
    # Schema / DDL lifecycle
    # ------------------------------------------------------------------

    async def initialize(self, conn: Any) -> None:
        """
        Create or verify the global event table (e.g. ``events.events``).

        Called once by EventsModule during its lifespan startup, under an
        advisory lock so the operation is safe across multiple Cloud Run
        instances.
        """
        ...

    async def init_catalog_scope(self, conn: Any, catalog_schema: str) -> None:
        """
        Create or verify per-catalog event tables inside ``catalog_schema``.

        Called by CatalogModule once per catalog schema during its lifespan
        startup.  Idempotent (CREATE … IF NOT EXISTS).
        """
        ...

    async def init_collection_scope(
        self, conn: Any, catalog_schema: str, collection_id: str
    ) -> None:
        """
        Create a dedicated partition for a collection in ``catalog_schema``.

        Called by CatalogModule when a new collection is created.
        """
        ...

    async def drop_collection_scope(
        self, conn: Any, catalog_schema: str, collection_id: str
    ) -> None:
        """
        Remove the dedicated partition for a collection in ``catalog_schema``.

        Called by CatalogModule when a collection is hard-deleted.
        """
        ...

    # ------------------------------------------------------------------
    # Produce
    # ------------------------------------------------------------------

    async def publish(
        self,
        event_type: str,
        payload: Dict[str, Any],
        scope: str = "PLATFORM",
        schema_name: Optional[str] = None,
        collection_id: Optional[str] = None,
        dedup_key: Optional[str] = None,
        db_resource: Optional[Any] = None,
    ) -> Optional[str]:
        """
        Persist an event to the durable outbox.

        Returns the ``event_id`` (str) on success, or ``None`` if the event
        was deduplicated (dedup_key already present with a non-terminal status).

        If ``db_resource`` is supplied the INSERT participates in the caller's
        existing transaction (transactional outbox pattern), otherwise the
        implementation opens its own transaction.
        """
        ...

    # ------------------------------------------------------------------
    # Consume
    # ------------------------------------------------------------------

    async def consume_batch(
        self,
        scope: str = "PLATFORM",
        batch_size: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Claim and return a batch of PENDING events for the given scope.

        Uses ``FOR UPDATE SKIP LOCKED`` (or an equivalent backend mechanism)
        to avoid double-processing.  Claimed events are marked PROCESSING.

        Each returned dict contains at minimum:
            event_id, event_type, scope, schema_name, collection_id, payload
        """
        ...

    async def ack(self, event_ids: List[str]) -> None:
        """
        Acknowledge successfully processed events.

        Removes them permanently from the outbox.
        """
        ...

    async def nack(self, event_id: str, error: str) -> None:
        """
        Negative-acknowledge a failed event.

        Increments ``retry_count``.  If max retries are exceeded the event is
        moved to ``DEAD_LETTER`` status; otherwise it is reset to ``PENDING``
        for re-delivery.
        """
        ...

    async def search_events(
        self,
        engine: Any,
        catalog_id: str,
        collection_id: Optional[str] = None,
        event_type: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """
        Search for events matching the given criteria.
        """
        ...

    # ------------------------------------------------------------------
    # Consumer synchronisation
    # ------------------------------------------------------------------

    async def wait_for_events(self, timeout: float = 10.0) -> None:
        """
        Wait until new events may be available, or until *timeout* seconds
        have elapsed.

        PostgreSQL implementation:
            Listens on channel ``dynastore_events_channel`` and returns as
            soon as a ``pg_notify`` is received (or the timeout expires).

        Non-notify implementations:
            Simply ``await asyncio.sleep(timeout)`` — gives the same polling
            semantics without any special-casing in the consumer.
        """
        ...

    # ------------------------------------------------------------------
    # Webhook subscription management
    # ------------------------------------------------------------------

    async def subscribe(
        self,
        subscription_data: "EventSubscriptionCreate",
        engine: Optional["DbResource"] = None,
    ) -> "EventSubscription":
        """Create or update a webhook subscription."""
        ...

    async def unsubscribe(
        self,
        subscriber_name: str,
        event_type: str,
        engine: Optional["DbResource"] = None,
    ) -> Optional["EventSubscription"]:
        """Delete a webhook subscription."""
        ...

    async def get_subscriptions_for_event_type(
        self,
        event_type: str,
        engine: Optional["DbResource"] = None,
    ) -> List["EventSubscription"]:
        """Return all webhook subscribers for an event type."""
        ...
