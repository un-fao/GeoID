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

import logging
import asyncio
import json
from typing import (
    List,
    Optional,
    Any,
    Callable,
    Coroutine,
    Dict,
    Tuple,
    Protocol,
    runtime_checkable,
    Awaitable,
    cast,
    Set,
    Union,
)
from collections import defaultdict
from contextlib import asynccontextmanager
from enum import Enum

from dynastore.modules.events.primitives import (
    EventScope,
    EventRegistry,
    define_event,
)
from dynastore.models.shared_models import EventType, SYSTEM_SCHEMA
from dynastore.models.protocols import EventsProtocol, EventStorageProtocol
from dynastore.models.protocols.event_bus import EventBusProtocol
from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    DDLQuery,
    ResultHandler,
    managed_transaction,
    DbResource,
    QueryExecutionError,
)
from dynastore.modules.db_config.locking_tools import (
    check_trigger_exists,
    check_function_exists,
    check_table_exists,
)
from dynastore.tools.json import CustomJSONEncoder
from dynastore.modules.catalog.lifecycle_manager import (
    lifecycle_registry,
    sync_collection_initializer,
    sync_collection_destroyer,
)
from dynastore.modules.db_config.maintenance_tools import register_cron_job
from dynastore.tools.discovery import get_protocol
from dynastore.modules.concurrency import get_background_executor
from dynastore.models.driver_context import DriverContext

logger = logging.getLogger(__name__)



@sync_collection_initializer()
async def _create_collection_events_partition(
    conn: DbResource, schema: str, catalog_id: str, collection_id: str, **kwargs
) -> None:
    """Delegates partition creation to EventBusProtocol."""
    bus = get_protocol(EventBusProtocol)
    if bus:
        await bus.init_namespace(schema, collection_id, db_resource=conn)


@sync_collection_destroyer()
async def _drop_collection_events_partition(
    conn: DbResource, schema: str, catalog_id: str, collection_id: str, **kwargs
) -> None:
    """Delegates partition destruction to EventBusProtocol."""
    bus = get_protocol(EventBusProtocol)
    if bus:
        await bus.drop_events(schema, collection_id, db_resource=conn)
class CatalogEventType(EventType):
    # Catalog Lifecycle
    BEFORE_CATALOG_CREATION = define_event(
        "before_catalog_creation", EventScope.PLATFORM
    )
    CATALOG_CREATION = define_event("catalog_creation", EventScope.PLATFORM)
    AFTER_CATALOG_CREATION = define_event("after_catalog_creation", EventScope.PLATFORM)

    BEFORE_CATALOG_UPDATE = define_event("before_catalog_update", EventScope.PLATFORM)
    CATALOG_UPDATE = define_event("catalog_update", EventScope.PLATFORM)
    AFTER_CATALOG_UPDATE = define_event("after_catalog_update", EventScope.PLATFORM)

    BEFORE_CATALOG_DELETION = define_event(
        "before_catalog_deletion", EventScope.PLATFORM
    )
    CATALOG_DELETION = define_event("catalog_deletion", EventScope.PLATFORM)
    AFTER_CATALOG_DELETION = define_event("after_catalog_deletion", EventScope.PLATFORM)

    BEFORE_CATALOG_HARD_DELETION = define_event(
        "before_catalog_hard_deletion", EventScope.PLATFORM
    )
    CATALOG_HARD_DELETION = define_event("catalog_hard_deletion", EventScope.PLATFORM)
    AFTER_CATALOG_HARD_DELETION = define_event(
        "after_catalog_hard_deletion", EventScope.PLATFORM
    )
    CATALOG_HARD_DELETION_FAILURE = define_event(
        "catalog_hard_deletion_failure", EventScope.PLATFORM
    )

    # Collection Lifecycle
    BEFORE_COLLECTION_CREATION = define_event(
        "before_collection_creation", EventScope.CATALOG
    )
    COLLECTION_CREATION = define_event("collection_creation", EventScope.CATALOG)
    AFTER_COLLECTION_CREATION = define_event(
        "after_collection_creation", EventScope.CATALOG
    )

    BEFORE_COLLECTION_UPDATE = define_event(
        "before_collection_update", EventScope.CATALOG
    )
    COLLECTION_UPDATE = define_event("collection_update", EventScope.CATALOG)
    AFTER_COLLECTION_UPDATE = define_event(
        "after_collection_update", EventScope.CATALOG
    )

    BEFORE_COLLECTION_DELETION = define_event(
        "before_collection_deletion", EventScope.CATALOG
    )
    COLLECTION_DELETION = define_event("collection_deletion", EventScope.CATALOG)
    AFTER_COLLECTION_DELETION = define_event(
        "after_collection_deletion", EventScope.CATALOG
    )

    BEFORE_COLLECTION_HARD_DELETION = define_event(
        "before_collection_hard_deletion", EventScope.CATALOG
    )
    COLLECTION_HARD_DELETION = define_event(
        "collection_hard_deletion", EventScope.CATALOG
    )
    AFTER_COLLECTION_HARD_DELETION = define_event(
        "after_collection_hard_deletion", EventScope.CATALOG
    )

    # Asset Lifecycle
    BEFORE_ASSET_CREATION = define_event("before_asset_creation", EventScope.COLLECTION)
    ASSET_CREATION = define_event("asset_creation", EventScope.COLLECTION)
    AFTER_ASSET_CREATION = define_event("after_asset_creation", EventScope.COLLECTION)

    BEFORE_ASSET_UPDATE = define_event("before_asset_update", EventScope.COLLECTION)
    ASSET_UPDATE = define_event("asset_update", EventScope.COLLECTION)
    AFTER_ASSET_UPDATE = define_event("after_asset_update", EventScope.COLLECTION)

    BEFORE_ASSET_DELETION = define_event("before_asset_deletion", EventScope.COLLECTION)
    ASSET_DELETION = define_event("asset_deletion", EventScope.COLLECTION)
    AFTER_ASSET_DELETION = define_event("after_asset_deletion", EventScope.COLLECTION)

    BEFORE_ASSET_HARD_DELETION = define_event(
        "before_asset_hard_deletion", EventScope.COLLECTION
    )
    ASSET_HARD_DELETION = define_event("asset_hard_deletion", EventScope.COLLECTION)
    AFTER_ASSET_HARD_DELETION = define_event(
        "after_asset_hard_deletion", EventScope.COLLECTION
    )


    # Item Lifecycle
    BEFORE_ITEM_CREATION = define_event("before_item_creation", EventScope.COLLECTION)
    ITEM_CREATION = define_event("item_creation", EventScope.COLLECTION)
    AFTER_ITEM_CREATION = define_event("after_item_creation", EventScope.COLLECTION)

    BEFORE_ITEM_UPDATE = define_event("before_item_update", EventScope.COLLECTION)
    ITEM_UPDATE = define_event("item_update", EventScope.COLLECTION)
    AFTER_ITEM_UPDATE = define_event("after_item_update", EventScope.COLLECTION)

    BEFORE_ITEM_DELETION = define_event("before_item_deletion", EventScope.COLLECTION)
    ITEM_DELETION = define_event("item_deletion", EventScope.COLLECTION)
    AFTER_ITEM_DELETION = define_event("after_item_deletion", EventScope.COLLECTION)

    BEFORE_ITEM_HARD_DELETION = define_event(
        "before_item_hard_deletion", EventScope.COLLECTION
    )
    ITEM_HARD_DELETION = define_event("item_hard_deletion", EventScope.COLLECTION)
    AFTER_ITEM_HARD_DELETION = define_event(
        "after_item_hard_deletion", EventScope.COLLECTION
    )

    # Bulk Event Lifecycle
    BEFORE_BULK_ITEM_CREATION = define_event("before_bulk_item_creation", EventScope.COLLECTION)
    BULK_ITEM_CREATION = define_event("bulk_item_creation", EventScope.COLLECTION)
    AFTER_BULK_ITEM_CREATION = define_event("after_bulk_item_creation", EventScope.COLLECTION)

    # Task Lifecycle Events (platform-scoped, fired by runners - no module coupling)
    TASK_FAILED = define_event("task.failed", EventScope.PLATFORM)

Listener = Callable[..., Coroutine[Any, Any, None]]




class EventService(EventBusProtocol):
    """
    Manages event registration, emission, and persistence.
    Implements EventBusProtocol (which extends EventsProtocol) for durable
    event delivery via the global tasks.events outbox table.
    """

    # Shared state for listeners across all instances (Singleton behavior for registration)
    _sync_listeners: Dict[str, List[Listener]] = defaultdict(list)
    _async_listeners: Dict[str, List[Listener]] = defaultdict(list)

    def __init__(self):
        self._consumer_running = False
        self._consumer_task = None

    def register(self, event_type: Union[EventType, str], listener: Listener):
        """Register an async listener for an event type (legacy, defaults to sync)."""
        e_val = event_type.value if isinstance(event_type, EventType) else event_type
        self._sync_listeners[e_val].append(listener)
        logger.debug(f"Registered listener {listener} for event {e_val}")

    def sync_event_listener(self, event_type: Union[EventType, str]):
        """Decorator to register a synchronous (in-transaction) event listener."""
        e_val = event_type.value if isinstance(event_type, EventType) else event_type

        def decorator(func: Listener) -> Listener:
            self._sync_listeners[e_val].append(func)
            logger.info(
                f"Registered sync event listener {func.__module__}.{func.__name__} for {e_val}"
            )
            return func

        return decorator

    def async_event_listener(self, event_type: Union[EventType, str]):
        """Decorator to register an asynchronous (background) event listener."""
        e_val = event_type.value if isinstance(event_type, EventType) else event_type

        def decorator(func: Listener) -> Listener:
            self._async_listeners[e_val].append(func)
            logger.info(
                f"Registered async event listener {func.__module__}.{func.__name__} for {e_val}"
            )
            return func

        return decorator

    def unregister(self, event_type: Union[EventType, str], listener: Listener) -> bool:
        """Remove a previously registered listener. Returns True if found and removed."""
        e_val = event_type.value if isinstance(event_type, EventType) else event_type
        for bucket in (self._async_listeners, self._sync_listeners):
            listeners = bucket.get(e_val)
            if listeners and listener in listeners:
                listeners.remove(listener)
                logger.debug(
                    "Unregistered listener %s.%s for %s",
                    listener.__module__, listener.__name__, e_val,
                )
                return True
        return False

    async def emit(
        self,
        event_type: Union[EventType, str],
        *args,
        db_resource: Optional[DbResource] = None,
        raise_on_error: bool = False,
        **kwargs,
    ):
        """
        Emit an event.
        Persists to Outbox if db_resource is provided.
        Fires sync listeners immediately.
        Schedules async listeners.
        """
        e_val = event_type.value if isinstance(event_type, EventType) else event_type

        # Optional: Validate event existence
        if not EventRegistry.is_valid(e_val):
            logger.debug(f"Emitting unregistered event type: {e_val}")

        # 1. Persistence (Outbox Pattern)
        # Determine scope based on registry or heuristics
        scope = EventRegistry._events.get(e_val, EventScope.PLATFORM)

        # Override scope logic based on known types for backward compat if registry is empty/partial
        is_global_event = scope == EventScope.PLATFORM

        if db_resource:
            catalog_id = kwargs.get("catalog_id")
            collection_id = kwargs.get("collection_id")
            payload = {"args": args, "kwargs": kwargs}
            schema_name = None

            if not is_global_event and catalog_id:
                # For tenant-specific events, we need the physical schema
                from dynastore.models.protocols import CatalogsProtocol
                from dynastore.tools.discovery import get_protocol

                catalogs = get_protocol(CatalogsProtocol)
                if catalogs:
                    schema_name = await catalogs.resolve_physical_schema(
                        catalog_id, ctx=DriverContext(db_resource=db_resource)
                    )

            from dynastore.models.protocols import EventStorageProtocol
            from dynastore.tools.discovery import get_protocol
            event_storage = get_protocol(EventStorageProtocol)
            if event_storage:
                await event_storage.publish(
                    event_type=e_val,
                    payload=payload,
                    scope=str(scope),
                    schema_name=schema_name,
                    collection_id=collection_id,
                    db_resource=db_resource,
                )

        # 2. Sync Listeners (In-Transaction, Blocking)
        logger.debug(f"Emitting sync event {e_val}")
        for listener in self._sync_listeners.get(e_val, []):
            try:
                await listener(*args, **kwargs)
            except Exception as e:
                logger.error(
                    f"Error in sync event listener for '{e_val}': {e}", exc_info=True
                )
                if raise_on_error:
                    raise e

        # 3. Async Listeners (Background, Non-Blocking)
        async_listeners = self._async_listeners.get(e_val, [])
        if async_listeners:
            logger.debug(
                f"Scheduling {len(async_listeners)} async listeners for {e_val}"
            )
            for listener in async_listeners:
                try:
                    asyncio.create_task(listener(*args, **kwargs))
                except Exception as e:
                    logger.error(
                        f"Error scheduling async listener for '{e_val}': {e}",
                        exc_info=True,
                    )

    def emit_side_effect(self, event_type: Union[EventType, str], *args, **kwargs):
        self.emit_detached(event_type, *args, **kwargs)

    def emit_detached(self, event_type: Union[EventType, str], *args, **kwargs):
        e_val = event_type.value if isinstance(event_type, EventType) else event_type
        listeners = self._async_listeners.get(e_val, [])
        if not listeners:
            return

        logger.debug(
            f"Scheduling detached event {e_val} for {len(listeners)} async listeners"
        )

        for listener in listeners:
            try:
                asyncio.create_task(listener(*args, **kwargs))
            except Exception as e:
                logger.error(
                    f"Error scheduling detached listener for '{e_val}': {e}",
                    exc_info=True,
                )

    async def search_events(
        self,
        engine: DbResource,
        catalog_id: str,
        collection_id: Optional[str] = None,
        event_type: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Searches events across system and tenant schemas."""
        storage = get_protocol(EventStorageProtocol)
        if storage:
            return await storage.search_events(
                engine=engine,
                catalog_id=catalog_id,
                collection_id=collection_id,
                event_type=event_type,
                limit=limit,
                offset=offset,
            )
        return []

    # --- EventBusProtocol durable outbox methods ---

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
        """Delegates to EventStorageProtocol."""
        storage = get_protocol(EventStorageProtocol)
        if not storage:
            logger.warning("EventStorageProtocol not available for publish")
            return None
        
        return await storage.publish(
            event_type=event_type,
            payload=payload,
            scope=scope,
            schema_name=schema_name,
            collection_id=collection_id,
            dedup_key=dedup_key,
            db_resource=db_resource,
        )

    async def consume_batch(
        self,
        engine: Any,
        batch_size: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Claim and return a batch of pending events.
        Delegates to EventStorageProtocol.
        """
        from dynastore.models.protocols import EventStorageProtocol
        from dynastore.tools.discovery import get_protocol
        
        event_storage = get_protocol(EventStorageProtocol)
        if not event_storage:
            logger.warning("EventStorageProtocol not available for consume_batch")
            return []
            
        return await event_storage.consume_batch(scope="PLATFORM", batch_size=batch_size)

    async def ack(
        self,
        engine: Any,
        event_ids: List[str],
    ) -> None:
        """
        Acknowledge consumed events. Delegates to EventStorageProtocol.
        """
        if not event_ids:
            return

        from dynastore.models.protocols import EventStorageProtocol
        from dynastore.tools.discovery import get_protocol
        
        event_storage = get_protocol(EventStorageProtocol)
        if event_storage:
            await event_storage.ack(event_ids=event_ids)

    async def nack(
        self,
        engine: Any,
        event_id: str,
        error: str,
    ) -> None:
        """
        Negative-acknowledge a consumed event. Delegates to EventStorageProtocol.
        """
        from dynastore.models.protocols import EventStorageProtocol
        from dynastore.tools.discovery import get_protocol
        
        event_storage = get_protocol(EventStorageProtocol)
        if event_storage:
            await event_storage.nack(event_id=event_id, error=error)

    def has_listeners(self) -> bool:
        """Returns True if any async event listeners are registered."""
        return bool(self._async_listeners)

    async def start_consumer(self, shutdown_event: Any) -> None:
        """
        Start the background event consumer that processes events from
        the global outbox using EventStorageProtocol.wait_for_events.
        """
        if getattr(self, "_consumer_running", False):
            logger.warning("EventService: Consumer already running.")
            return

        self._consumer_running = True
        self._consumer_task = None

        async def _consumer_loop():
            logger.info("EventService async consumer loop started.")
            from dynastore.models.protocols import EventStorageProtocol
            from dynastore.tools.discovery import get_protocol

            while not getattr(shutdown_event, "is_set", lambda: False)():
                try:
                    event_storage = get_protocol(EventStorageProtocol)
                    if not event_storage:
                        await asyncio.sleep(5.0)
                        continue

                    events = await self.consume_batch(None, batch_size=100)
                    if not events:
                        # Wait for new events rather than busy-polling
                        await event_storage.wait_for_events(timeout=10.0)
                        continue

                    for event in events:
                        event_type_str = event.get("event_type", "")
                        payload = event.get("payload", {})
                        if isinstance(payload, str):
                            import json
                            payload = json.loads(payload)

                        event_id = str(event.get("event_id") or event.get("id"))
                        try:
                            async_listeners = self._async_listeners.get(event_type_str, [])
                            for listener in async_listeners:
                                args = payload.get("args", [])
                                kwargs = payload.get("kwargs", {})
                                await listener(*args, **kwargs)

                            await self.ack(None, [event_id])
                        except Exception as e:
                            logger.error(f"Failed to process event {event_id}: {e}", exc_info=True)
                            await self.nack(None, event_id, str(e))

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"EventService background consumer error: {e}", exc_info=True)
                    await asyncio.sleep(5.0)

            self._consumer_running = False
            logger.info("EventService async consumer loop stopped.")

        from dynastore.modules.concurrency import get_background_executor
        executor = get_background_executor()
        self._consumer_task = executor.submit(
            _consumer_loop(), task_name="EventServiceConsumerLoop"
        )
        logger.info("EventService: Durable event consumer started via EventStorageProtocol.")

    async def stop_consumer(self) -> None:
        """Stop the background event consumer loop gracefully."""
        self._consumer_running = False
        task = getattr(self, "_consumer_task", None)
        if task is not None:
            task.cancel()
            self._consumer_task = None
            logger.info("EventService: Durable event consumer task cancelled.")

    # --- Tenant event space lifecycle ---

    async def init_tenant_events(self, tenant: str, **extras: Any) -> None:
        """Register a tenant as an event space. Delegates to EventStorageProtocol."""
        storage = get_protocol(EventStorageProtocol)
        if not storage:
            logger.warning(
                "EventStorageProtocol not available — cannot init tenant '%s'.",
                tenant,
            )
            return
        conn = extras.get("db_resource")
        if conn:
            await storage.init_catalog_scope(conn, tenant)
        else:
            from dynastore.models.protocols import DatabaseProtocol

            db = get_protocol(DatabaseProtocol)
            if db is None:
                raise RuntimeError("DatabaseProtocol not registered")
            async with managed_transaction(db.engine) as txn:
                await storage.init_catalog_scope(txn, tenant)

    async def init_namespace(
        self, tenant: str, namespace: str, **extras: Any
    ) -> None:
        """Register a namespace within a tenant event space. Delegates to EventStorageProtocol."""
        storage = get_protocol(EventStorageProtocol)
        if not storage:
            logger.warning(
                "EventStorageProtocol not available — cannot init namespace '%s.%s'.",
                tenant,
                namespace,
            )
            return
        conn = extras.get("db_resource")
        if conn:
            await storage.init_collection_scope(conn, tenant, namespace)
        else:
            from dynastore.models.protocols import DatabaseProtocol

            db = get_protocol(DatabaseProtocol)
            if db is None:
                raise RuntimeError("DatabaseProtocol not registered")
            async with managed_transaction(db.engine) as txn:
                await storage.init_collection_scope(txn, tenant, namespace)

    async def drop_events(
        self, tenant: str, namespace: str, **extras: Any
    ) -> None:
        """Remove event storage for a namespace within a tenant."""
        storage = get_protocol(EventStorageProtocol)
        if not storage:
            return
        conn = extras.get("db_resource")
        if conn:
            await storage.drop_collection_scope(conn, tenant, namespace)
        else:
            from dynastore.models.protocols import DatabaseProtocol

            db = get_protocol(DatabaseProtocol)
            if db is None:
                raise RuntimeError("DatabaseProtocol not registered")
            async with managed_transaction(db.engine) as txn:
                await storage.drop_collection_scope(txn, tenant, namespace)

    @asynccontextmanager
    async def transaction(
        self, detached: bool = False, db_resource: Optional[DbResource] = None
    ):
        buffer: List[Tuple[str, tuple, dict]] = []

        async def emitter(event_type: Union[EventType, str], *args, **kwargs):
            e_val = (
                event_type.value if isinstance(event_type, EventType) else event_type
            )
            if db_resource and "db_resource" not in kwargs:
                kwargs["db_resource"] = db_resource

            # Store immediately if persistent
            await self.emit(event_type, *args, **kwargs)

            # Buffer for potential future use (though emit handles async dispatch already)
            # If 'detached' logic was intended to defer ALL processing, we'd need to block emit here.
            # But standard 'emit' separates sync/async.
            # This transaction manager seems to be for grouping logic primarily.
            buffer.append((e_val, args, kwargs))

        yield emitter


event_service = EventService()


def register_event_listener(event_type: Union[EventType, str], listener: Listener):
    event_service.register(event_type, listener)


sync_event_listener = event_service.sync_event_listener
async_event_listener = event_service.async_event_listener


async def emit_event(
    event_type: Union[EventType, str],
    *args,
    db_resource: Optional[DbResource] = None,
    **kwargs,
):
    await event_service.emit(event_type, *args, db_resource=db_resource, **kwargs)


