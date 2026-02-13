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
from typing import List, Optional, Any, Callable, Coroutine, Dict, Tuple, Protocol, runtime_checkable, Awaitable, cast, Set, Union
from collections import defaultdict
from contextlib import asynccontextmanager
from enum import Enum

from dynastore.modules.catalog.models import EventType
from dynastore.modules.db_config.query_executor import (
    DQLQuery, DDLQuery, ResultHandler, managed_transaction, DbResource, QueryExecutionError
)
from dynastore.modules.db_config.locking_tools import (
    acquire_lock_if_needed, acquire_startup_lock, check_trigger_exists, 
    check_function_exists, check_table_exists
)
from dynastore.tools.json import CustomJSONEncoder
from dynastore.modules.catalog.tenant_schema import register_tenant_initializer
from dynastore.modules.db_config.maintenance_tools import ensure_future_partitions, register_retention_policy

logger = logging.getLogger(__name__)

# Import system schema constant
from dynastore.models.shared_models import SYSTEM_SCHEMA

# ==============================================================================
#  TENANT INITIALIZATION (Events Slice)
# ==============================================================================

TENANT_CATALOG_EVENTS_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.events (
    id BIGSERIAL,
    event_type VARCHAR NOT NULL,
    catalog_id VARCHAR,
    collection_id VARCHAR,
    item_id VARCHAR,
    payload JSONB,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    processed_at TIMESTAMPTZ,
    status VARCHAR DEFAULT 'PENDING',
    PRIMARY KEY (timestamp, id)
) PARTITION BY RANGE (timestamp);
"""

@register_tenant_initializer
async def _initialize_events_tenant_slice(conn: DbResource, schema: str, catalog_id: str):
    """Initializes the events module's slice of the tenant schema."""
    from dynastore.modules.db_config.locking_tools import execute_safe_ddl
    from dynastore.modules.db_config import maintenance_tools
    try:
        # Events table creation
        async def table_exists_check():
            return await check_table_exists(conn, "events", schema)

        await execute_safe_ddl(
            conn=conn,
            ddl_statement=TENANT_CATALOG_EVENTS_DDL,
            lock_key=f"{schema}_events",
            existence_check=table_exists_check,
            schema=schema
        )

        await maintenance_tools.ensure_future_partitions(conn, schema=schema, table="events", interval="monthly", periods_ahead=12, column="timestamp")
        await register_retention_policy(conn, schema=schema, table="events", policy="prune", interval="daily", retention_period="1 month", column="timestamp")
    except Exception:
        import traceback
        traceback.print_exc()
        raise
    
    # Schema-scoped Function - avoids global lock contention and deadlocks
    # We use the schema name in the function name to make it unique per tenant
    func_name = f"notify_tenant_event_{schema}"
    
    GLOBAL_NOTIFY_FUNC = f"""
    CREATE OR REPLACE FUNCTION "{schema}"."{func_name}"() RETURNS TRIGGER AS $$
    BEGIN
        -- Payload: schema_name
        PERFORM pg_notify('dynastore_events_channel', TG_TABLE_SCHEMA);
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    """
    
    from dynastore.modules.db_config.locking_tools import check_function_exists, acquire_lock_if_needed
    
    async def check_notify_func():
        return await check_function_exists(conn, func_name, schema)
    
    # We lock on the specific schema/function, not a global key
    async with acquire_lock_if_needed(conn, f"init_{schema}_{func_name}", check_notify_func) as should_create:
        if should_create:
            await DDLQuery(GLOBAL_NOTIFY_FUNC).execute(conn)

            # Attach Trigger to catalog_events for this tenant
            TRIGGER_DDL = f"""
            CREATE TRIGGER trg_catalog_events_insert
            AFTER INSERT ON "{schema}".events
            FOR EACH ROW
            EXECUTE FUNCTION "{schema}"."{func_name}"();
            """
            await DDLQuery(TRIGGER_DDL).execute(conn, schema=schema)

# ==============================================================================
#  EVENT REGISTRY (Annotations)
# ==============================================================================

class EventScope(str, Enum):
    PLATFORM = "platform"
    CATALOG = "catalog"
    COLLECTION = "collection"

class EventRegistry:
    """Central registry for dynamic event definitions."""
    _events: Dict[str, EventScope] = {}

    @classmethod
    def register(cls, name: str, scope: EventScope):
        if name in cls._events and cls._events[name] != scope:
            logger.warning(f"Event '{name}' re-registered with different scope: {scope} (was {cls._events[name]})")
        cls._events[name] = scope
        logger.debug(f"Registered event '{name}' with scope '{scope.value}'")

    @classmethod
    def is_valid(cls, name: str) -> bool:
        return name in cls._events

def define_event(name: str, scope: EventScope = EventScope.CATALOG):
    """
    Decorator/Function to register a new event type dynamically.
    Usage:
        MY_EVENT = define_event("my.custom.event", EventScope.CATALOG)
    """
    EventRegistry.register(name, scope)
    return name

# --- Standard Events ---
# These are pre-registered for backward compatibility and core functionality.

class CatalogEventType(EventType):
    # Catalog Lifecycle
    BEFORE_CATALOG_CREATION = define_event("before_catalog_creation", EventScope.PLATFORM)
    CATALOG_CREATION = define_event("catalog_creation", EventScope.PLATFORM)
    AFTER_CATALOG_CREATION = define_event("after_catalog_creation", EventScope.PLATFORM)
    
    BEFORE_CATALOG_UPDATE = define_event("before_catalog_update", EventScope.PLATFORM)
    CATALOG_UPDATE = define_event("catalog_update", EventScope.PLATFORM)
    AFTER_CATALOG_UPDATE = define_event("after_catalog_update", EventScope.PLATFORM)
    
    BEFORE_CATALOG_DELETION = define_event("before_catalog_deletion", EventScope.PLATFORM)
    CATALOG_DELETION = define_event("catalog_deletion", EventScope.PLATFORM)
    AFTER_CATALOG_DELETION = define_event("after_catalog_deletion", EventScope.PLATFORM)
    
    BEFORE_CATALOG_HARD_DELETION = define_event("before_catalog_hard_deletion", EventScope.PLATFORM)
    CATALOG_HARD_DELETION = define_event("catalog_hard_deletion", EventScope.PLATFORM)
    AFTER_CATALOG_HARD_DELETION = define_event("after_catalog_hard_deletion", EventScope.PLATFORM)
    CATALOG_HARD_DELETION_FAILURE = define_event("catalog_hard_deletion_failure", EventScope.PLATFORM)
    
    # Collection Lifecycle
    BEFORE_COLLECTION_CREATION = define_event("before_collection_creation", EventScope.CATALOG)
    COLLECTION_CREATION = define_event("collection_creation", EventScope.CATALOG)
    AFTER_COLLECTION_CREATION = define_event("after_collection_creation", EventScope.CATALOG)
    
    BEFORE_COLLECTION_UPDATE = define_event("before_collection_update", EventScope.CATALOG)
    COLLECTION_UPDATE = define_event("collection_update", EventScope.CATALOG)
    AFTER_COLLECTION_UPDATE = define_event("after_collection_update", EventScope.CATALOG)
    
    BEFORE_COLLECTION_DELETION = define_event("before_collection_deletion", EventScope.CATALOG)
    COLLECTION_DELETION = define_event("collection_deletion", EventScope.CATALOG)
    AFTER_COLLECTION_DELETION = define_event("after_collection_deletion", EventScope.CATALOG)
    
    BEFORE_COLLECTION_HARD_DELETION = define_event("before_collection_hard_deletion", EventScope.CATALOG)
    COLLECTION_HARD_DELETION = define_event("collection_hard_deletion", EventScope.CATALOG)
    AFTER_COLLECTION_HARD_DELETION = define_event("after_collection_hard_deletion", EventScope.CATALOG)
    
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
    
    BEFORE_ASSET_HARD_DELETION = define_event("before_asset_hard_deletion", EventScope.COLLECTION)
    ASSET_HARD_DELETION = define_event("asset_hard_deletion", EventScope.COLLECTION)
    AFTER_ASSET_HARD_DELETION = define_event("after_asset_hard_deletion", EventScope.COLLECTION)

Listener = Callable[..., Coroutine[Any, Any, None]]

@runtime_checkable
class BackgroundRunner(Protocol):
    def run(self, coro: Coroutine[Any, Any, None]) -> None: ...

class DefaultRunner:
    def run(self, coro: Coroutine[Any, Any, None]) -> None:
        from dynastore.modules.concurrency import get_background_executor
        get_background_executor().submit(coro, task_name="event_listener_runner")

# --- Event Storage Extension Point ---

@runtime_checkable
class EventStorageSPI(Protocol):
    async def initialize(self, conn: DbResource) -> None: ...
    async def ensure_partition(self, conn: DbResource, catalog_id: str, schema: Optional[str] = None) -> None: ...
    async def store(self, conn: DbResource, catalog_id: str, event_type: str, payload: Dict[str, Any]) -> None: ...
    async def consume(self, conn: DbResource, limit: int = 100) -> List[Dict[str, Any]]: ...

class GlobalSystemEventStore:
    """
    Implementation of the Outbox Pattern for GLOBAL SYSTEM EVENTS.
    Includes explicit notification trigger on insert.
    """
    
    GLOBAL_SYSTEM_EVENTS_SCHEMA = f"""
    CREATE TABLE IF NOT EXISTS {SYSTEM_SCHEMA}.system_events (
        id BIGSERIAL,
        catalog_id VARCHAR,
        event_type VARCHAR NOT NULL,
        payload JSONB NOT NULL,
        timestamp TIMESTAMPTZ DEFAULT NOW(),
        status VARCHAR DEFAULT 'PENDING',
        PRIMARY KEY (id)
    );
    """
    
    # Notify trigger for the global table (distinct from tenant trigger)
    GLOBAL_TRIGGER_DDL = f"""
    CREATE OR REPLACE TRIGGER trigger_notify_global_event
    AFTER INSERT ON {SYSTEM_SCHEMA}.system_events
    FOR EACH ROW EXECUTE FUNCTION {SYSTEM_SCHEMA}.notify_system_event();
    """

    GLOBAL_NOTIFY_FUNC = f"""
    CREATE OR REPLACE FUNCTION {SYSTEM_SCHEMA}.notify_system_event() RETURNS TRIGGER AS $$
    BEGIN
        PERFORM pg_notify('dynastore_events_channel', TG_TABLE_SCHEMA);
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    """
    
    _insert_query = DQLQuery(
        f"""
        INSERT INTO {SYSTEM_SCHEMA}.system_events (catalog_id, event_type, payload)
        VALUES (:catalog_id, :event_type, :payload)
        RETURNING id;
        """,
        result_handler=ResultHandler.SCALAR_ONE
    )
    
    _consume_query = DQLQuery(
        f"""
        DELETE FROM {SYSTEM_SCHEMA}.system_events
        WHERE id IN (
            SELECT id
            FROM {SYSTEM_SCHEMA}.system_events
            WHERE status = 'PENDING'
            FOR UPDATE SKIP LOCKED
            LIMIT :limit
        )
        RETURNING *;
        """,
        result_handler=ResultHandler.ALL_DICTS
    )

    async def initialize(self, conn: DbResource) -> None:
        
            async def check_table_exists_fn():
                return await check_table_exists(conn, "system_events", SYSTEM_SCHEMA)
            
            async with acquire_lock_if_needed(conn, "system_events", check_table_exists_fn) as should_create:
                if should_create:
                    await DDLQuery(self.GLOBAL_SYSTEM_EVENTS_SCHEMA).execute(conn)
            
            # Ensure the notify function exists (optimized with existence check)
            async def check_notify_func():
                return await check_function_exists(conn, "notify_system_event", SYSTEM_SCHEMA)
            
            async with acquire_lock_if_needed(conn, "notify_system_event", check_notify_func) as should_create:
                if should_create:
                    await DDLQuery(self.GLOBAL_NOTIFY_FUNC).execute(conn)

            # Ensure the trigger exists (optimized with existence check)
            async def check_trigger():
                return await check_trigger_exists(conn, "trigger_notify_global_event", SYSTEM_SCHEMA)
            
            async with acquire_lock_if_needed(conn, "trigger_notify_global_event", check_trigger) as should_create:
                if should_create:
                    await DDLQuery(self.GLOBAL_TRIGGER_DDL).execute(conn)

    async def store(self, conn: DbResource, catalog_id: Optional[str], event_type: str, payload: Dict[str, Any], schema: Optional[str] = None) -> None:
        await self._insert_query.execute(
            conn, 
            catalog_id=catalog_id, 
            event_type=event_type, 
            payload=json.dumps(payload, cls=CustomJSONEncoder)
        )

    async def consume(self, conn: DbResource, limit: int = 100) -> List[Dict[str, Any]]:
        return await self._consume_query.execute(conn, limit=limit)

_tenant_event_insert_query = DQLQuery(
    """
    INSERT INTO {schema}.events (event_type, catalog_id, collection_id, item_id, payload)
    VALUES (:event_type, :catalog_id, :collection_id, :item_id, :payload)
    RETURNING id;
    """,
    result_handler=ResultHandler.SCALAR_ONE
)

class EventConsumerWorker:
    """
    Optimized background worker that listens for DB notifications (LISTEN/NOTIFY)
    to process events immediately, falling back to polling only as a health check.
    """
    CHANNEL_NAME = "dynastore_events_channel"

    def __init__(
        self, 
        engine: DbResource, 
        storage: EventStorageSPI,
        handler: Callable[[Dict[str, Any]], Awaitable[None]], 
        batch_size: int = 100, 
        poll_interval: float = 10.0 # Increased poll interval since we have NOTIFY
    ):
        self.engine = engine
        self.storage = storage
        self.handler = handler
        self.batch_size = batch_size
        self.poll_interval = poll_interval
        self._task: Optional[asyncio.Task] = None
        self._listen_task: Optional[asyncio.Task] = None
        self._running = False
        self._notify_event = asyncio.Event()

    async def start(self):
        """Starts the background worker loops."""
        if self._running:
            return
        self._running = True
        from dynastore.modules.concurrency import get_background_executor
        executor = get_background_executor()
        self._task = executor.submit(self._processing_loop(), task_name="EventProcessingLoop")
        self._listen_task = executor.submit(self._listener_loop(), task_name="EventListenerLoop")
        logger.info("EventConsumerWorker started with LISTEN/NOTIFY optimization.")

    async def stop(self):
        """Stops the background worker gracefully."""
        if not self._running:
            return
        self._running = False
        
        if self._listen_task:
            self._listen_task.cancel()
        
        if self._task:
            self._task.cancel()
            # Trigger event to wake up processing loop if it's waiting
            self._notify_event.set()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("EventConsumerWorker stopped.")

    async def _listener_loop(self):
        """Dedicated connection loop to LISTEN for notifications."""
        logger.info(f"Starting DB Listener on channel '{self.CHANNEL_NAME}'...")
        while self._running:
            conn = None
            try:
                # We need a raw asyncpg connection for LISTEN
                # Accessing the raw pool from the SQLAlchemy engine
                raw_conn = await self.engine.raw_connection()
                try:
                    conn = raw_conn.driver_connection
                    
                    # Define callback
                    def notify_handler(connection, pid, channel, payload):
                        # logger.debug(f"Received notification on {channel}: {payload}")
                        self._notify_event.set()

                    await conn.add_listener(self.CHANNEL_NAME, notify_handler)
                    
                    # Keep connection open and waiting
                    while self._running:
                        await asyncio.sleep(self.poll_interval)
                        # Ping or check connection health if needed
                        if conn.is_closed():
                            break
                            
                finally:
                    if conn and not conn.is_closed():
                        try:
                            await conn.remove_listener(self.CHANNEL_NAME, notify_handler)
                        except Exception:
                            import traceback
                            traceback.print_exc()
                            raise
                    raw_conn.close() # Return to pool/close wrapper
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"DB Listener connection lost: {e}. Reconnecting in 5s...")
                await asyncio.sleep(5.0)

    async def _processing_loop(self):
        """Main loop that processes events when notified or on timeout."""
        logger.info("EventConsumerWorker processing loop running.")
        while self._running:
            try:
                # 1. Process pending events
                has_more = await self._process_batch()
                
                # 2. If we processed events, there might be more (backlog). Don't wait.
                if has_more:
                    continue
                
                # 3. If no events, wait for notification OR timeout (heartbeat)
                try:
                    await asyncio.wait_for(self._notify_event.wait(), timeout=self.poll_interval)
                    # Clear event immediately to detect subsequent signals
                    self._notify_event.clear()
                except asyncio.TimeoutError:
                    # Timeout just means "check anyway" (heartbeat)
                    pass
                    
            except asyncio.CancelledError:
                logger.debug("EventConsumerWorker processing task cancelled.")
                break
            except Exception as e:
                logger.error(f"EventConsumerWorker crashed: {e}. Restarting loop in 5s...", exc_info=True)
                await asyncio.sleep(5.0)

    async def _process_batch(self) -> bool:
        """Fetch and process a batch. Returns True if a full batch was processed (implying more work)."""
        events = []
        async with managed_transaction(self.engine) as conn:
            events = await self.storage.consume(conn, limit=self.batch_size)
        
        if not events:
            return False

        for event in events:
            try:
                await self.handler(event)
            except Exception as e:
                logger.error(f"Error processing event {event.get('id')}: {e}", exc_info=True)
        
        # If we fetched a full batch, return True to indicate immediate retry
        return len(events) >= self.batch_size

class EventManager:
    """
    Manages event registration, emission, and persistence.
    Supports pluggable background execution strategies and persistent storage.
    """
    def __init__(self):
        self._sync_listeners: Dict[str, List[Listener]] = defaultdict(list)
        self._async_listeners: Dict[str, List[Listener]] = defaultdict(list)
        self._runner: BackgroundRunner = DefaultRunner()
        self.storage: Optional[GlobalSystemEventStore] = GlobalSystemEventStore()

    def set_runner(self, runner: BackgroundRunner) -> None:
        self._runner = runner
        logger.info(f"EventManager configured with runner: {type(runner).__name__}")

    def set_storage(self, storage: EventStorageSPI) -> None:
        self.storage = storage
        logger.info(f"EventManager configured with storage: {type(storage).__name__}")

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
            logger.info(f"Registered sync event listener {func.__module__}.{func.__name__} for {e_val}")
            return func
        return decorator
    
    def async_event_listener(self, event_type: Union[EventType, str]):
        """Decorator to register an asynchronous (background) event listener."""
        e_val = event_type.value if isinstance(event_type, EventType) else event_type
        def decorator(func: Listener) -> Listener:
            self._async_listeners[e_val].append(func)
            logger.info(f"Registered async event listener {func.__module__}.{func.__name__} for {e_val}")
            return func
        return decorator

    async def emit(self, event_type: Union[EventType, str], *args, db_resource: Optional[DbResource] = None, raise_on_error: bool = False, **kwargs):
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
            catalog_id = kwargs.get('catalog_id')
            payload = {'args': args, 'kwargs': kwargs}

            if is_global_event and self.storage:
                await self.storage.store(db_resource, catalog_id, e_val, payload)
            elif not is_global_event and catalog_id:
                # For tenant-specific events, we need the physical schema
                from dynastore.models.protocols import CatalogsProtocol
                from dynastore.tools.discovery import get_protocol
                
                catalogs = get_protocol(CatalogsProtocol)
                phys_schema = await catalogs.resolve_physical_schema(catalog_id, db_resource=db_resource) if catalogs else None
                
                if phys_schema:
                    try:
                        await _tenant_event_insert_query.execute(
                            db_resource,
                            schema=phys_schema,
                            catalog_id=catalog_id,
                            collection_id=kwargs.get('collection_id'),
                            asset_code=kwargs.get('asset_code'),
                            event_type=e_val,
                            payload=json.dumps(payload, cls=CustomJSONEncoder)
                        )
                    except QueryExecutionError as e:
                        logger.error(f"Failed to store tenant event {e_val} for catalog {catalog_id} in schema {phys_schema}: {e}", exc_info=True)
                        original = getattr(e, 'original_exception', None)
                        # Check for 'check_violation' (23514) which occurs when no partition exists
                        if hasattr(original, 'pgcode') and original.pgcode == '23514':
                            logger.warning(f"Tenant event storage skipped for {catalog_id}: partition missing.")
                            return
                        raise
                else:
                    logger.warning(f"Cannot persist tenant event {e_val} for catalog {catalog_id}: physical schema not resolved.")
            else:
                pass # Global event but no storage, or tenant event but no catalog code

        # 2. Sync Listeners (In-Transaction, Blocking)
        logger.debug(f"Emitting sync event {e_val}")
        for listener in self._sync_listeners.get(e_val, []):
            try:
                await listener(*args, **kwargs)
            except Exception as e:
                logger.error(f"Error in sync event listener for '{e_val}': {e}", exc_info=True)
                if raise_on_error:
                    raise e
        
        # 3. Async Listeners (Background, Non-Blocking)
        async_listeners = self._async_listeners.get(e_val, [])
        if async_listeners:
            logger.debug(f"Scheduling {len(async_listeners)} async listeners for {e_val}")
            for listener in async_listeners:
                try:
                    coro = listener(*args, **kwargs)
                    self._runner.run(coro)
                except Exception as e:
                    logger.error(f"Error scheduling async listener for '{e_val}': {e}", exc_info=True)

    def emit_side_effect(self, event_type: Union[EventType, str], *args, **kwargs):
        self.emit_detached(event_type, *args, **kwargs)

    def emit_detached(self, event_type: Union[EventType, str], *args, **kwargs):
        e_val = event_type.value if isinstance(event_type, EventType) else event_type
        listeners = self._async_listeners.get(e_val, [])
        if not listeners:
            return

        logger.debug(f"Scheduling detached event {e_val} for {len(listeners)} async listeners")
        
        for listener in listeners:
            try:
                coro = listener(*args, **kwargs)
                self._runner.run(coro)
            except Exception as e:
                logger.error(f"Error scheduling detached listener for '{e_val}': {e}", exc_info=True)
    
    def create_consumer(
        self, 
        engine: DbResource, 
        handler: Callable[[Dict[str, Any]], Awaitable[None]], 
        batch_size: int = 100,
        poll_interval: float = 10.0
    ) -> EventConsumerWorker:
        if not self.storage:
             raise RuntimeError("Cannot create consumer: No EventStorageSPI configured.")
             
        return EventConsumerWorker(engine, self.storage, handler, batch_size, poll_interval)

    @asynccontextmanager
    async def transaction(self, detached: bool = False, db_resource: Optional[DbResource] = None):
        buffer: List[Tuple[str, tuple, dict]] = []
        
        async def emitter(event_type: Union[EventType, str], *args, **kwargs):
            e_val = event_type.value if isinstance(event_type, EventType) else event_type
            if db_resource and 'db_resource' not in kwargs:
                kwargs['db_resource'] = db_resource
            
            # Store immediately if persistent
            await self.emit(event_type, *args, **kwargs)
            
            # Buffer for potential future use (though emit handles async dispatch already)
            # If 'detached' logic was intended to defer ALL processing, we'd need to block emit here.
            # But standard 'emit' separates sync/async. 
            # This transaction manager seems to be for grouping logic primarily.
            buffer.append((e_val, args, kwargs))
        
        yield emitter

event_manager = EventManager()

def register_event_listener(event_type: Union[EventType, str], listener: Listener):
    event_manager.register(event_type, listener)

sync_event_listener = event_manager.sync_event_listener
async_event_listener = event_manager.async_event_listener

async def emit_event(event_type: Union[EventType, str], *args, db_resource: Optional[DbResource] = None, **kwargs):
    await event_manager.emit(event_type, *args, db_resource=db_resource, **kwargs)

async def process_queued_event(event: Dict[str, Any]):
    """
    Handler for the background worker. 
    Deserializes events from the Outbox and processes them.
    """
    event_type_str = event.get('event_type')
    payload = event.get('payload', {})
    args = payload.get('args', [])
    kwargs = payload.get('kwargs', {})

    logger.debug(f"Worker processing event: {event_type_str} (ID: {event.get('id')})")
    event_manager.emit_detached(event_type_str, *args, **kwargs)