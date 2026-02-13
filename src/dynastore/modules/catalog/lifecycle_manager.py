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
Lifecycle Management Framework

Provides a centralized registry for modular resource management at Catalog and Collection levels.
Plugins can register initialization and destruction hooks to create/destroy resources (tables, etc.)
in a safe, granular manner that prevents locking issues.

Supports both:
- Synchronous transactional operations (fast, in-transaction)
- Asynchronous external component operations (slow, with config snapshots)
"""

import logging
import asyncio
import inspect
from typing import Callable, Awaitable, List, Dict, Any, Union
from dynastore.modules.db_config.query_executor import DbResource

logger = logging.getLogger(__name__)

# Type aliases for synchronous lifecycle hooks (transactional table operations)
# Note: These can now be async OR sync functions, but they run within the synchronous transaction context.
SyncCatalogInitializer = Union[Callable[[DbResource, str, str], Awaitable[None]], Callable[[DbResource, str, str], None]]
SyncCatalogDestroyer = Union[Callable[[DbResource, str, str], Awaitable[None]], Callable[[DbResource, str, str], None]]
SyncCollectionInitializer = Union[Callable[[DbResource, str, str, str], Awaitable[None]], Callable[[DbResource, str, str, str], None]]
SyncCollectionDestroyer = Union[Callable[[DbResource, str, str, str], Awaitable[None]], Callable[[DbResource, str, str, str], None]]

# Type aliases for async external component hooks (receive config snapshots, run in background)
AsyncCatalogInitializer = Callable[[str, str, Dict[str, Any]], Awaitable[None]]  # (schema, catalog_id, config_snapshot)
AsyncCatalogDestroyer = Callable[[str, str, Dict[str, Any]], Awaitable[None]]  # (schema, catalog_id, config_snapshot)
AsyncCollectionInitializer = Callable[[str, str, str, Dict[str, Any]], Awaitable[None]]  # (schema, catalog_id, collection_id, config_snapshot)
AsyncCollectionDestroyer = Callable[[str, str, str, Dict[str, Any]], Awaitable[None]]  # (schema, catalog_id, collection_id, config_snapshot)

# Type aliases for asset lifecycle hooks
SyncAssetInitializer = Union[Callable[[DbResource, str, str, str, str], Awaitable[None]], Callable[[DbResource, str, str, str, str], None]]
SyncAssetDestroyer = Union[Callable[[DbResource, str, str, str, str], Awaitable[None]], Callable[[DbResource, str, str, str, str], None]]
AsyncAssetInitializer = Callable[[str, str, str, str, Dict[str, Any]], Awaitable[None]]  # (schema, catalog_id, collection_id, asset_code, config_snapshot)
AsyncAssetDestroyer = Callable[[str, str, str, str, Dict[str, Any]], Awaitable[None]]    # (schema, catalog_id, collection_id, asset_code, config_snapshot)


class LifecycleRegistry:
    """
    Centralized registry for catalog and collection lifecycle hooks.
    
    Features:
    - Safe execution: Errors in one plugin don't block others
    - Granular control: Separate hooks for catalog vs collection
    - Extensible: Plugins register via decorators
    - Dual-mode: Sync transactional + Async external components
    """
    
    def __init__(self):
        # Synchronous transactional hooks (tables)
        self._sync_catalog_initializers: List[SyncCatalogInitializer] = []
        self._sync_catalog_destroyers: List[SyncCatalogDestroyer] = []
        self._sync_collection_initializers: List[SyncCollectionInitializer] = []
        self._sync_collection_destroyers: List[SyncCollectionDestroyer] = []
        
        logger.info(f"INSTANCE: LifecycleRegistry initialized. ID: {id(self)}")
        
        # Track background tasks to prevent loop errors in tests and allow clean shutdown
        self._active_tasks: List[asyncio.Task] = []
        
        # Async external component hooks (GCP buckets, etc.)
        self._async_catalog_initializers: List[AsyncCatalogInitializer] = []
        self._async_catalog_destroyers: List[AsyncCatalogDestroyer] = []
        self._async_collection_initializers: List[AsyncCollectionInitializer] = []
        self._async_collection_destroyers: List[AsyncCollectionDestroyer] = []
        
        # Asset lifecycle hooks
        self._sync_asset_initializers: List[SyncAssetInitializer] = []
        self._sync_asset_destroyers: List[SyncAssetDestroyer] = []
        self._async_asset_initializers: List[AsyncAssetInitializer] = []
        self._async_asset_destroyers: List[AsyncAssetDestroyer] = []
    
    # Synchronous transactional hook registration
    def sync_catalog_initializer(self, func: SyncCatalogInitializer) -> SyncCatalogInitializer:
        """Register a catalog initialization hook (transactional table creation)."""
        self._sync_catalog_initializers.append(func)
        logger.info(f"Registered sync catalog initializer: {func.__module__}.{func.__name__}")
        return func
    
    def sync_catalog_destroyer(self, func: SyncCatalogDestroyer) -> SyncCatalogDestroyer:
        """Register a catalog destruction hook (transactional table cleanup)."""
        self._sync_catalog_destroyers.append(func)
        logger.info(f"Registered sync catalog destroyer: {func.__module__}.{func.__name__}")
        return func
    
    def sync_collection_initializer(self, func: SyncCollectionInitializer) -> SyncCollectionInitializer:
        """Register a collection initialization hook (transactional table creation)."""
        if func not in self._sync_collection_initializers:
            self._sync_collection_initializers.append(func)
            logger.info(f"Registered sync collection initializer: {func.__module__}.{func.__name__}")
        return func
    
    def sync_collection_destroyer(self, func: SyncCollectionDestroyer) -> SyncCollectionDestroyer:
        """Register a collection destruction hook (transactional table cleanup)."""
        self._sync_collection_destroyers.append(func)
        logger.info(f"Registered sync collection destroyer: {func.__module__}.{func.__name__}")
        return func
    
    # Async external component hook registration
    def async_catalog_initializer(self, func: AsyncCatalogInitializer) -> AsyncCatalogInitializer:
        """Register async catalog external component initialization (e.g., GCP bucket creation)."""
        self._async_catalog_initializers.append(func)
        logger.info(f"Registered async catalog initializer: {func.__module__}.{func.__name__}")
        return func
    
    def async_catalog_destroyer(self, func: AsyncCatalogDestroyer) -> AsyncCatalogDestroyer:
        """Register async catalog external component destruction (e.g., GCP bucket deletion)."""
        self._async_catalog_destroyers.append(func)
        logger.info(f"Registered async catalog destroyer: {func.__module__}.{func.__name__}")
        return func
    
    def async_collection_initializer(self, func: AsyncCollectionInitializer) -> AsyncCollectionInitializer:
        """Register async collection external component initialization."""
        self._async_collection_initializers.append(func)
        logger.info(f"Registered async collection initializer: {func.__module__}.{func.__name__}")
        return func
    
    def async_collection_destroyer(self, func: AsyncCollectionDestroyer) -> AsyncCollectionDestroyer:
        """Register async collection external component destruction."""
        self._async_collection_destroyers.append(func)
        logger.info(f"Registered async collection destroyer: {func.__module__}.{func.__name__}")
        return func
    
    # Asset lifecycle hook registration
    def sync_asset_initializer(self, func: SyncAssetInitializer) -> SyncAssetInitializer:
        """Register an asset initialization hook (transactional)."""
        self._sync_asset_initializers.append(func)
        logger.info(f"Registered sync asset initializer: {func.__module__}.{func.__name__}")
        return func
    
    def sync_asset_destroyer(self, func: SyncAssetDestroyer) -> SyncAssetDestroyer:
        """Register an asset destruction hook (transactional)."""
        self._sync_asset_destroyers.append(func)
        logger.info(f"Registered sync asset destroyer: {func.__module__}.{func.__name__}")
        return func
    
    def async_asset_initializer(self, func: AsyncAssetInitializer) -> AsyncAssetInitializer:
        """Register async asset external component initialization."""
        self._async_asset_initializers.append(func)
        logger.info(f"Registered async asset initializer: {func.__module__}.{func.__name__}")
        return func
    
    def async_asset_destroyer(self, func: AsyncAssetDestroyer) -> AsyncAssetDestroyer:
        """Register async asset external component destruction."""
        self._async_asset_destroyers.append(func)
        logger.info(f"Registered async asset destroyer: {func.__module__}.{func.__name__}")
        return func
    
    # Synchronous transactional execution
    async def init_catalog(self, conn: DbResource, schema: str, catalog_id: str) -> None:
        """Execute all registered sync catalog initializers (transactional)."""
        logger.info(f"Initializing catalog resources for '{catalog_id}' (schema: {schema})")
        
        for initializer in self._sync_catalog_initializers:
            try:
                if inspect.iscoroutinefunction(initializer):
                    await initializer(conn, schema, catalog_id)
                else:
                    initializer(conn, schema, catalog_id)
            except Exception as e:
                logger.error(
                    f"Sync catalog initializer {initializer.__module__}.{initializer.__name__} "
                    f"failed for '{catalog_id}': {e}",
                    exc_info=True
                )
    
    async def destroy_catalog(self, conn: DbResource, schema: str, catalog_id: str) -> None:
        """Execute all registered sync catalog destroyers (transactional)."""
        logger.info(f"Destroying catalog resources for '{catalog_id}' (schema: {schema})")
        
        for destroyer in self._sync_catalog_destroyers:
            try:
                if inspect.iscoroutinefunction(destroyer):
                    await destroyer(conn, schema, catalog_id)
                else:
                    destroyer(conn, schema, catalog_id)
            except Exception as e:
                logger.error(
                    f"Sync catalog destroyer {destroyer.__module__}.{destroyer.__name__} "
                    f"failed for '{catalog_id}': {e}",
                    exc_info=True
                )
    
    async def init_collection(
        self, 
        conn: DbResource, 
        schema: str, 
        catalog_id: str, 
        collection_id: str,
        **kwargs
    ) -> None:
        """Execute all registered sync collection initializers (transactional)."""
        logger.info(
            f"Initializing collection resources for '{catalog_id}:{collection_id}' "
            f"(schema: {schema})"
        )
        
        for initializer in self._sync_collection_initializers:
            try:
                if inspect.iscoroutinefunction(initializer):
                    await initializer(conn, schema, catalog_id, collection_id, **kwargs)
                else:
                    initializer(conn, schema, catalog_id, collection_id, **kwargs)
            except Exception as e:
                logger.error(
                    f"Sync collection initializer {initializer.__module__}.{initializer.__name__} "
                    f"failed for '{catalog_id}:{collection_id}': {e}",
                    exc_info=True
                )
    
    async def destroy_collection(
        self, 
        conn: DbResource, 
        schema: str, 
        catalog_id: str, 
        collection_id: str
    ) -> None:
        """Execute all registered sync collection destroyers (transactional)."""
        logger.info(
            f"Destroying collection resources for '{catalog_id}:{collection_id}' "
            f"(schema: {schema})"
        )
        
        for destroyer in self._sync_collection_destroyers:
            try:
                if inspect.iscoroutinefunction(destroyer):
                    await destroyer(conn, schema, catalog_id, collection_id)
                else:
                    destroyer(conn, schema, catalog_id, collection_id)
            except Exception as e:
                logger.error(
                    f"Sync collection destroyer {destroyer.__module__}.{destroyer.__name__} "
                    f"failed for '{catalog_id}:{collection_id}': {e}",
                    exc_info=True
                )
    
    # Async external component execution (with config snapshots)
    def init_async_catalog(
        self,
        schema: str,
        catalog_id: str,
        config_snapshot: Dict[str, Any]
    ) -> None:
        """
        Execute async external component initializers for catalog (e.g., GCP bucket creation).
        Runs in background, receives config snapshot.
        """
        if not self._async_catalog_initializers:
            return
            
        logger.info(f"Scheduling async catalog initialization for '{catalog_id}'")
        
        from dynastore.modules.concurrency import run_in_background
        
        async def _run_all():
            for initializer in self._async_catalog_initializers:
                try:
                    await initializer(schema, catalog_id, config_snapshot)
                except Exception as e:
                    logger.error(
                        f"Async catalog initializer {initializer.__module__}.{initializer.__name__} "
                        f"failed for '{catalog_id}': {e}",
                        exc_info=True
                    )
        
        task = run_in_background(_run_all(), name=f"init_catalog_async_{catalog_id}")
        self._active_tasks.append(task)
        task.add_done_callback(self._active_tasks.remove)
        return task
    
    def destroy_async_catalog(
        self,
        schema: str,
        catalog_id: str,
        config_snapshot: Dict[str, Any]
    ) -> None:
        """
        Execute async external component destroyers for catalog (e.g., GCP bucket deletion).
        Runs in background with config snapshot BEFORE schema drop.
        """
        if not self._async_catalog_destroyers:
            return
            
        logger.info(f"Scheduling async catalog destruction for '{catalog_id}'")
        
        from dynastore.modules.concurrency import run_in_background
        
        async def _run_all():
            for destroyer in self._async_catalog_destroyers:
                try:
                    await destroyer(schema, catalog_id, config_snapshot)
                except Exception as e:
                    logger.error(
                        f"Async catalog destroyer {destroyer.__module__}.{destroyer.__name__} "
                        f"failed for '{catalog_id}': {e}",
                        exc_info=True
                    )
        
        task = run_in_background(_run_all(), name=f"destroy_catalog_async_{catalog_id}")
        self._active_tasks.append(task)
        task.add_done_callback(self._active_tasks.remove)
        return task
    
    def init_async_collection(
        self,
        schema: str,
        catalog_id: str,
        collection_id: str,
        config_snapshot: Dict[str, Any]
    ) -> None:
        """Execute async external component initializers for collection."""
        if not self._async_collection_initializers:
            return
            
        logger.info(
            f"Scheduling async collection initialization for '{catalog_id}:{collection_id}'"
        )
        
        from dynastore.modules.concurrency import run_in_background
        
        async def _run_all():
            for initializer in self._async_collection_initializers:
                try:
                    await initializer(schema, catalog_id, collection_id, config_snapshot)
                except Exception as e:
                    logger.error(
                        f"Async collection initializer {initializer.__module__}.{initializer.__name__} "
                        f"failed for '{catalog_id}:{collection_id}': {e}",
                        exc_info=True
                    )
        
        task = run_in_background(_run_all(), name=f"init_collection_async_{catalog_id}_{collection_id}")
        self._active_tasks.append(task)
        task.add_done_callback(self._active_tasks.remove)
        return task
    
    def destroy_async_collection(
        self,
        schema: str,
        catalog_id: str,
        collection_id: str,
        config_snapshot: Dict[str, Any]
    ) -> None:
        """Execute async external component destroyers for collection."""
        if not self._async_collection_destroyers:
            return
            
        logger.info(
            f"Scheduling async collection destruction for '{catalog_id}:{collection_id}'"
        )
        
        from dynastore.modules.concurrency import run_in_background
        
        async def _run_all():
            for destroyer in self._async_collection_destroyers:
                try:
                    await destroyer(schema, catalog_id, collection_id, config_snapshot)
                except Exception as e:
                    logger.error(
                        f"Async collection destroyer {destroyer.__module__}.{destroyer.__name__} "
                        f"failed for '{catalog_id}:{collection_id}': {e}",
                        exc_info=True
                    )
        
        task = run_in_background(_run_all(), name=f"destroy_collection_async_{catalog_id}_{collection_id}")
        self._active_tasks.append(task)
        task.add_done_callback(self._active_tasks.remove)
        return task
    
    # Asset lifecycle execution
    async def init_asset(
        self,
        conn: DbResource,
        schema: str,
        catalog_id: str,
        collection_id: str,
        asset_code: str
    ) -> None:
        """Execute all registered sync asset initializers (transactional)."""
        logger.info(
            f"[LIFECYCLE] Initializing asset '{asset_code}' for '{catalog_id}:{collection_id}' "
            f"(schema: {schema})"
        )
        
        for initializer in self._sync_asset_initializers:
            try:
                if inspect.iscoroutinefunction(initializer):
                    await initializer(conn, schema, catalog_id, collection_id, asset_code)
                else:
                    initializer(conn, schema, catalog_id, collection_id, asset_code)
            except Exception as e:
                logger.error(
                    f"Sync asset initializer {initializer.__module__}.{initializer.__name__} "
                    f"failed for '{catalog_id}:{collection_id}:{asset_code}': {e}",
                    exc_info=True
                )
    
    async def destroy_asset(
        self,
        conn: DbResource,
        schema: str,
        catalog_id: str,
        collection_id: str,
        asset_code: str
    ) -> None:
        """Execute all registered sync asset destroyers (transactional)."""
        logger.info(
            f"[LIFECYCLE] Destroying asset '{asset_code}' for '{catalog_id}:{collection_id}' "
            f"(schema: {schema})"
        )
        
        for destroyer in self._sync_asset_destroyers:
            try:
                if inspect.iscoroutinefunction(destroyer):
                    await destroyer(conn, schema, catalog_id, collection_id, asset_code)
                else:
                    destroyer(conn, schema, catalog_id, collection_id, asset_code)
            except Exception as e:
                logger.error(
                    f"Sync asset destroyer {destroyer.__module__}.{destroyer.__name__} "
                    f"failed for '{catalog_id}:{collection_id}:{asset_code}': {e}",
                    exc_info=True
                )
    
    def init_async_asset(
        self,
        schema: str,
        catalog_id: str,
        collection_id: str,
        asset_code: str,
        config_snapshot: Dict[str, Any]
    ) -> None:
        """Execute async external component initializers for asset."""
        if not self._async_asset_initializers:
            return
            
        logger.info(
            f"[LIFECYCLE] Scheduling async asset initialization for "
            f"'{catalog_id}:{collection_id}:{asset_code}'"
        )
        
        from dynastore.modules.concurrency import run_in_background
        
        async def _run_all():
            for initializer in self._async_asset_initializers:
                try:
                    await initializer(schema, catalog_id, collection_id, asset_code, config_snapshot)
                except Exception as e:
                    logger.error(
                        f"Async asset initializer {initializer.__module__}.{initializer.__name__} "
                        f"failed for '{catalog_id}:{collection_id}:{asset_code}': {e}",
                        exc_info=True
                    )
        
        task = run_in_background(_run_all(), name=f"init_asset_async_{catalog_id}_{collection_id}_{asset_code}")
        self._active_tasks.append(task)
        task.add_done_callback(self._active_tasks.remove)
        return task
    
    def destroy_async_asset(
        self,
        schema: str,
        catalog_id: str,
        collection_id: str,
        asset_code: str,
        config_snapshot: Dict[str, Any]
    ) -> None:
        """Execute async external component destroyers for asset."""
        if not self._async_asset_destroyers:
            return
            
        logger.info(
            f"[LIFECYCLE] Scheduling async asset destruction for "
            f"'{catalog_id}:{collection_id}:{asset_code}'"
        )
        
        from dynastore.modules.concurrency import run_in_background
        
        async def _run_all():
            for destroyer in self._async_asset_destroyers:
                try:
                    await destroyer(schema, catalog_id, collection_id, asset_code, config_snapshot)
                except Exception as e:
                    logger.error(
                        f"Async asset destroyer {destroyer.__module__}.{destroyer.__name__} "
                        f"failed for '{catalog_id}:{collection_id}:{asset_code}': {e}",
                        exc_info=True
                    )
        
        task = run_in_background(_run_all(), name=f"destroy_asset_async_{catalog_id}_{collection_id}_{asset_code}")
        self._active_tasks.append(task)
        task.add_done_callback(self._active_tasks.remove)
        return task

    async def wait_for_all_tasks(self, timeout: float = 30.0) -> None:
        """
        Wait for all active background tasks to complete (useful for tests).
        Filters tasks to only those on the current running event loop.
        """
        if not self._active_tasks:
            return
            
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No loop running, can't wait for tasks
            return
            
        # Filter tasks that belong to the current loop and are not done
        current_tasks = [
            t for t in self._active_tasks 
            if not t.done() and t.get_loop() == loop
        ]
        
        if not current_tasks:
            # Remove any tasks from other loops that are already done
            self._active_tasks = [t for t in self._active_tasks if not t.done()]
            return
            
        logger.info(f"Waiting for {len(current_tasks)} background lifecycle tasks to complete on the current loop...")
        try:
            # We wait only for tasks on the CURRENT loop
            await asyncio.wait(current_tasks, timeout=timeout)
        except Exception as e:
            logger.warning(f"Error while waiting for background tasks: {e}")
            
        # Clean up the list to remove finished tasks from any loop
        self._active_tasks = [t for t in self._active_tasks if not t.done()]


# Global instance
lifecycle_registry = LifecycleRegistry()

# Convenience decorator exports (using registry methods as decorators)
sync_catalog_initializer = lifecycle_registry.sync_catalog_initializer
sync_catalog_destroyer = lifecycle_registry.sync_catalog_destroyer
sync_collection_initializer = lifecycle_registry.sync_collection_initializer
sync_collection_destroyer = lifecycle_registry.sync_collection_destroyer

async_catalog_initializer = lifecycle_registry.async_catalog_initializer
async_catalog_destroyer = lifecycle_registry.async_catalog_destroyer
async_collection_initializer = lifecycle_registry.async_collection_initializer
async_collection_destroyer = lifecycle_registry.async_collection_destroyer

sync_asset_initializer = lifecycle_registry.sync_asset_initializer
sync_asset_destroyer = lifecycle_registry.sync_asset_destroyer
async_asset_initializer = lifecycle_registry.async_asset_initializer
async_asset_destroyer = lifecycle_registry.async_asset_destroyer
