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
import os
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import create_async_engine
from typing import Optional, Any
from sqlalchemy.ext.asyncio import AsyncEngine
from dynastore.modules import ModuleProtocol, dynastore_module
from dynastore.modules.db_config.db_config import DBConfig
from dynastore.modules.db_config.tools import get_config, ensure_init_db, normalize_db_url
from dynastore.modules.db_config.query_executor import managed_transaction
from dynastore.modules.db_config import maintenance_tools

from dynastore.models.protocols import DatabaseProtocol

logger = logging.getLogger(__name__)

@dynastore_module
class DBService(ModuleProtocol, DatabaseProtocol):
    app_state: object

    def __init__(self, app_state: object):
        self.app_state = app_state

    @property
    def priority(self) -> int:
        return 10  # Higher priority for async engine in API context

    @property
    def engine(self) -> Optional[AsyncEngine]:
        """DatabaseProtocol implementation."""
        return getattr(self.app_state, 'engine', None)

    @property
    def async_engine(self) -> Optional[AsyncEngine]:
        """DatabaseProtocol implementation."""
        return getattr(self.app_state, 'engine', None)

    @property
    def sync_engine(self) -> Optional[Any]:
        """DatabaseProtocol implementation."""
        return getattr(self.app_state, 'sync_engine', None)

    def get_any_engine(self) -> Optional[Any]:
        """DatabaseProtocol implementation."""
        from dynastore.tools.protocol_helpers import get_engine
        try:
            return get_engine()
        except RuntimeError:
            return None

    def get_engine(self) -> Optional[AsyncEngine]:
        # Legacy method for backward compatibility
        return self.engine

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        """
        Manages the lifespan of the async database engine.
        """
        logger.info("DBService: Async database connection startup initiated...")

        if not hasattr(app_state, 'db_config'):
            raise RuntimeError("db_config not found in app_state. Ensure 'db_config' module is loaded before 'db'.")
        
        db_config: DBConfig = get_config(app_state)
        
        # Check if engine is already injected (e.g. by tests)
        existing_engine = getattr(app_state, 'engine', None)
        engine_created_by_service = False

        if existing_engine:
             logger.info("DBService: Using existing engine from app_state.")
        else:
             app_state.engine = None

        try:
            if not existing_engine:
                logger.info(f"DBService: Using DB configuration: {db_config.database_url}")
                
                # 1. Create Engine
                app_state.engine = create_async_engine(
                    normalize_db_url(db_config.database_url, is_async=True),
                    pool_size=db_config.pool_min_size,
                    max_overflow=db_config.pool_max_size - db_config.pool_min_size,
                    pool_timeout=db_config.pool_command_timeout,
                    pool_pre_ping=True
                )
                engine_created_by_service = True
                logger.info("DBService: ASYNC Database connection pool established successfully.")
        
            # A. Ensure Critical Extensions (PostGIS)
            if app_state.engine:
                await ensure_init_db(app_state.engine)
                
                # B. Ensure Global Maintenance Tasks
                # This registers the daily job to clean up orphaned cron jobs.
                # We pass the engine directly so maintenance tools can manage their own 
                # transactions and retries/locks safely.
                await maintenance_tools.ensure_global_cron_cleanup(app_state.engine)

            # 2. Bootstrap Database (Extensions & Init Script)
            # async with managed_transaction(app_state.engine) as conn:
                # ...
            
            yield

        except Exception as e:
            logger.critical(f"DBService: FATAL: Failed to create database connection pool: {e}", exc_info=True)
            raise
        finally:
            logger.info("DBService: Database connection shutdown initiated...")
            # Only dispose if we created it
            if engine_created_by_service and hasattr(app_state, 'engine') and app_state.engine:
                await app_state.engine.dispose()
                app_state.engine = None
                logger.info("DBService: Database connection pool closed.")
            logger.info("DBService: Database connection shutdown completed.")