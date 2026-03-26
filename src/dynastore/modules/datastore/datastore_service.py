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
from sqlalchemy import create_engine
from contextlib import asynccontextmanager
from dynastore.modules.db_config.db_config import DBConfig
from dynastore.modules.db_config.tools import (
    get_config,
    ensure_init_db,
    normalize_db_url,
)
from dynastore.modules.db_config.query_executor import managed_transaction
import os
from dynastore.modules import ModuleProtocol
from typing import Optional, Any

from dynastore.models.protocols import DatabaseProtocol

logger = logging.getLogger(__name__)
class DatastoreModule(ModuleProtocol, DatabaseProtocol):
    priority: int = 7
    app_state: object

    def __init__(self, app_state: object):
        self.app_state = app_state

    @property
    def engine(self) -> Any:
        """
        DatabaseProtocol implementation.
        Returns the first available database engine (sync or async).
        """
        engine = getattr(self.app_state, "sync_engine", None)
        if engine:
            return engine
        engine = getattr(self.app_state, "engine", None)
        if engine:
            return engine
        raise RuntimeError("No database engine available (sync or async).")

    @property
    def async_engine(self) -> Optional[Any]:
        """DatabaseProtocol implementation - not available in task context."""
        return None

    @property
    def sync_engine(self) -> Optional[Any]:
        """DatabaseProtocol implementation."""
        return getattr(self.app_state, "sync_engine", None)

    def get_any_engine(self) -> Optional[Any]:
        """DatabaseProtocol implementation."""
        from dynastore.tools.protocol_helpers import get_engine

        return get_engine()

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        """
        Manages the lifespan for the synchronous database engine.
        """
        logger.info("Synchronous database connection startup initiated...")

        try:
            db_config: DBConfig = get_config(app_state)
            app_state.sync_engine = None

            if hasattr(app_state, "sync_engine") and app_state.sync_engine is not None:
                logger.info("Synchronous database engine already exists in app state.")
                raise Exception("Synchronous database engine already initialized.")
            else:
                app_state.sync_engine = create_engine(
                    normalize_db_url(db_config.database_url, is_async=False),
                    pool_size=db_config.pool_min_size,
                    max_overflow=db_config.pool_max_size - db_config.pool_min_size,
                    pool_timeout=db_config.pool_command_timeout,
                )
                logger.info("SQLAlchemy SyncEngine established successfully.")

                # Run initialization scripts using the shared maintenance tool.
                # Even though the engine is synchronous, we are in an async lifespan context,
                # so we can await the tool. The tool will handle the sync engine correctly.
                # _current_file_dir = os.path.dirname(os.path.abspath(__file__))
                # init_sql_path: str = os.path.join(_current_file_dir, "db_init/init.sql")

                # managed_transaction works for sync engines too (yields a standard connection)
                # But here we are passing the engine directly to the tool via a transaction wrapper
                # to ensure we have a connection context for the lock.

                # Note: managed_transaction for a sync engine behaves synchronously,
                # but we need to wrap it to call the async tool?
                # Actually, managed_transaction is an @asynccontextmanager that yields a sync conn if engine is sync.
                await ensure_init_db(app_state.sync_engine)

                # Status-check only: migrations are admin-triggered, never auto-applied.
                from dynastore.modules.db_config.migration_runner import (
                    check_migration_status,
                    MigrationStatus,
                )
                status = await check_migration_status(app_state.sync_engine)
                if status == MigrationStatus.PENDING_MIGRATIONS:
                    logger.warning(
                        "DatastoreModule: PENDING MIGRATIONS detected. "
                        "Apply via admin API before full functionality is available."
                    )
                elif status == MigrationStatus.DRIFT_DETECTED:
                    logger.warning(
                        "DatastoreModule: DRIFT DETECTED — check admin API."
                    )
                elif status == MigrationStatus.UNCHECKED:
                    logger.warning(
                        "DatastoreModule: Database not yet initialized. "
                        "Run initial migration via admin API."
                    )

        except Exception as e:
            logger.error(
                f"Failed to initialize synchronous database pool: {e}", exc_info=True
            )
            raise

        yield

        # --- Application Shutdown ---
        logger.info("Synchronous database connection shutdown initiated...")

        if app_state.sync_engine:
            app_state.sync_engine.dispose()
            app_state.sync_engine = None
            logger.info("SQLAlchemy SyncEngine disposed.")

        logger.info("Synchronous database connection shutdown complete.")
