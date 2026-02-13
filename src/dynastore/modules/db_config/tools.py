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
import secrets
import warnings
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.sql import text
from typing import Optional
from dynastore.modules.db_config.db_config import DBConfig
from dynastore.modules.db_config.query_executor import managed_transaction, DbResource, DbEngine, DbConnection, DDLQuery
from dynastore.modules.db_config import maintenance_tools

def normalize_db_url(url: str, is_async: bool = False) -> str:
    """
    Normalizes a database URL for use with sync (psycopg2) or async (asyncpg) drivers.
    
    It ensures the correct protocol prefix and converts driver-specific
    parameters (like ssl vs sslmode).
    """
    if not url:
        return url

    # 1. Handle Protocol
    if is_async:
        if url.startswith("postgresql://"):
            url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    else:
        if url.startswith("postgresql+asyncpg://"):
            url = url.replace("postgresql+asyncpg://", "postgresql://", 1)

    # 2. Handle SSL Parameters
    # asyncpg uses 'ssl'
    # psycopg2 uses 'sslmode'
    if is_async:
        # Convert sslmode=... to ssl=...
        if "sslmode=" in url:
            url = url.replace("sslmode=", "ssl=")
    else:
        # Convert ssl=... to sslmode=...
        if "ssl=" in url:
            url = url.replace("ssl=", "sslmode=")
            
    return url

async def ensure_init_db(resource: DbResource):
    """Initializes the database base extensions."""
    # --- Bootstrap Critical Extensions ---
    # We pass the resource (Engine) directly to maintenance tools.
    # This allows their internal 'acquire_lock_if_needed' to manage its own connections
    # and retries, ensuring that if one connection fails/closes, a fresh one can be acquired.
    
    await maintenance_tools.ensure_db_extension(resource, "postgis")
    await maintenance_tools.ensure_db_extension(resource, "postgis_topology")
    await maintenance_tools.ensure_db_extension(resource, "btree_gist")
    await maintenance_tools.ensure_db_extension(resource, "btree_gin")

def get_any_engine(app_state: object) -> Optional[DbEngine]:
    """
    DEPRECATED: Use get_protocol(DatabaseProtocol).engine instead.
    
    Abstracts engine retrieval from the application state.

    It intelligently finds and returns whichever database engine is available,
    prioritizing the asynchronous 'engine' but gracefully falling back to the
    synchronous 'sync_engine'.
    
    This function is deprecated and will be removed in a future release.
    Use get_protocol(DatabaseProtocol).engine for standardized access.
    """
    warnings.warn(
        "get_any_engine() is deprecated. Use get_protocol(DatabaseProtocol).engine "
        "or the helper function get_engine() from dynastore.tools.protocol_helpers instead.",
        DeprecationWarning,
        stacklevel=2
    )

    # 1. Try to resolve via Protocol (Dynamic Discovery)
    try:
        from dynastore.modules import get_protocol
        from dynastore.models.protocols import DatabaseProtocol
        db = get_protocol(DatabaseProtocol)
        if db and db.engine:
            return db.engine
    except (ImportError, RuntimeError):
        pass

    # 2. Fallback for very early initialization or test contexts
    # Prioritize the async engine, but fall back to the sync one.
    return getattr(app_state, 'engine', None) or getattr(app_state, 'sync_engine', None)

def get_config(app_state) -> DBConfig:
    """Returns the current database configuration."""
    return app_state.db_config


@asynccontextmanager
async def isolated_transaction(
    conn: DbConnection,
) -> AsyncGenerator[None, None]:
    """
    Creates an isolated sub-transaction using a SAVEPOINT.

    This is critical for running DDL statements that might fail harmlessly
    (e.g., CREATE TYPE ... IF NOT EXISTS). If such a statement fails,
    PostgreSQL would normally abort the entire parent transaction.

    This context manager wraps the operation in a SAVEPOINT. If any exception
    occurs within the `with` block, it rolls back to the savepoint, which
    clears the error state from the connection, allowing the parent transaction
    to proceed without being poisoned.

    Usage:
        try:
            async with isolated_transaction(conn):
                await conn.execute(text("CREATE TYPE ..."))
        except DuplicateObjectError:
            logger.debug("Type already exists, continuing.")

    """
    savepoint_name = f"sp_{secrets.token_hex(6)}"
    await DDLQuery(f"SAVEPOINT {savepoint_name}").execute(conn)
    rolled_back = False
    try:
        yield
    except Exception:
        rolled_back = True
        await DDLQuery(f"ROLLBACK TO SAVEPOINT {savepoint_name}").execute(conn)
        raise  # Re-raise the exception for the caller to handle
    if not rolled_back:
        await DDLQuery(f"RELEASE SAVEPOINT {savepoint_name}").execute(conn)
