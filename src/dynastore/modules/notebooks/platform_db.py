"""DDL and CRUD operations for notebooks.platform_notebooks (global, cross-tenant).

Read operations use @cached for performance across multi-instance deployments.
The database is the single source of truth; cache is invalidated on writes.
"""
import json
import logging
from typing import Dict, List, Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from dynastore.modules.db_config.query_executor import DDLQuery, DQLQuery, ResultHandler
from dynastore.modules.db_config.exceptions import ResourceNotFoundError
from dynastore.tools.cache import cached
from .models import PlatformNotebookCreate, OwnerType

logger = logging.getLogger(__name__)

PLATFORM_NOTEBOOKS_DDL = """
CREATE TABLE IF NOT EXISTS notebooks.platform_notebooks (
    notebook_id    VARCHAR NOT NULL PRIMARY KEY,
    title          TEXT NOT NULL,
    description    TEXT,
    content        JSONB NOT NULL,
    metadata       JSONB DEFAULT '{}'::jsonb,
    registered_by  VARCHAR NOT NULL,
    owner_type     VARCHAR NOT NULL CHECK (owner_type IN ('module', 'sysadmin')),
    created_at     TIMESTAMPTZ DEFAULT NOW(),
    updated_at     TIMESTAMPTZ DEFAULT NOW(),
    deleted_at     TIMESTAMPTZ DEFAULT NULL
);
"""


async def seed_platform_notebook(conn: AsyncConnection, notebook: PlatformNotebookCreate) -> None:
    """Insert a platform notebook only if it does not already exist."""
    query = text("""
        INSERT INTO notebooks.platform_notebooks
            (notebook_id, title, description, content, metadata, registered_by, owner_type)
        VALUES
            (:notebook_id, :title, :description, :content, :metadata, :registered_by, :owner_type)
        ON CONFLICT (notebook_id) DO NOTHING
    """)
    params = {
        "notebook_id": notebook.notebook_id,
        "title": notebook.title,
        "description": notebook.description,
        "content": json.dumps(notebook.content),
        "metadata": json.dumps(notebook.metadata),
        "registered_by": notebook.registered_by,
        "owner_type": notebook.owner_type.value,
    }
    await conn.execute(query, params)


async def seed_platform_notebooks(conn: AsyncConnection) -> int:
    """Seed all registered platform notebooks. Returns count of entries processed."""
    from .example_registry import get_registered_notebooks

    entries = get_registered_notebooks()
    for entry in entries:
        await seed_platform_notebook(conn, entry)
    if entries:
        logger.info("Seeded %d platform notebook(s) (ON CONFLICT DO NOTHING).", len(entries))
    return len(entries)


@cached(maxsize=128, ttl=300, jitter=30, namespace="platform_notebooks_list", ignore=["conn"])
async def list_platform_notebooks(conn: AsyncConnection) -> List[Dict[str, Any]]:
    """List active platform notebooks (metadata only, no content). Cached for 5 min."""
    query = text("""
        SELECT notebook_id, title, description, metadata, registered_by, owner_type,
               created_at, updated_at
        FROM notebooks.platform_notebooks
        WHERE deleted_at IS NULL
        ORDER BY title
    """)
    return await DQLQuery(query, result_handler=ResultHandler.ALL_DICTS).execute(conn)


@cached(maxsize=256, ttl=300, jitter=30, namespace="platform_notebooks_get", ignore=["conn"])
async def get_platform_notebook(conn: AsyncConnection, notebook_id: str) -> Dict[str, Any]:
    """Retrieve a platform notebook by ID. Cached for 5 min."""
    query = text("""
        SELECT notebook_id, title, description, content, metadata, registered_by,
               owner_type, created_at, updated_at, deleted_at
        FROM notebooks.platform_notebooks
        WHERE notebook_id = :notebook_id AND deleted_at IS NULL
    """)
    row = await DQLQuery(query, result_handler=ResultHandler.ONE_DICT).execute(
        conn, notebook_id=notebook_id
    )
    if not row:
        raise ResourceNotFoundError(f"Platform notebook '{notebook_id}' not found")
    return row


async def save_platform_notebook(
    conn: AsyncConnection, notebook: PlatformNotebookCreate
) -> Dict[str, Any]:
    """Upsert a platform notebook (sysadmin use). Invalidates cache."""
    query = text("""
        INSERT INTO notebooks.platform_notebooks
            (notebook_id, title, description, content, metadata, registered_by, owner_type, updated_at)
        VALUES
            (:notebook_id, :title, :description, :content, :metadata, :registered_by, :owner_type, NOW())
        ON CONFLICT (notebook_id) DO UPDATE SET
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            content = EXCLUDED.content,
            metadata = EXCLUDED.metadata,
            registered_by = EXCLUDED.registered_by,
            owner_type = EXCLUDED.owner_type,
            updated_at = NOW(),
            deleted_at = NULL
        RETURNING notebook_id, title, description, content, metadata, registered_by,
                  owner_type, created_at, updated_at, deleted_at
    """)
    params = {
        "notebook_id": notebook.notebook_id,
        "title": notebook.title,
        "description": notebook.description,
        "content": json.dumps(notebook.content),
        "metadata": json.dumps(notebook.metadata),
        "registered_by": notebook.registered_by,
        "owner_type": notebook.owner_type.value,
    }
    result = await DQLQuery(query, result_handler=ResultHandler.ONE_DICT).execute(conn, **params)
    # Invalidate caches after write
    list_platform_notebooks.cache_clear()
    get_platform_notebook.cache_invalidate(conn, notebook.notebook_id)
    return result


async def soft_delete_platform_notebook(conn: AsyncConnection, notebook_id: str) -> None:
    """Soft-delete a platform notebook. Invalidates cache."""
    query = text("""
        UPDATE notebooks.platform_notebooks
        SET deleted_at = NOW()
        WHERE notebook_id = :notebook_id AND deleted_at IS NULL
    """)
    result = await conn.execute(query, {"notebook_id": notebook_id})
    if result.rowcount == 0:
        raise ResourceNotFoundError(f"Platform notebook '{notebook_id}' not found")
    # Invalidate caches after delete
    list_platform_notebooks.cache_clear()
    get_platform_notebook.cache_invalidate(conn, notebook_id)
