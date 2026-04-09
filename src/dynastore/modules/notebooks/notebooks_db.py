# src/dynastore/modules/notebooks/notebooks_db.py
"""DDL and CRUD for {tenant_schema}.notebooks (per-catalog, tenant-isolated)."""
import json
import logging
from typing import Dict, List, Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from dynastore.modules.db_config.query_executor import DDLQuery, DQLQuery, ResultHandler
from dynastore.modules.db_config.exceptions import ResourceNotFoundError
from .models import NotebookCreate

logger = logging.getLogger(__name__)

NOTEBOOKS_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.notebooks (
    notebook_id VARCHAR NOT NULL,
    catalog_id VARCHAR NOT NULL,
    title TEXT,
    description TEXT,
    content JSONB NOT NULL,
    metadata JSONB DEFAULT '{{}}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    deleted_at TIMESTAMPTZ DEFAULT NULL,
    owner_id VARCHAR,
    copied_from VARCHAR DEFAULT NULL,
    PRIMARY KEY (notebook_id)
);
"""

NOTEBOOKS_MIGRATION_DDL = """
ALTER TABLE {schema}.notebooks ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ DEFAULT NULL;
ALTER TABLE {schema}.notebooks ADD COLUMN IF NOT EXISTS owner_id VARCHAR;
ALTER TABLE {schema}.notebooks ADD COLUMN IF NOT EXISTS copied_from VARCHAR DEFAULT NULL;
"""


async def init_notebooks_storage(conn: AsyncConnection, schema: str, catalog_id: str):
    """Initialize notebooks storage for a tenant schema.

    Runs CREATE TABLE (for new catalogs) and ALTER TABLE (for existing catalogs
    that were created before the soft-delete columns were added). Both are idempotent.
    """
    await DDLQuery(NOTEBOOKS_DDL).execute(conn, schema=schema)
    await DDLQuery(NOTEBOOKS_MIGRATION_DDL).execute(conn, schema=schema)


async def get_notebook(conn: AsyncConnection, schema: str, notebook_id: str) -> Dict[str, Any]:
    """Retrieve a notebook by notebook_id (active only)."""
    query = text(f"""
        SELECT notebook_id, catalog_id, title, description, content, metadata,
               created_at, updated_at, deleted_at, owner_id, copied_from
        FROM {schema}.notebooks
        WHERE notebook_id = :notebook_id AND deleted_at IS NULL
    """)
    row = await DQLQuery(query, result_handler=ResultHandler.ONE_DICT).execute(
        conn, notebook_id=notebook_id
    )
    if not row:
        raise ResourceNotFoundError(f"Notebook '{notebook_id}' not found")
    return row


async def list_notebooks(conn: AsyncConnection, schema: str) -> List[Dict[str, Any]]:
    """List active notebooks in the schema (metadata only, no content)."""
    query = text(f"""
        SELECT notebook_id, catalog_id, title, description, metadata,
               created_at, updated_at, owner_id, copied_from
        FROM {schema}.notebooks
        WHERE deleted_at IS NULL
        ORDER BY updated_at DESC
    """)
    return await DQLQuery(query, result_handler=ResultHandler.ALL_DICTS).execute(conn)


async def soft_delete_notebook(conn: AsyncConnection, schema: str, notebook_id: str) -> None:
    """Soft-delete a notebook."""
    query = text(f"""
        UPDATE {schema}.notebooks
        SET deleted_at = NOW()
        WHERE notebook_id = :notebook_id AND deleted_at IS NULL
    """)
    result = await conn.execute(query, {"notebook_id": notebook_id})
    if result.rowcount == 0:
        raise ResourceNotFoundError(f"Notebook '{notebook_id}' not found")


async def save_notebook(
    conn: AsyncConnection, schema: str, catalog_id: str, notebook: NotebookCreate,
    owner_id: str | None = None, copied_from: str | None = None,
) -> Dict[str, Any]:
    """Save or update a notebook."""
    query = text(f"""
        INSERT INTO {schema}.notebooks
            (notebook_id, catalog_id, title, description, content, metadata, owner_id, copied_from, updated_at)
        VALUES
            (:notebook_id, :catalog_id, :title, :description, :content, :metadata, :owner_id, :copied_from, NOW())
        ON CONFLICT (notebook_id) DO UPDATE SET
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            content = EXCLUDED.content,
            metadata = EXCLUDED.metadata,
            updated_at = NOW()
        RETURNING notebook_id, catalog_id, title, description, content, metadata,
                  created_at, updated_at, deleted_at, owner_id, copied_from
    """)
    params = {
        "notebook_id": notebook.notebook_id,
        "catalog_id": catalog_id,
        "title": notebook.title,
        "description": notebook.description,
        "content": json.dumps(notebook.content),
        "metadata": json.dumps(notebook.metadata),
        "owner_id": owner_id,
        "copied_from": copied_from,
    }
    return await DQLQuery(query, result_handler=ResultHandler.ONE_DICT).execute(conn, **params)


async def copy_from_platform(
    conn: AsyncConnection, schema: str, catalog_id: str,
    platform_notebook: Dict[str, Any], owner_id: str,
) -> Dict[str, Any]:
    """Copy a platform notebook into a tenant catalog. Does nothing if already exists."""
    query = text(f"""
        INSERT INTO {schema}.notebooks
            (notebook_id, catalog_id, title, description, content, metadata, owner_id, copied_from)
        VALUES
            (:notebook_id, :catalog_id, :title, :description, :content, :metadata, :owner_id, :copied_from)
        ON CONFLICT (notebook_id) DO NOTHING
        RETURNING notebook_id, catalog_id, title, description, content, metadata,
                  created_at, updated_at, deleted_at, owner_id, copied_from
    """)
    content = platform_notebook["content"]
    metadata = platform_notebook.get("metadata", {})
    params = {
        "notebook_id": platform_notebook["notebook_id"],
        "catalog_id": catalog_id,
        "title": platform_notebook["title"],
        "description": platform_notebook.get("description"),
        "content": json.dumps(content) if isinstance(content, dict) else content,
        "metadata": json.dumps(metadata) if isinstance(metadata, dict) else metadata,
        "owner_id": owner_id,
        "copied_from": platform_notebook["notebook_id"],
    }
    row = await DQLQuery(query, result_handler=ResultHandler.ONE_DICT).execute(conn, **params)
    if not row:
        # Already existed — return existing
        return await get_notebook(conn, schema, platform_notebook["notebook_id"])
    return row
