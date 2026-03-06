from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy import text
from typing import Optional, Dict, List, Any
from dynastore.modules.db_config.query_executor import DDLQuery, DQLQuery, ResultHandler
import json
from datetime import datetime
from dynastore.modules.db_config.exceptions import ResourceNotFoundError

from .models import NotebookCreate, Notebook

# DDL for the notebooks table within a tenant schema
# This follows the cellular architecture pattern
NOTEBOOKS_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.notebooks (
    notebook_id VARCHAR NOT NULL,
    catalog_id VARCHAR NOT NULL,
    title TEXT,
    description TEXT,
    content JSONB NOT NULL,
    metadata JSONB DEFAULT '{{}}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (notebook_id)
);
"""

async def init_notebooks_storage(conn: AsyncConnection, schema: str, catalog_id: str):
    """
    Initialize the notebooks storage for a specific tenant schema.
    This is called by the tenant initialization hook.
    """
    ddl = NOTEBOOKS_DDL.format(schema=schema)
    await DDLQuery(ddl).execute(conn)

async def get_notebook(conn: AsyncConnection, schema: str, notebook_id: str) -> Dict[str, Any]:
    """Retrieve a notebook by notebook_id from the specific schema"""
    query = text(f"""
        SELECT notebook_id, catalog_id, title, description, content, metadata, created_at, updated_at
        FROM {schema}.notebooks
        WHERE notebook_id = :notebook_id
    """)
    row = await DQLQuery(query, result_handler=ResultHandler.ONE_DICT).execute(conn, notebook_id=notebook_id)
    
    if not row:
        raise ResourceNotFoundError(f"Notebook '{notebook_id}' not found")
        
    return row

async def list_notebooks(conn: AsyncConnection, schema: str) -> List[Dict[str, Any]]:
    """List all notebooks in the schema (metadata only, no content)"""
    query = text(f"""
        SELECT notebook_id, catalog_id, title, description, metadata, created_at, updated_at
        FROM {schema}.notebooks
        ORDER BY updated_at DESC
    """)
    return await DQLQuery(query, result_handler=ResultHandler.ALL_DICTS).execute(conn)

async def save_notebook(conn: AsyncConnection, schema: str, catalog_id: str, notebook: NotebookCreate) -> Dict[str, Any]:
    """Save or update a notebook"""
    query = text(f"""
        INSERT INTO {schema}.notebooks (notebook_id, catalog_id, title, description, content, metadata, updated_at)
        VALUES (:notebook_id, :catalog_id, :title, :description, :content, :metadata, NOW())
        ON CONFLICT (notebook_id) DO UPDATE SET
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            content = EXCLUDED.content,
            metadata = EXCLUDED.metadata,
            updated_at = NOW()
        RETURNING notebook_id, catalog_id, title, description, content, metadata, created_at, updated_at
    """)
    
    params = {
        "notebook_id": notebook.notebook_id,
        "catalog_id": catalog_id,
        "title": notebook.title,
        "description": notebook.description,
        "content": json.dumps(notebook.content),
        "metadata": json.dumps(notebook.metadata)
    }
    
    return await DQLQuery(query, result_handler=ResultHandler.ONE_DICT).execute(conn, **params)
