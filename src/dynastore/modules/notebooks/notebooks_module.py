from typing import List, Dict, Any, Optional
from types import SimpleNamespace
from dynastore.modules import ModuleProtocol, get_protocol
from dynastore.models.protocols import DatabaseProtocol, CatalogsProtocol
from dynastore.modules.db_config.query_executor import managed_transaction
from dynastore.modules.db_config.shared_queries import DbResource
from .models import NotebookCreate, Notebook
class NotebooksModule(ModuleProtocol):
    priority: int = 100
    def __init__(self, app_state: SimpleNamespace):
        self.app_state = app_state
        global _module_instance
        _module_instance = self

    async def get_notebook(self, catalog_id: str, notebook_id: str, db_resource: Optional[DbResource] = None) -> Notebook:
        """Get a specific notebook"""
        catalogs = get_protocol(CatalogsProtocol)
        db = get_protocol(DatabaseProtocol)
        engine = db.engine if db else None
        
        schema = await catalogs.resolve_physical_schema(catalog_id, db_resource=db_resource) if catalogs else None
        async with managed_transaction(db_resource or engine) as conn:
            from .notebooks_db import get_notebook as db_get_notebook
            data = await db_get_notebook(conn, schema, notebook_id)
        return Notebook(**data)

    async def list_notebooks(self, catalog_id: str) -> List[Dict[str, Any]]:
        """List notebooks for a catalog"""
        catalogs = get_protocol(CatalogsProtocol)
        db = get_protocol(DatabaseProtocol)
        engine = db.engine if db else None
        
        schema = await catalogs.resolve_physical_schema(catalog_id) if catalogs else None
        
        async with managed_transaction(engine) as conn:
            from .notebooks_db import list_notebooks as db_list_notebooks
            return await db_list_notebooks(conn, schema)

    async def delete_notebook(self, catalog_id: str, notebook_id: str) -> None:
        """Delete a notebook."""
        catalogs = get_protocol(CatalogsProtocol)
        db = get_protocol(DatabaseProtocol)
        engine = db.engine if db else None

        schema = await catalogs.resolve_physical_schema(catalog_id) if catalogs else None

        async with managed_transaction(engine) as conn:
            from .notebooks_db import delete_notebook as db_delete_notebook
            await db_delete_notebook(conn, schema, notebook_id)

    async def save_notebook(self, catalog_id: str, notebook: NotebookCreate) -> Notebook:
        """Save a notebook"""
        catalogs = get_protocol(CatalogsProtocol)
        db = get_protocol(DatabaseProtocol)
        engine = db.engine if db else None
        
        schema = await catalogs.resolve_physical_schema(catalog_id) if catalogs else None
    
        async with managed_transaction(engine) as conn:
            from .notebooks_db import save_notebook as db_save_notebook
            data = await db_save_notebook(conn, schema, catalog_id, notebook)
            return Notebook(**data)

# --- Public Helper Functions ---

_module_instance: Optional[NotebooksModule] = None

def _get_service() -> NotebooksModule:
    global _module_instance
    if _module_instance:
        return _module_instance
        
    instance = get_protocol(NotebooksModule)
    if not instance:
        raise RuntimeError("NotebooksModule is not initialized")
    _module_instance = instance
    return instance

async def get_notebook(catalog_id: str, notebook_id: str) -> Notebook:
    return await _get_service().get_notebook(catalog_id, notebook_id)

async def list_notebooks(catalog_id: str) -> List[Dict[str, Any]]:
    return await _get_service().list_notebooks(catalog_id)

async def delete_notebook(catalog_id: str, notebook_id: str) -> None:
    return await _get_service().delete_notebook(catalog_id, notebook_id)

async def save_notebook(catalog_id: str, notebook: NotebookCreate) -> Notebook:
    return await _get_service().save_notebook(catalog_id, notebook)
