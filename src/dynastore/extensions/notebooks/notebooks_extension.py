from fastapi import APIRouter, FastAPI, Depends, HTTPException
from typing import List, Optional, Any, Dict
from dynastore.extensions.registry import get_extension_instance # Kept get_extension_instance if needed
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.web import expose_static
from dynastore.modules.notebooks import notebooks_module as notebook_service
from dynastore.modules.notebooks.models import NotebookCreate, Notebook
from dynastore.extensions.auth.dependencies import get_current_active_user
from dynastore.modules.apikey.models import Principal as User
class NotebooksExtension(ExtensionProtocol):
    priority: int = 100
    """
    Extension that exposes notebook management endpoints.
    
    This extension bridges the JupyterLite frontend with the 
    cellular database backend for notebook persistence.
    """
    router = APIRouter(prefix="/notebooks", tags=["Notebooks"])

    def __init__(self, app: Optional[FastAPI] = None):
        super().__init__()
        self.app = app
        self._register_routes()

    def _register_routes(self):
        self.router.add_api_route("/{catalog_id}", self.list_notebooks, methods=["GET"], response_model=List[Dict[str, Any]])
        self.router.add_api_route("/{catalog_id}/{notebook_code}", self.get_notebook, methods=["GET"], response_model=Notebook)
        self.router.add_api_route("/{catalog_id}/{notebook_code}", self.save_notebook, methods=["PUT"], response_model=Notebook)

    async def list_notebooks(
        self,
        catalog_id: str,
        current_user: User = Depends(get_current_active_user)
    ):
        """List all notebooks in a catalog"""
        # TODO: Add catalog access permission check here
        return await notebook_service.list_notebooks(catalog_id)

    async def get_notebook(
        self,
        catalog_id: str,
        notebook_code: str,
        current_user: User = Depends(get_current_active_user)
    ):
        """Get full notebook content"""
        # TODO: Add catalog access permission check here
        return await notebook_service.get_notebook(catalog_id, notebook_code)

    async def save_notebook(
        self,
        catalog_id: str,
        notebook_code: str,
        content: Dict[str, Any],
        current_user: User = Depends(get_current_active_user)
    ):
        """Save a notebook (compatible with JupyterLite save mechanism)"""
        # TODO: Add catalog access permission check here
        
        # Extract title from metadata or content if possible, otherwise default
        title = content.get("metadata", {}).get("title", notebook_code)
        
        notebook_model = NotebookCreate(
            notebook_id=notebook_code,
            title=title,
            content=content,
            metadata=content.get("metadata", {})
        )
        
        return await notebook_service.save_notebook(catalog_id, notebook_model)

    @expose_static("lite")
    def serve_lite_static(self) -> List[str]:
        """
        Serve JupyterLite static assets.
        This recursively lists all files in the static/lite directory.
        """
        import os
        static_dir = os.path.join(os.path.dirname(__file__), "static", "lite")
        files = []
        for root, _, filenames in os.walk(static_dir):
            for filename in filenames:
                files.append(os.path.join(root, filename))
        return files

    def get_router(self) -> APIRouter:
        return self.router
