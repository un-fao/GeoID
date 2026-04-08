from fastapi import APIRouter, FastAPI, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, Response
from typing import List, Optional, Any, Dict
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.web import expose_static, expose_web_page
from dynastore.modules.notebooks import notebooks_module as notebook_service
from dynastore.modules.notebooks.models import NotebookCreate, Notebook
from dynastore.extensions.auth.dependencies import get_current_active_user
from dynastore.modules.apikey.models import Principal as User

import json
import logging
import os

logger = logging.getLogger(__name__)


class NotebooksExtension(ExtensionProtocol):
    priority: int = 100
    """
    Extension that exposes notebook management endpoints and a web-based
    notebook browser.  Bridges the JupyterLite frontend with the cellular
    database backend for notebook persistence.

    Provides:
    - REST API for notebook CRUD (per tenant/catalog)
    - System-level example notebooks (bundled with the platform)
    - Web page for browsing, viewing and managing notebooks
    - JupyterLite static assets for in-browser execution
    """
    router = APIRouter(prefix="/notebooks", tags=["Notebooks"])

    def __init__(self, app: Optional[FastAPI] = None):
        super().__init__()
        self.app = app
        self._static_dir = os.path.join(os.path.dirname(__file__), "static")
        self._examples_dir = os.path.join(os.path.dirname(__file__), "examples")
        self._register_routes()

    def _register_routes(self):
        # System-level example notebooks (read-only, bundled)
        self.router.add_api_route(
            "/examples",
            self.list_example_notebooks,
            methods=["GET"],
            response_model=List[Dict[str, Any]],
            summary="List bundled example notebooks",
        )
        self.router.add_api_route(
            "/examples/{notebook_code}",
            self.get_example_notebook,
            methods=["GET"],
            summary="Get a bundled example notebook",
        )
        # Tenant CRUD
        self.router.add_api_route(
            "/{catalog_id}",
            self.list_notebooks,
            methods=["GET"],
            response_model=List[Dict[str, Any]],
            summary="List notebooks in a catalog",
        )
        self.router.add_api_route(
            "/{catalog_id}/{notebook_code}",
            self.get_notebook,
            methods=["GET"],
            response_model=Notebook,
            summary="Get full notebook content",
        )
        self.router.add_api_route(
            "/{catalog_id}/{notebook_code}",
            self.save_notebook,
            methods=["PUT"],
            response_model=Notebook,
            summary="Save a notebook",
        )
        self.router.add_api_route(
            "/{catalog_id}/{notebook_code}",
            self.delete_notebook,
            methods=["DELETE"],
            summary="Delete a notebook",
        )

    # ------------------------------------------------------------------
    # Web page: Notebook browser
    # ------------------------------------------------------------------

    @expose_web_page(
        page_id="notebooks",
        title="Notebooks",
        icon="fa-book-open",
        description="Interactive notebooks for learning and documenting platform workflows.",
        priority=40,
    )
    async def provide_notebooks_page(self, request: Request = None, language: str = "en"):
        """Serve the notebooks browser HTML page."""
        html_path = os.path.join(self._static_dir, "notebooks.html")
        if not os.path.exists(html_path):
            return Response(content="Notebooks page template not found", status_code=404)
        with open(html_path, "r", encoding="utf-8") as f:
            return Response(content=f.read(), media_type="text/html")

    # ------------------------------------------------------------------
    # System-level example notebooks (read-only, shipped with platform)
    # ------------------------------------------------------------------

    async def list_example_notebooks(self) -> List[Dict[str, Any]]:
        """List bundled example notebooks (no auth required)."""
        examples = []
        if not os.path.isdir(self._examples_dir):
            return examples
        for fname in sorted(os.listdir(self._examples_dir)):
            if not fname.endswith(".ipynb"):
                continue
            fpath = os.path.join(self._examples_dir, fname)
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    nb = json.load(f)
                notebook_id = fname.replace(".ipynb", "")
                title = nb.get("metadata", {}).get("title", notebook_id)
                cell_count = len(nb.get("cells", []))
                examples.append({
                    "notebook_id": notebook_id,
                    "title": title,
                    "description": f"Example notebook ({cell_count} cells)",
                    "source": "bundled",
                })
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning("Skipping malformed example %s: %s", fname, exc)
        return examples

    async def get_example_notebook(self, notebook_code: str):
        """Return a bundled example notebook by code (filename without .ipynb)."""
        fpath = os.path.join(self._examples_dir, f"{notebook_code}.ipynb")
        if not os.path.isfile(fpath):
            raise HTTPException(status_code=404, detail=f"Example notebook '{notebook_code}' not found")
        # Prevent path traversal
        real = os.path.realpath(fpath)
        if not real.startswith(os.path.realpath(self._examples_dir)):
            raise HTTPException(status_code=404, detail="Not found")
        with open(real, "r", encoding="utf-8") as f:
            return json.load(f)

    # ------------------------------------------------------------------
    # Tenant notebook CRUD
    # ------------------------------------------------------------------

    async def list_notebooks(
        self,
        catalog_id: str,
        current_user: User = Depends(get_current_active_user),
    ):
        """List all notebooks in a catalog."""
        return await notebook_service.list_notebooks(catalog_id)

    async def get_notebook(
        self,
        catalog_id: str,
        notebook_code: str,
        current_user: User = Depends(get_current_active_user),
    ):
        """Get full notebook content."""
        return await notebook_service.get_notebook(catalog_id, notebook_code)

    async def save_notebook(
        self,
        catalog_id: str,
        notebook_code: str,
        content: Dict[str, Any],
        current_user: User = Depends(get_current_active_user),
    ):
        """Save a notebook (compatible with JupyterLite save mechanism)."""
        title = content.get("metadata", {}).get("title", notebook_code)
        notebook_model = NotebookCreate(
            notebook_id=notebook_code,
            title=title,
            content=content,
            metadata=content.get("metadata", {}),
        )
        return await notebook_service.save_notebook(catalog_id, notebook_model)

    async def delete_notebook(
        self,
        catalog_id: str,
        notebook_code: str,
        current_user: User = Depends(get_current_active_user),
    ):
        """Delete a notebook from a catalog."""
        await notebook_service.delete_notebook(catalog_id, notebook_code)
        return {"status": "deleted", "notebook_id": notebook_code}

    # ------------------------------------------------------------------
    # Static file providers
    # ------------------------------------------------------------------

    @expose_static("lite")
    def serve_lite_static(self) -> List[str]:
        """Serve JupyterLite static assets."""
        static_dir = os.path.join(self._static_dir, "lite")
        files = []
        if not os.path.isdir(static_dir):
            return files
        for root, _, filenames in os.walk(static_dir):
            for filename in filenames:
                files.append(os.path.join(root, filename))
        return files

    def get_router(self) -> APIRouter:
        return self.router
