import os
import logging
import inspect
from typing import List, Any, Dict, Optional

from fastapi import APIRouter, FastAPI, Response, HTTPException, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.cors import CORSMiddleware
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

from dynastore.extensions import dynastore_extension, ExtensionProtocol
from dynastore.modules import get_protocol
from dynastore.models.protocols.web import WebModuleProtocol

# Use the new module-level decorators
from dynastore.modules.web.decorators import expose_static, expose_web_page

logger = logging.getLogger(__name__)


@dynastore_extension
class Geoid(ExtensionProtocol):
    def __init__(self, app: FastAPI):
        self.app = app
        self.router = APIRouter(prefix="/web", tags=["GeoID Web Service"])

        self.static_dir = os.path.join(os.path.dirname(__file__), "static")

        # Get the Web Module
        self.web_module: Optional[WebModuleProtocol] = get_protocol(WebModuleProtocol)

        if not self.web_module:
            logger.error(
                "GeoidExtension: WebModule not found! Functionality will be limited."
            )
            # We could raise an error, but let's try to degrade gracefully or fail hard.
            # Given it replaces 'web', it implies it needs the module.

        self._configure_app()
        self._register_routes()

        # Register self
        if self.web_module:
            self.web_module.scan_and_register_providers(self)

    def _configure_app(self):
        self.app.add_middleware(ProxyHeadersMiddleware, trusted_hosts="*")
        self.app.add_middleware(GZipMiddleware, minimum_size=1000)
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        # Scan other extensions for content
        if self.web_module and hasattr(self.app.state, "ordered_configs"):
            logger.info("GeoidExtension: Scanning extensions for web content...")
            for config in self.app.state.ordered_configs:
                if config.instance and config.instance is not self:
                    self.web_module.scan_and_register_providers(config.instance)

        # Root Redirects
        @self.app.get("", include_in_schema=False)
        @self.app.get("/", include_in_schema=False)
        async def root_redirect(request: Request):
            root_path = request.scope.get("root_path", "").rstrip("/")
            return RedirectResponse(url=f"{root_path}/web/")

        @self.app.get("/web", include_in_schema=False)
        async def web_redirect(request: Request):
            root_path = request.scope.get("root_path", "").rstrip("/")
            return RedirectResponse(url=f"{root_path}/web/")

    @expose_static("geoid")
    def _provide_geoid_static(self) -> List[str]:
        if not self.static_dir or not os.path.isdir(self.static_dir):
            return []
        files = []
        for root, _, filenames in os.walk(self.static_dir):
            for filename in filenames:
                files.append(os.path.join(root, filename))
        return files

    @expose_web_page(
        page_id="geoid",
        title={
            "en": "Geoid Service",
            "es": "Servicio de Geoide",
            "fr": "Service géoïde",
        },
        icon="fa-earth-americas",
        description={
            "en": "Geoid height calculation service.",
            "es": "Cálculo de altura del geoide.",
            "fr": "Calcul de la hauteur du géoïde.",
        },
    )
    def geoid_page(self, language: str = "en"):
        # Sanitize language to avoid path traversal or invalid chars
        lang_code = language.lower().split("-")[0]
        if lang_code not in ["en", "es", "fr"]:
            lang_code = "en"

        # Try to find specific language index
        index_filename = f"index_{lang_code}.html"
        index_path = os.path.join(self.static_dir, index_filename)

        # Fallback to default index.html if specific lang not found
        if not os.path.exists(index_path):
            index_path = os.path.join(self.static_dir, "index.html")

        if os.path.exists(index_path):
            with open(index_path, "r", encoding="utf-8") as f:
                content = f.read()
                return HTMLResponse(content=content)
        return HTMLResponse("Geoid service page not found", status_code=404)

    # --- Routing Logic (Delegates to WebModule registries) ---

    async def serve_file(self, file_path: str) -> Response:
        if not os.path.isfile(file_path):
            raise HTTPException(status_code=404, detail="File not found")

        mime_types = {
            ".css": "text/css",
            ".js": "application/javascript",
            ".json": "application/json",
            ".png": "image/png",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".svg": "image/svg+xml",
            ".gif": "image/gif",
            ".ico": "image/x-icon",
            ".html": "text/html",
            ".md": "text/markdown",
            ".wasm": "application/wasm",
            ".whl": "application/octet-stream",
        }
        _, ext = os.path.splitext(file_path)
        media_type = mime_types.get(ext.lower(), "application/octet-stream")

        try:
            with open(file_path, "rb") as f:
                content = f.read()
            return Response(content=content, media_type=media_type)
        except Exception as e:
            logger.error(f"Error serving file {file_path}: {e}")
            raise HTTPException(status_code=500, detail="Internal Server Error")

    def _register_routes(self):

        @self.router.get("", include_in_schema=False)
        async def redirect_web_root(request: Request):
            path = request.url.path
            if not path.endswith("/"):
                return RedirectResponse(url=path + "/")
            return await read_extension_root()

        @self.router.get("/", include_in_schema=False)
        async def read_extension_root():
            # Geoid Extension specific root: Serve the Geoid App directly?
            # Or redirect to the geoid page?
            # The previous 'web' extension served a dashboard/website.
            # Let's serve the geoid page as the root for this extension.
            return self.geoid_page()

        @self.router.get("/health", tags=["Web Health"])
        async def health_check():
            return {"status": "ok", "provider": "geoid"}

        @self.router.get("/config/pages", response_class=JSONResponse)
        async def get_web_pages_config(language: str = Query("en")):
            if not self.web_module:
                return []
            return self.web_module.get_web_pages_config(language)

        @self.router.get("/pages/{page_id}", response_class=HTMLResponse)
        async def get_web_page_content(
            page_id: str, request: Request, language: str = Query("en")
        ):
            if not self.web_module or page_id not in self.web_module.web_pages:
                raise HTTPException(status_code=404, detail="Page not found")

            handler = self.web_module.web_pages[page_id]["handler"]
            try:
                sig = inspect.signature(handler)
                kwargs = {}
                if "request" in sig.parameters:
                    kwargs["request"] = request
                if "language" in sig.parameters:
                    kwargs["language"] = language

                if inspect.iscoroutinefunction(handler):
                    content = await handler(**kwargs)
                else:
                    content = handler(**kwargs)

                if isinstance(content, Response):
                    return content
                return HTMLResponse(content=str(content))
            except Exception as e:
                logger.error(f"Error rendering page {page_id}: {e}", exc_info=True)
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.get("/docs-manifest", response_class=JSONResponse)
        async def get_docs_manifest():
            if not self.web_module:
                return {}
            return self.web_module.get_docs_manifest()

        @self.router.get("/docs-content/{doc_id:path}", response_class=HTMLResponse)
        async def get_doc_content(doc_id: str):
            if not self.web_module:
                raise HTTPException(status_code=404)

            path = self.web_module.get_doc_path(doc_id)
            if not path or not os.path.exists(path):
                raise HTTPException(status_code=404, detail="Documentation not found")

            try:
                import markdown

                with open(path, "r", encoding="utf-8") as f:
                    md_content = f.read()
                html_content = markdown.markdown(
                    md_content,
                    extensions=["fenced_code", "tables", "def_list", "nl2br"],
                )
                return HTMLResponse(content=html_content)
            except Exception as e:
                logger.error(f"Error reading doc {doc_id}: {e}")
                raise HTTPException(status_code=500, detail="Error reading document")

        @self.router.get("/{prefix}/{filename:path}", include_in_schema=False)
        async def serve_static_content(prefix: str, filename: str):
            if not self.web_module or prefix not in self.web_module.static_providers:
                raise HTTPException(
                    status_code=404, detail=f"No provider found for prefix '{prefix}'"
                )

            provider = self.web_module.static_providers[prefix]
            try:
                allowed_files = provider()
            except Exception as e:
                raise HTTPException(status_code=500, detail="Provider error")

            allowed_map = {os.path.abspath(f): f for f in allowed_files}
            target_file = None
            for abs_path in allowed_map.keys():
                if abs_path.replace(os.sep, "/").endswith(
                    filename.replace(os.sep, "/")
                ):
                    target_file = abs_path
                    break

            if not target_file:
                raise HTTPException(status_code=404, detail="File not found")

            return await self.serve_file(target_file)
