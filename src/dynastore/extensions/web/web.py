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

import os
import glob
import hashlib
import inspect
import logging
import re
from pathlib import Path
from typing import List, Any, Dict, Optional, Callable

from fastapi import APIRouter, FastAPI, Response, HTTPException, Request, Query
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.cors import CORSMiddleware
from fastapi.routing import APIRoute
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware
from dynastore.extensions import dynastore_extension, ExtensionProtocol
from dynastore.extensions.tools.conformance import register_conformance_uris, get_active_conformance, Conformance
from dynastore.extensions.web.decorators import expose_static
from dynastore.modules.apikey.policies import register_policy, register_principal, Policy, Principal
from dynastore.modules.stats.service import STATS_SERVICE
from dynastore.modules.catalog.log_manager import LOG_SERVICE

logger = logging.getLogger(__name__)

# Register public access policy for web extension
register_policy(Policy(
    id="public_access",
    description="Allows anonymous access to public landing pages and documentation.",
    actions=["GET", "OPTIONS"],
    resources=[
        "/$", 
        "/docs.*", 
        "/openapi.json", 
        "/favicon.ico.*",
        "/web.*",
        "/web/.*",
        "/web/auth/.*",
        "/web/static/.*", 
        "/web/extension-static/.*",
        "/web/website/.*",
        "/web/docs-.*",
        "/web/docs-content/.*",
        "/web/dashboard/.*",
        "/web/health",
        "/web/static/swagger_custom.js",
        "/.well-known/.*"
    ],
    effect="ALLOW",
    partition_key="global" # Will be overridden during provisioning
))

# Register Anonymous Principal
register_principal(Principal(
    provider="system",
    subject_id="anonymous",
    display_name="Anonymous User",
    roles=["anonymous"],
    is_active=True
))

def _find_project_root(start_path: str, markers: List[str]) -> Optional[str]:
    """Walks up from start_path to find a directory containing one of the marker files.
    Uses pathlib for robust parent traversal.
    """
    try:
        # Resolve path and ensure we start from a directory
        start = Path(start_path).resolve()
        if start.is_file():
            start = start.parent

        # Iterate up through parents (including current directory)
        for current_path in [start, *start.parents]:
            # Check for marker files in current path
            for marker in markers:
                marker_path = current_path / marker
                if marker_path.exists():
                    # If the marker is a project/config file, accept immediately
                    if marker.lower() in ("pyproject.toml", "setup.py"):
                        logger.info(f"Found project root via marker '{marker}' at: {current_path}")
                        return str(current_path)

                    # For generic markers (main.py), verify app structure
                    if _has_app_structure(current_path):
                        logger.info(f"Found project root via marker '{marker}' at: {current_path}")
                        return str(current_path)

        return None
    except Exception as e:
        logger.debug(f"Error resolving project root: {e}")
        return None

def _has_app_structure(root_dir: Path) -> bool:
    """Helper to verify if a directory looks like a Dynastore app."""
    comps = ["modules", "extensions", "tasks"]
    
    # 1. Check direct children
    for comp in comps:
        if (root_dir / comp).is_dir():
            return True

    # 2. Check src directory
    src_dir = root_dir / "src"
    if src_dir.is_dir():
        for child in src_dir.iterdir():
            if child.is_dir():
                for comp in comps:
                    if (child / comp).is_dir():
                        return True

    # 3. Check immediate subdirectories (permissive check)
    try:
        for child in root_dir.iterdir():
            if child.is_dir():
                for comp in comps:
                    if (child / comp).is_dir():
                        return True
    except Exception:
        pass

    return False


WEB_CONFORMANCE_URIS = [
    "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/oas30",
    "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/html",
]

@dynastore_extension
class Web(ExtensionProtocol):
    def generate_etag(self, content_parts: List[bytes]) -> str:
        """Generates a strong ETag for a list of content parts."""
        hasher = hashlib.md5()
        for part in content_parts:
            hasher.update(part)
        return f'"{hasher.hexdigest()}"'

    def get_cache_headers(self, max_age: Optional[int] = None) -> Dict[str, str]:
        """Provides default cache control headers."""
        eff_max_age = max_age if max_age is not None else self.DEFAULT_CACHE_MAX_AGE
        return {
            "Cache-Control": f"public, max-age={eff_max_age}, stale-while-revalidate=60",
            "Vary": "Accept-Encoding"
        }

    def configure_app(self, app: FastAPI):
        app.add_middleware(ProxyHeadersMiddleware, trusted_hosts="*")
        app.add_middleware(GZipMiddleware, minimum_size=1000)
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        # Discover and register static providers from other extensions
        if hasattr(app.state, "ordered_configs"):
            logger.info("WebService: Scanning other extensions for static content providers...")
            for config in app.state.ordered_configs:
                if config.instance and config.instance is not self:
                    self._scan_and_register_providers(config.instance)

        # Add root redirect if no other root endpoint exists.
        # This assumes the 'web' extension is loaded LAST in DYNASTORE_EXTENSION_MODULES
        # to correctly detect if other extensions (like 'features') have already claimed the root path.
        # has_root_endpoint = any(
        #     r.path == "/" for r in app.routes if isinstance(r, APIRoute)
        # )
        # if not has_root_endpoint:
        # This route is added directly to the app, not the router, to be at the root.
        @app.get("", include_in_schema=False)
        @app.get("/", include_in_schema=False)
        async def root_redirect(request: Request):
            # Absolute redirect to /web/ to ensure consistency
            # root_path handles proxy prefixes if configured
            root_path = request.scope.get("root_path", "").rstrip("/")
            return RedirectResponse(url=f"{root_path}/web/")

        # Explicitly handle /web redirect to ensure consistent behavior 
        # regardless of Router prefix mounting order or strict slashes config.
        @app.get("/web", include_in_schema=False)
        async def web_redirect(request: Request):
            root_path = request.scope.get("root_path", "").rstrip("/")
            # Redirect to /web/ (with trailing slash)
            return RedirectResponse(url=f"{root_path}/web/")

        #     # Prepend the new route to avoid being shadowed by path converters.
        #     # This is a common pattern when dynamically adding routes to a FastAPI app instance.
        #     app.router.routes.insert(0, app.router.routes.pop())
        #     logger.info("WebService: No root ('/') endpoint found. Added redirect to '/web'.")


    def __init__(self, app: FastAPI):
        self.app = app
        self.router = APIRouter(prefix="/web", tags=["Dynastore Web Service"])
        
        register_conformance_uris(WEB_CONFORMANCE_URIS)
        logger.info("WebService: Successfully registered generic web conformance classes.")

        # Use __file__ with helper to find root
        self.project_root = _find_project_root(__file__, ["setup.py", "pyproject.toml", "main.py"])
        
        if not self.project_root:
            logger.warning("WebService: Could not find project root. Docs scanning will be limited to local web directory.")

        self.static_dir = self._find_static_dir()
        
        # Discover application directories under the detected project root
        self.app_dirs: List[str] = []
        if self.project_root:
            try:
                self.app_dirs = self._discover_app_dirs(self.project_root)
                logger.info(f"WebService: discovered app dirs: {self.app_dirs}")
            except Exception as e:
                logger.debug(f"Error discovering app dirs: {e}")

        # Scan and build a registry of all documentation
        self.docs_registry: Dict[str, Dict[str, Any]] = self._scan_for_documentation()
        
        self.DEFAULT_CACHE_MAX_AGE = int(os.getenv("DEFAULT_CACHE_MAX_AGE", 3600))
        
        # Registry for static content providers: { prefix: bound_method }
        self.static_providers: Dict[str, Callable[[], List[str]]] = {}

        # Registry for pluggable web pages: { page_id: {title, icon, handler} }
        self.web_pages: Dict[str, Dict[str, Any]] = {}
        
        # Self-register static files from this class
        self._scan_and_register_providers(self)
        
        self._register_routes()

    def _find_static_dir(self) -> Optional[str]:
        # MODERN APPROACH: Use pathlib to find the directory of this file
        current_path = Path(__file__).resolve().parent
        
        possible_dirs = [
            current_path / "static",
            current_path.parent / "static"
        ]
        
        for d in possible_dirs:
            if d.is_dir():
                logger.info(f"WebService: Serving static files from {d}")
                return str(d)
                
        logger.warning(f"WebService: Could not find 'static' directory. Checked: {[str(p) for p in possible_dirs]}")
        return None

    def _discover_app_dirs(self, project_root: str) -> List[str]:
        """Return a list of candidate application directories."""
        candidates: List[str] = []
        comps = ("modules", "extensions", "tasks")
        root = Path(project_root)

        # 1) Direct child directories under <project_root>/src
        src_dir = root / "src"
        if src_dir.is_dir():
            for child in src_dir.iterdir():
                if child.is_dir():
                    for comp in comps:
                        if (child / comp).is_dir():
                            candidates.append(str(child.resolve()))
                            break

        # 2) Direct child directories under project_root
        for child in root.iterdir():
            if child.is_dir():
                for comp in comps:
                    if (child / comp).is_dir():
                        candidates.append(str(child.resolve()))
                        break

        # 3) project_root itself
        for comp in comps:
            if (root / comp).is_dir():
                candidates.append(str(root.resolve()))
                break

        # Deduplicate while preserving order
        seen = set()
        result: List[str] = []
        for p in candidates:
            if p not in seen:
                seen.add(p)
                result.append(p)
        return result

    def _scan_for_documentation(self) -> Dict[str, Dict[str, Any]]:
        """
        Scans for documentation recursively and returns a flat registry map.
        Key: Unique ID
        Value: { id, title, path, category }
        """
        registry = {}

        def process_doc_file(fpath: Path, category: str, component_name: Optional[str], root_reference: Path):
            """Helper to process a single markdown file"""
            try:
                # 1. Generate a clean, unique, and readable ID
                id_parts = []
                if category != "root":
                    id_parts.append(category)

                if component_name:
                    id_parts.append(component_name)

                is_readme = fpath.stem.lower() == 'readme'
                if not is_readme:
                    stem = fpath.stem.lower()
                    # If stem is 'apikey_upgrade' and component is 'apikey', we just want 'upgrade'
                    if component_name and stem.startswith(component_name):
                        clean_stem = stem[len(component_name):].lstrip('_-')
                        # Only use if it's not empty
                        if clean_stem:
                            id_parts.append(clean_stem)
                        # else, it was something like 'apikey.md', which we treat as a readme
                    else:
                        id_parts.append(stem)

                if not id_parts:
                    id_parts.append(fpath.stem.lower())
                
                # Use ':' as a separator for readability, it's a valid path parameter character.
                doc_id = ":".join(id_parts)

                # Final collision check for safety (e.g. readme.md and README.md in same folder)
                final_id = doc_id
                counter = 1
                while final_id in registry:
                    final_id = f"{doc_id}_{counter}"
                    counter += 1
                doc_id = final_id

                # 2. Extract Title (First H1)
                # Smart Fallback:
                # If README.md in 'apikey', title is likely 'Apikey' (parent dir), not 'Readme'
                if is_readme:
                    title = fpath.parent.name.replace("_", " ").title()
                else:
                    title = fpath.stem.replace("_", " ").title()

                # Attempt to read the actual title from file content
                try:
                    with open(fpath, 'r', encoding='utf-8', errors='ignore') as f:
                        for _ in range(50): # Read first 50 lines max
                            line = f.readline()
                            if not line: break
                            stripped = line.strip()
                            if stripped.startswith('#'):
                                raw_title = stripped.lstrip('#').strip()
                                if raw_title:
                                    # Clean Markdown links [Text](url) -> Text
                                    clean_title = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', raw_title)
                                    # Clean Markdown formatting * _ `
                                    title = re.sub(r'[*_`]', '', clean_title).strip()
                                    break
                except Exception as e:
                    logger.debug(f"Could not read title from {fpath}: {e}")

                # Contextualize title for sub-components
                if component_name and category != "root":
                    comp_title = component_name.replace("_", " ").title()
                    # Check if title already starts with component name (case insensitive)
                    # And avoid duplicating if title is exactly the component name
                    if title.lower() != comp_title.lower() and not title.lower().startswith(comp_title.lower()):
                         title = f"{comp_title}: {title}"

                # 3. Add to Registry
                registry[doc_id] = {
                    "id": doc_id,
                    "title": title,
                    "path": str(fpath.resolve()),
                    "category": category
                }
                logger.debug(f"Registered doc: {doc_id} -> {title}")
                
            except Exception as e:
                logger.error(f"Error processing doc file {fpath}: {e}")

        # --- A. Scan Project Root ---
        if self.project_root:
            root_path = Path(self.project_root)
            logger.info(f"Scanning root docs in: {root_path}")
            for item in root_path.glob("*.md"):
                process_doc_file(item, "root", None, root_path)
            
            # Also scan 'docs' folder in root if it exists
            docs_folder = root_path / "docs"
            if docs_folder.is_dir():
                for item in docs_folder.glob("*.md"):
                    process_doc_file(item, "root", None, docs_folder)

            # --- B. Scan App Directories (Modules, Extensions, Tasks) ---
            app_dirs = getattr(self, 'app_dirs', []) or []
            categories = ["modules", "extensions", "tasks"]
            
            for app_dir_str in app_dirs:
                app_path = Path(app_dir_str)
                
                # Scan 'docs' folder in app_path if it exists
                app_docs = app_path / "docs"
                if app_docs.is_dir():
                    for item in app_docs.glob("*.md"):
                        process_doc_file(item, "root", None, app_docs)

                for cat in categories:
                    cat_dir = app_path / cat
                    if not cat_dir.is_dir():
                        continue
                        
                    # Iterate over components (e.g., modules/auth, modules/users)
                    for component_dir in cat_dir.iterdir():
                        if not component_dir.is_dir():
                            continue
                            
                        # Recursively find ALL .md files in this component
                        # but EXPLICITLY SKIP node_modules
                        
                        # Note: We use os.walk instead of rglob to efficiently skip directories
                        for root, dirs, files in os.walk(component_dir):
                            # In-place filtering of dirs to skip node_modules traversal
                            dirs[:] = [d for d in dirs if d != "node_modules" and not d.startswith(".")]
                            
                            for filename in files:
                                if filename.lower().endswith(('.md', '.markdown')):
                                    md_file = Path(root) / filename
                                    process_doc_file(md_file, cat, component_dir.name, component_dir)

                    # If cat is "extensions", also check "extensions" in the module itself
                    if cat == "extensions":
                        # Some extensions might be inside modules
                         # Re-scan "dynastore/modules" to see if any module has an "extensions" folder
                         pass # This logic is a bit circular if not careful. Sticking to standard structure.

        logger.info(f"Documentation scan complete. Found {len(registry)} documents.")
        return registry
    
    def _scan_and_register_providers(self, instance: Any):
        for name, method in inspect.getmembers(instance, predicate=inspect.ismethod):
            if hasattr(method, "_web_static_prefix"):
                prefix = getattr(method, "_web_static_prefix").strip("/")
                self.register_static_provider(prefix, method)

            if hasattr(method, "_web_page_config"):
                config = getattr(method, "_web_page_config")
                self.register_web_page(config, method)

    def register_web_page(self, config: Dict[str, str], provider: Callable[[], Any]):
        page_id = config["id"]
        if page_id in self.web_pages:
            logger.warning(f"WebService: Overwriting web page '{page_id}'")
        
        self.web_pages[page_id] = {
            "id": page_id,
            "title": config["title"],
            "icon": config["icon"],
            "description": config.get("description", ""),
            "handler": provider
        }
        logger.info(f"WebService: Registered web page '{page_id}'")

    def register_static_provider(self, prefix: str, provider: Callable[[], List[str]]):
        if prefix in self.static_providers:
            logger.warning(f"WebService: Overwriting static provider for prefix '{prefix}'")
        self.static_providers[prefix] = provider
        logger.info(f"WebService: Registered static provider for prefix '{prefix}'")

    @expose_static("static")
    def _provide_default_static(self) -> List[str]:
        if not self.static_dir:
            return []
        files = []
        for root, _, filenames in os.walk(self.static_dir):
            for filename in filenames:
                files.append(os.path.join(root, filename))
        return files

    @expose_static("website")
    def _provide_website_static(self) -> List[str]:
        website_dir = os.path.join(os.path.dirname(__file__), "static", "website")
        files = []
        for root, _, filenames in os.walk(website_dir):
            for filename in filenames:
                files.append(os.path.join(root, filename))
        return files

    @expose_static("dashboard")
    def _provide_dashboard_static(self) -> List[str]:
        dashboard_dir = os.path.join(os.path.dirname(__file__), "static", "dashboard")
        files = []
        for root, _, filenames in os.walk(dashboard_dir):
            for filename in filenames:
                files.append(os.path.join(root, filename))
        return files

    @expose_static("extension-static")
    def _provide_extension_static(self) -> List[str]:
        return self._provide_default_static()

    async def serve_file(self, file_path: str) -> Response:
        if not os.path.isfile(file_path):
            raise HTTPException(status_code=404, detail="File not found")
            
        mime_types = {
            ".css": "text/css", ".js": "application/javascript", ".json": "application/json",
            ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
            ".svg": "image/svg+xml", ".gif": "image/gif", ".ico": "image/x-icon",
            ".html": "text/html", ".md": "text/markdown", ".wasm": "application/wasm",
            ".whl": "application/octet-stream"
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

        from starlette.responses import RedirectResponse

        @self.router.get("", include_in_schema=False)
        async def redirect_web_root(request: Request):
             # Force trailing slash for relative assets to work
             path = request.url.path
             if not path.endswith('/'):
                 return RedirectResponse(url=path + "/")
             # If already has slash, serve index (should be handled by @router.get("/"))
             return await read_extension_root()

        @self.router.get("/", include_in_schema=False)
        async def read_extension_root():
            # Redirect root to website
            website_index = os.path.join(os.path.dirname(__file__), "static", "website", "index.html")
            if os.path.exists(website_index):
                return await self.serve_file(website_index)
            # Fallback to default index if website not present
            if self.static_dir:
                 index_path = os.path.join(self.static_dir, "index.html")
                 if os.path.exists(index_path):
                     return await self.serve_file(index_path)
            return HTMLResponse("Not Found", status_code=404)

        @self.router.get("/dashboard/")
        async def read_dashboard_root():
             dashboard_index = os.path.join(os.path.dirname(__file__), "static", "dashboard", "index.html")
             if os.path.exists(dashboard_index):
                 return await self.serve_file(dashboard_index)
             return HTMLResponse("Dashboard Not Found", status_code=404)
        
        @self.router.get("/health", tags=["Web Health"])
        async def health_check():
            return {"status": "ok"}

        @self.router.get("/config/pages", response_class=JSONResponse)
        async def get_web_pages_config(language: str = Query("en")):
            """Returns the list of registered web pages for the frontend navigation."""
            results = []
            for p in self.web_pages.values():
                title = p["title"]
                if isinstance(title, dict):
                    title = title.get(language, title.get("en", next(iter(title.values()))))
                
                description = p.get("description", "")
                if isinstance(description, dict):
                    description = description.get(language, description.get("en", next(iter(description.values())) if description else ""))

                results.append({
                    "id": p["id"], 
                    "title": title, 
                    "icon": p["icon"], 
                    "description": description
                })
            return results

        @self.router.get("/pages/{page_id}", response_class=HTMLResponse)
        async def get_web_page_content(page_id: str, request: Request, language: str = Query("en")):
             if page_id not in self.web_pages:
                 raise HTTPException(status_code=404, detail="Page not found")
             
             handler = self.web_pages[page_id]["handler"]
             try:
                 # Check if handler accepts arguments to avoid TypeError
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
            """
            Returns the manifest of all found documentation, grouped by category.
            Includes ID, Title, and Full Path.
            """
            # Use raw buckets first
            buckets = {"root": [], "modules": [], "extensions": [], "tasks": []}
            
            # Iterate through the registry and bucket items by category
            for doc_item in self.docs_registry.values():
                cat = doc_item.get("category", "root")
                if cat not in buckets:
                    buckets[cat] = []
                
                buckets[cat].append({
                    "id": doc_item["id"],
                    "title": doc_item["title"],
                    "path": doc_item["path"]
                })

            # Create the final manifest using raw category IDs as keys
            manifest = {}
            for cat, items in buckets.items():
                if not items:
                    continue
                    
                # Sort each category alphabetically by title
                items.sort(key=lambda x: x['title'])
                
                # Use raw category ID as key for the frontend
                manifest[cat] = items
            
            return manifest

        @self.router.get("/docs-content/{doc_id:path}", response_class=HTMLResponse)
        async def get_doc_content(doc_id: str):
            doc_item = self.docs_registry.get(doc_id)
            
            if not doc_item or not os.path.exists(doc_item["path"]):
                raise HTTPException(status_code=404, detail="Documentation not found")
            
            try:
                import markdown
            except ImportError:
                logger.error("Markdown library not installed. Documentation rendering unavailable.")
                return HTMLResponse("<h1>Documentation renderer (markdown) not installed</h1>", status_code=500)

            try:
                with open(doc_item["path"], "r", encoding="utf-8") as f:
                    md_content = f.read()
                
                # Convert to HTML
                html_content = markdown.markdown(
                    md_content, 
                    extensions=['fenced_code', 'tables', 'def_list', 'nl2br']
                )
                return HTMLResponse(content=html_content)
            except Exception as e:
                logger.error(f"Error reading doc {doc_id}: {e}")
                raise HTTPException(status_code=500, detail="Error reading document")

        @self.router.get("/dashboard/stats", response_class=JSONResponse)
        async def get_dashboard_stats(request: Request):
            catalog_id = getattr(request.state, "catalog_id", None)
            
            # Resolve schema using ApiKeyManager logic or CatalogModule
            from dynastore.modules import get_protocol
            from dynastore.models.protocols import CatalogsProtocol
            catalogs = get_protocol(CatalogsProtocol)
            db_resource = catalogs.engine if catalogs else None
            schema = await catalogs.resolve_physical_schema(catalog_id, db_resource=db_resource) or "catalog"

            summary = await STATS_SERVICE.get_summary(schema=schema)
            return summary.model_dump() if summary else {"total_requests": 0, "average_latency_ms": 0}

        @self.router.get("/dashboard/tasks", response_class=JSONResponse)
        async def get_dashboard_tasks():
            tasks_ext = getattr(self.app.state, 'tasks', None)
            if tasks_ext:
                tasks = await tasks_ext.get_tasks()
                return tasks
            return []

        @self.router.get("/dashboard/logs", response_class=JSONResponse)
        async def get_dashboard_logs(limit: int = 50):
            from dynastore.extensions.logs.log_extension import get_log_extension
            log_ext = get_log_extension()
            if log_ext:
                logs = await log_ext.search_logs(catalog_id="_system_", limit=limit)
                return [l.model_dump() for l in logs]
            return []

        @self.router.get("/{prefix}/{filename:path}", include_in_schema=False)
        async def serve_static_content(prefix: str, filename: str):
            if prefix not in self.static_providers:
                raise HTTPException(status_code=404, detail=f"No provider found for prefix '{prefix}'")
            
            provider = self.static_providers[prefix]
            try:
                allowed_files = provider()
            except Exception as e:
                logger.error(f"Provider for '{prefix}' failed: {e}")
                raise HTTPException(status_code=500, detail="Provider error")
                
            allowed_map = {os.path.abspath(f): f for f in allowed_files}
            target_file = None
            for abs_path in allowed_map.keys():
                if abs_path.replace(os.sep, '/').endswith(filename.replace(os.sep, '/')):
                     target_file = abs_path
                     break
            
            if not target_file:
                 logger.warning(f"Access denied or file not found in provider allowlist: {filename} for prefix {prefix}")
                 raise HTTPException(status_code=404, detail="File not found")
            
            return await self.serve_file(target_file)