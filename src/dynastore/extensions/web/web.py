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
import itertools
import logging
import re
from pathlib import Path
from datetime import datetime
from typing import List, Any, Dict, Optional, Callable

from fastapi import APIRouter, FastAPI, Response, HTTPException, Request, Query, Header

from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.cors import CORSMiddleware
from fastapi.routing import APIRoute
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware
from dynastore.extensions import ExtensionProtocol, get_extension_instance
from dynastore.extensions.tools.conformance import (
    register_conformance_uris,
    get_active_conformance,
    Conformance,
)
from dynastore.extensions.web.decorators import expose_static, expose_web_page
from dynastore.models.protocols.policies import PermissionProtocol, Policy, Role, Principal
from dynastore.tools.discovery import get_protocol, get_protocols, register_plugin

# Register public access policy for web extension
logger = logging.getLogger(__name__)

def register_web_policies():
    """Register the web extension's public-access policy and anonymous role.

    Always registers into the module-level in-memory registry so that
    `provision_default_policies()` picks them up at lifespan startup, even
    when this is called before the `PermissionProtocol` (ApiKeyModule) is
    fully initialised.
    """
    from dynastore.modules.apikey.policies import (
        register_policy as _reg_policy,
        register_role as _reg_role,
    )

    web_policy = Policy(
        id="web_public_access",
        description="Allows anonymous access to web UI, pages, and static assets.",
        actions=["GET", "OPTIONS"],
        resources=[
            "/$",
            "/docs.*",
            "/openapi.json",
            "/favicon.ico.*",
            "/web",
            "/web/",
            "/web/.*",
            "/web/pages/.*",           # expose_web_page routes
            "/web/extension-static/.*", # expose_static routes
            "/web/static/.*",
            "/web/website/.*",
            "/web/docs-content/.*",
            "/web/dashboard/.*",
            "/web/health",
            "/.well-known/.*",
            "/processes.*",
            "/configs/schemas",
            "/configs/web_config",
            "/configs/plugins",
        ],
        effect="ALLOW",
    )
    _reg_policy(web_policy)
    _reg_role(Role(name="anonymous", policies=["web_public_access"], is_system=True))

    # Sysadmin-only: POST/DELETE actions on /web/admin/* (demo populate/cleanup, etc.)
    web_sysadmin_policy = Policy(
        id="web_sysadmin_access",
        description="Grants sysadmin write access to web admin management endpoints.",
        actions=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        resources=[
            "/web/admin",
            "/web/admin/",
            "/web/admin/.*",
            "/web/pages/demo_manager",  # expose_web_page route
        ],
        effect="ALLOW",
    )
    _reg_policy(web_sysadmin_policy)
    _reg_role(Role(name="sysadmin", policies=["web_sysadmin_access"], is_system=True))

    logger.debug("Web policies pre-registered into in-memory registry.")

    # If the PermissionProtocol is already live (e.g. on a hot-reload path), push
    # the changes immediately so they take effect without waiting for lifespan.
    policy_manager = get_protocol(PermissionProtocol)
    if policy_manager:
        policy_manager.register_policy(web_policy)
        policy_manager.register_role(
            Role(name="anonymous", policies=["web_public_access"], is_system=True)
        )
        logger.debug("Web policies also applied to live PermissionProtocol.")

    # Register Anonymous Principal as a plugin for discovery
    register_plugin(
        Principal(
            provider="system",
            subject_id="anonymous",
            display_name="Anonymous User",
            roles=["anonymous"],
            is_active=True,
        )
    )

# Auto-register policies on import so that modular loaders (like ApiKeyModule)
# pick them up during foundational lifespan before extension instantiation.
register_web_policies()

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
                        logger.info(
                            f"Found project root via marker '{marker}' at: {current_path}"
                        )
                        return str(current_path)

                    # For generic markers (main.py), verify app structure
                    if _has_app_structure(current_path):
                        logger.info(
                            f"Found project root via marker '{marker}' at: {current_path}"
                        )
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

from dynastore.models.protocols.web import WebModuleProtocol, WebPageProtocol, StaticFilesProtocol
from dynastore.modules.db_config.platform_config_service import PluginConfig, ConfigRegistry


class WebConfig(PluginConfig):
    """Configuration for the Web Platform interface."""
    brand_name: str = "Agro-Informatics Platform"
    brand_subtitle: str = "Catalog Services"
    token_key: str = "ds_token"
    default_language: str = "en"
    enterprise_tier: bool = True


ConfigRegistry.register("web_config", WebConfig)
class Web(ExtensionProtocol):
    """
    Core web platform extension.

    All pages (home, docs, dashboard) and static prefixes (static, website,
    dashboard, extension-static) are registered exclusively via the
    @expose_web_page / @expose_static decorator scan that runs in configure_app.

    This class intentionally does NOT inherit WebPageProtocol or
    StaticFilesProtocol — that would trigger a second registration of the
    same handlers through the global protocol scan in WebModule.lifespan,
    causing every page to render its content twice.

    Other extensions (e.g. Geoid, Tiles, STAC) that implement those protocols
    are discovered by the decorator scan AND the global scan; the deduplication
    guard in WebModule.register_web_page prevents double-rendering.
    """

    priority: int = 100  # High number = low priority (registers last)
    router: APIRouter = APIRouter(prefix="/web", tags=["Dynastore Web Service"])

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
            "Vary": "Accept-Encoding",
        }

    def configure_app(self, app: FastAPI):
        """Configures global settings like middleware and CORS."""
        app.add_middleware(ProxyHeadersMiddleware, trusted_hosts="*")
        app.add_middleware(GZipMiddleware, minimum_size=1000)
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        # Discover and register static / page providers from other extensions.
        # Skip self — Web.__init__() already called scan_and_register_providers(self).
        if hasattr(app.state, "ordered_configs"):
            logger.info(
                "WebService: Scanning other extensions for static content providers..."
            )
            for config in app.state.ordered_configs:
                if config.instance and config.instance is not self:
                    self.scan_and_register_providers(config.instance)

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

    def __init__(self, app: Optional[FastAPI] = None):
        self.app = app

        # Conformance URIs
        register_conformance_uris(WEB_CONFORMANCE_URIS)
        logger.info(
            "WebService: Successfully registered generic web conformance classes."
        )

        # Use __file__ with helper to find root
        self.project_root = _find_project_root(
            __file__, ["setup.py", "pyproject.toml", "main.py"]
        )

        if not self.project_root:
            logger.warning(
                "WebService: Could not find project root. Docs scanning will be limited to local web directory."
            )

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

        # Registry for pluggable web pages: DEPRECATED - now managed by WebModule
        # self.web_pages: Dict[str, Dict[str, Any]] = {}

        # Self-register via discovery if possible
        self.web_module = get_protocol(WebModuleProtocol)
        if self.web_module:
            self.web_module.scan_and_register_providers(self)

        self._register_routes()

    def _find_static_dir(self) -> Optional[str]:
        # MODERN APPROACH: Use pathlib to find the directory of this file
        current_path = Path(__file__).resolve().parent

        possible_dirs = [current_path / "static", current_path.parent / "static"]

        for d in possible_dirs:
            if d.is_dir():
                logger.info(f"WebService: Serving static files from {d}")
                return str(d)

        logger.warning(
            f"WebService: Could not find 'static' directory. Checked: {[str(p) for p in possible_dirs]}"
        )
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

        def process_doc_file(
            fpath: Path,
            category: str,
            component_name: Optional[str],
            root_reference: Path,
        ):
            """Helper to process a single markdown file"""
            try:
                # 1. Generate a clean, unique, and readable ID
                id_parts = []
                if category != "root":
                    id_parts.append(category)

                if component_name:
                    id_parts.append(component_name)

                is_readme = fpath.stem.lower() == "readme"
                if not is_readme:
                    stem = fpath.stem.lower()
                    # If stem is 'apikey_upgrade' and component is 'apikey', we just want 'upgrade'
                    if component_name and stem.startswith(component_name):
                        clean_stem = stem[len(component_name) :].lstrip("_-")
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
                    with open(fpath, "r", encoding="utf-8", errors="ignore") as f:
                        for _ in range(50):  # Read first 50 lines max
                            line = f.readline()
                            if not line:
                                break
                            stripped = line.strip()
                            if stripped.startswith("#"):
                                raw_title = stripped.lstrip("#").strip()
                                if raw_title:
                                    # Clean Markdown links [Text](url) -> Text
                                    clean_title = re.sub(
                                        r"\[([^\]]+)\]\([^)]+\)", r"\1", raw_title
                                    )
                                    # Clean Markdown formatting * _ `
                                    title = re.sub(r"[*_`]", "", clean_title).strip()
                                    break
                except Exception as e:
                    logger.debug(f"Could not read title from {fpath}: {e}")

                # Contextualize title for sub-components
                if component_name and category != "root":
                    comp_title = component_name.replace("_", " ").title()
                    # Check if title already starts with component name (case insensitive)
                    # And avoid duplicating if title is exactly the component name
                    if (
                        title.lower() != comp_title.lower()
                        and not title.lower().startswith(comp_title.lower())
                    ):
                        title = f"{comp_title}: {title}"

                # 3. Add to Registry
                registry[doc_id] = {
                    "id": doc_id,
                    "title": title,
                    "path": str(fpath.resolve()),
                    "category": category,
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
                _SKIP_STEMS = {"files_to_remove"}  # internal/admin docs to hide
                for item in sorted(docs_folder.iterdir()):
                    if item.is_file() and item.suffix.lower() in (".md", ".markdown"):
                        if item.stem.lower() not in _SKIP_STEMS:
                            process_doc_file(item, "platform", None, docs_folder)
                    elif item.is_dir():
                        # Sub-directory (architecture/, components/, …) → own category
                        subcat = item.name.lower()
                        for md in sorted(item.rglob("*.md")):
                            process_doc_file(md, subcat, None, item)

            # --- B. Scan App Directories (Modules, Extensions, Tasks) ---
            app_dirs = getattr(self, "app_dirs", []) or []
            categories = ["modules", "extensions", "tasks"]

            for app_dir_str in app_dirs:
                app_path = Path(app_dir_str)

                # Scan 'docs' folder in app_path if it exists
                app_docs = app_path / "docs"
                if app_docs.is_dir():
                    for item in app_docs.rglob("*.md"):
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
                            dirs[:] = [
                                d
                                for d in dirs
                                if d != "node_modules" and not d.startswith(".")
                            ]

                            for filename in files:
                                if filename.lower().endswith((".md", ".markdown")):
                                    md_file = Path(root) / filename
                                    process_doc_file(
                                        md_file, cat, component_dir.name, component_dir
                                    )

                    # If cat is "extensions", also check "extensions" in the module itself
                    if cat == "extensions":
                        # Some extensions might be inside modules
                        # Re-scan "dynastore/modules" to see if any module has an "extensions" folder
                        pass  # This logic is a bit circular if not careful. Sticking to standard structure.

        logger.info(f"Documentation scan complete. Found {len(registry)} documents.")
        return registry

    # No WebPageProtocol / StaticFilesProtocol methods here.
    # All pages and static prefixes are registered via @expose_web_page /
    # @expose_static decorators, discovered by Web.configure_app() calling
    # web_module.scan_and_register_providers(instance) for each extension.

    # Legacy method compatibility
    def scan_and_register_providers(self, instance: Any):
        if self.web_module:
            self.web_module.scan_and_register_providers(instance)

    @expose_web_page(page_id="home", title="Home", icon="fa-home", priority=-100)
    def home_page(self, language: str = "en"):
        # We wrap the content in a container that can be appended to or replaced
        return """
        <div id="section-home" class="fade-in max-w-7xl mx-auto">
            <header class="py-16 text-center ds-default-home">
                <div class="inline-flex items-center gap-2 px-3 py-1 rounded-full border border-emerald-500/30 bg-emerald-500/10 text-emerald-300 text-xs font-medium mb-6">
                    <span class="w-2 h-2 rounded-full bg-emerald-400 animate-pulse"></span>
                    Agro-Informatics Platform
                </div>
                <h1 class="text-4xl md:text-6xl font-bold text-white mb-6 tracking-tight">
                    <span id="home-title">Agro-Informatics Hub</span><br>
                    <span class="text-transparent bg-clip-text bg-gradient-to-r from-emerald-400 to-blue-400" id="home-subtitle">Digital Agricultural Infrastructure</span>
                </h1>
                <p class="text-slate-400 max-w-2xl mx-auto mb-10" id="home-description">
                    High-performance geospatial data catalog and processing platform for agricultural intelligence.
                </p>
                <div class="flex items-center justify-center gap-6" id="home-actions">
                    <button onclick="switchTab('docs')" class="px-6 py-3 rounded-lg bg-white/5 hover:bg-white/10 border border-white/10 text-white font-medium transition-all">
                        Documentation
                    </button>
                    <button onclick="switchTab('stac_browser')" class="px-6 py-3 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white font-medium shadow-lg shadow-emerald-500/20 transition-all">
                        Explore Data
                    </button>
                </div>
            </header>

            <div id="home-content-append" class="space-y-12">
                <!-- Extensions can append content here or replace parts via JS -->
            </div>
            
            <div class="grid md:grid-cols-2 lg:grid-cols-3 gap-6 mt-12 ds-default-home" id="home-featured-grid">
                 <div class="glass-panel p-6 rounded-2xl border border-white/5 hover:border-emerald-500/30 transition-colors group cursor-pointer" onclick="switchTab('stac_browser')">
                    <div class="w-12 h-12 rounded-xl bg-emerald-500/10 flex items-center justify-center text-emerald-400 mb-4 group-hover:scale-110 transition-transform">
                        <i class="fa-solid fa-layer-group text-xl"></i>
                    </div>
                    <h3 class="text-xl font-semibold text-white mb-2">STAC Browser</h3>
                    <p class="text-slate-400 text-sm">Explore satellite imagery and geospatial assets using the STAC standard.</p>
                 </div>
                 <div class="glass-panel p-6 rounded-2xl border border-white/5 hover:border-blue-500/30 transition-colors group cursor-pointer" onclick="switchTab('map_viewer')">
                    <div class="w-12 h-12 rounded-xl bg-blue-500/10 flex items-center justify-center text-blue-400 mb-4 group-hover:scale-110 transition-transform">
                        <i class="fa-solid fa-map text-xl"></i>
                    </div>
                    <h3 class="text-xl font-semibold text-white mb-2">Map Viewer</h3>
                    <p class="text-slate-400 text-sm">Visualize tiled datasets and explore geospatial layers in real-time.</p>
                 </div>
                 <div class="glass-panel p-6 rounded-2xl border border-white/5 hover:border-purple-500/30 transition-colors group cursor-pointer" onclick="switchTab('dashboard')">
                    <div class="w-12 h-12 rounded-xl bg-purple-500/10 flex items-center justify-center text-purple-400 mb-4 group-hover:scale-110 transition-transform">
                        <i class="fa-solid fa-gauge-high text-xl"></i>
                    </div>
                    <h3 class="text-xl font-semibold text-white mb-2">Platform Stats</h3>
                    <p class="text-slate-400 text-sm">Monitor system health, usage statistics, and background task progress.</p>
                 </div>
            </div>
        </div>
        """

    @expose_web_page(
        page_id="demo_manager",
        title="Demo Data",
        icon="fa-flask",
        description="Provision or clean up the demo catalog for testing.",
        required_roles=["sysadmin"],
        section="admin",
        priority=40,
    )
    def demo_manager_page(self, language: str = "en"):
        return """
<div class="space-y-8 max-w-2xl">
  <div>
    <h2 class="text-2xl font-bold text-white mb-1">Demo Data Manager</h2>
    <p class="text-slate-400 text-sm">
      Provision a <code class="bg-slate-800 px-1 rounded text-xs">demo_catalog</code> with sample
      geospatial points, or wipe it to start fresh. Requires <strong class="text-white">sysadmin</strong> role.
    </p>
  </div>

  <div class="grid md:grid-cols-2 gap-6">
    <!-- Provision -->
    <div class="glass-panel p-6 rounded-2xl border border-emerald-500/20">
      <div class="w-12 h-12 rounded-xl bg-emerald-500/10 border border-emerald-500/20 flex items-center justify-center text-emerald-400 mb-4">
        <i class="fa-solid fa-database-plus text-xl"></i>
      </div>
      <h3 class="font-semibold text-white mb-1">Provision Demo Data</h3>
      <p class="text-slate-400 text-sm mb-4">
        Creates <code class="bg-slate-800 px-1 rounded text-xs">demo_catalog</code> with
        <code class="bg-slate-800 px-1 rounded text-xs">demo_collection</code> containing a 2×3 grid of
        tile polygons covering Italy. Any existing demo catalog is replaced.
      </p>
      <button id="btn-populate"
        onclick="demoAction('populate')"
        class="w-full px-4 py-2 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white text-sm font-medium transition-all flex items-center justify-center gap-2">
        <i class="fa-solid fa-plus-circle"></i> Provision Demo Data
      </button>
    </div>

    <!-- Cleanup -->
    <div class="glass-panel p-6 rounded-2xl border border-red-500/20">
      <div class="w-12 h-12 rounded-xl bg-red-500/10 border border-red-500/20 flex items-center justify-center text-red-400 mb-4">
        <i class="fa-solid fa-trash-alt text-xl"></i>
      </div>
      <h3 class="font-semibold text-white mb-1">Clean Up Demo Data</h3>
      <p class="text-slate-400 text-sm mb-4">
        Permanently deletes <code class="bg-slate-800 px-1 rounded text-xs">demo_catalog</code>
        and all its collections and items. This action cannot be undone.
      </p>
      <button id="btn-cleanup"
        onclick="demoAction('cleanup')"
        class="w-full px-4 py-2 rounded-lg bg-red-700 hover:bg-red-600 text-white text-sm font-medium transition-all flex items-center justify-center gap-2">
        <i class="fa-solid fa-trash-alt"></i> Clean Up Demo Data
      </button>
    </div>
  </div>

  <div id="demo-result" class="hidden glass-panel p-4 rounded-xl border border-white/5 text-sm"></div>
</div>

<script>
async function demoAction(action) {
  const labels = { populate: 'provision demo data', cleanup: 'DELETE the demo catalog' };
  if (!confirm(`Are you sure you want to ${labels[action]}?\\nThis cannot be undone.`)) return;

  const btnId = action === 'populate' ? 'btn-populate' : 'btn-cleanup';
  const btn = document.getElementById(btnId);
  const resultDiv = document.getElementById('demo-result');
  if (btn) { btn.disabled = true; btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Working…'; }
  if (resultDiv) resultDiv.classList.add('hidden');

  const tkey = (typeof TOKEN_KEY !== 'undefined') ? TOKEN_KEY : 'ds_token';
  const token = (typeof authToken !== 'undefined' && authToken) || localStorage.getItem(tkey);
  try {
    const res = await fetch(`/web/admin/demo/${action}`, {
      method: 'POST',
      headers: token ? { 'Authorization': 'Bearer ' + token } : {}
    });
    const data = await res.json();
    if (resultDiv) {
      resultDiv.classList.remove('hidden', 'border-red-500/20', 'border-emerald-500/20');
      if (res.ok) {
        const msg = action === 'populate'
          ? `Demo catalog provisioned: <strong>${data.items}</strong> items in <code>${data.catalog_id} / ${data.collection_id}</code>.`
          : `Demo catalog <code>${data.deleted}</code> deleted successfully.`;
        resultDiv.className = 'glass-panel p-4 rounded-xl border border-emerald-500/20 text-emerald-300 text-sm';
        resultDiv.innerHTML = '<i class="fa-solid fa-check-circle mr-2"></i>' + msg;
      } else {
        resultDiv.className = 'glass-panel p-4 rounded-xl border border-red-500/20 text-red-400 text-sm';
        resultDiv.innerHTML = '<i class="fa-solid fa-exclamation-triangle mr-2"></i>' + (data.detail || JSON.stringify(data));
      }
    }
  } catch (e) {
    if (resultDiv) {
      resultDiv.className = 'glass-panel p-4 rounded-xl border border-red-500/20 text-red-400 text-sm';
      resultDiv.innerHTML = '<i class="fa-solid fa-exclamation-triangle mr-2"></i>' + e.message;
      resultDiv.classList.remove('hidden');
    }
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.innerHTML = action === 'populate'
        ? '<i class="fa-solid fa-plus-circle"></i> Provision Demo Data'
        : '<i class="fa-solid fa-trash-alt"></i> Clean Up Demo Data';
    }
  }
}
</script>
"""

    @expose_web_page(page_id="docs", title="Documentation", icon="fa-book", priority=-100)
    def docs_page(self, language: str = "en"):
        return """
        <div class="grid lg:grid-cols-[300px_1fr] gap-8">
            <aside class="glass-panel p-4 rounded-xl h-[calc(100vh-200px)] overflow-y-auto" id="docs-sidebar-content"></aside>
            <article class="glass-panel p-8 rounded-xl prose prose-invert max-w-none" id="docs-content">
                <p class="text-slate-500">Select a document from the sidebar to begin.</p>
            </article>
        </div>
        """


    @expose_web_page(page_id="dashboard", title="Dashboard", icon="fa-gauge-high", priority=-100)
    def dashboard_page(self, language: str = "en"):
        return """
        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6" id="dashboard-stats">
            <!-- Stats will be loaded here by custom.js -->
            <div class="glass-panel p-6 rounded-xl border border-white/5">
                <p class="text-slate-500 text-[10px] uppercase font-bold tracking-wider mb-1">Total Requests</p>
                <div class="text-2xl font-bold text-white mb-2" id="stat-total-requests">0</div>
                <div class="text-emerald-400 text-[10px]"><i class="fa-solid fa-arrow-up mr-1"></i> Live</div>
            </div>
            <div class="glass-panel p-6 rounded-xl border border-white/5">
                <p class="text-slate-500 text-[10px] uppercase font-bold tracking-wider mb-1">Avg Latency</p>
                <div class="text-2xl font-bold text-white mb-2" id="stat-avg-latency">0ms</div>
                <div class="text-blue-400 text-[10px]"><i class="fa-solid fa-bolt mr-1"></i> Real-time</div>
            </div>
            <div class="glass-panel p-6 rounded-xl border border-white/5">
                <p class="text-slate-500 text-[10px] uppercase font-bold tracking-wider mb-1">Success Rate</p>
                <div class="text-2xl font-bold text-white mb-2" id="stat-success-rate">0%</div>
                <div class="text-blue-400 text-[10px]"><i class="fa-solid fa-check-circle mr-1"></i> Verified</div>
            </div>
            <div class="glass-panel p-6 rounded-xl border border-white/5">
                <p class="text-slate-500 text-[10px] uppercase font-bold tracking-wider mb-1">Active Tasks</p>
                <div class="text-2xl font-bold text-white mb-2" id="stat-active-tasks">0</div>
                <div class="text-purple-400 text-[10px]"><i class="fa-solid fa-tasks mr-1"></i> Background</div>
            </div>
        </div>

        <div class="grid lg:grid-cols-2 gap-8 mt-8">
            <div class="glass-panel p-6 rounded-2xl border border-white/5">
                <div class="flex items-center justify-between mb-6">
                    <h3 class="text-lg font-bold text-white flex items-center gap-2">
                        <i class="fa-solid fa-terminal text-blue-400"></i> System Activity
                    </h3>
                    <select id="log-filter-level" onchange="fetchDashboardLogs()" class="bg-white/5 border border-white/10 text-slate-300 text-xs rounded-lg px-2 py-1 outline-none">
                        <option value="INFO">INFO</option>
                        <option value="WARNING">WARNING</option>
                        <option value="ERROR">ERROR</option>
                        <option value="DEBUG">DEBUG</option>
                    </select>
                </div>
                <div id="dashboard-logs" class="h-80 overflow-y-auto space-y-1 font-mono text-[11px]">
                    <div class="text-slate-500 py-4 text-center">Loading logs...</div>
                </div>
            </div>

            <div class="glass-panel p-6 rounded-2xl border border-white/5">
                 <h3 class="text-lg font-bold text-white mb-6 flex items-center gap-2">
                    <i class="fa-solid fa-list-check text-purple-400"></i> Background Tasks
                </h3>
                <div id="dashboard-tasks" class="h-80 overflow-y-auto space-y-3">
                    <div class="text-slate-500 py-4 text-center">No active tasks</div>
                </div>
            </div>
        </div>
        """

    def register_web_page(self, config: Dict[str, Any], provider: Callable[[], Any]):
        if self.web_module:
            self.web_module.register_web_page(config, provider)
        else:
            logger.error("WebService: Cannot register web page, WebModule not available")

    def register_static_provider(self, prefix: str, provider: Callable[[], List[str]]):
        if self.web_module:
            self.web_module.register_static_provider(prefix, provider)
        else:
            logger.error("WebService: Cannot register static provider, WebModule not available")

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

        from starlette.responses import RedirectResponse

        @self.router.get("", include_in_schema=False)
        async def redirect_web_root(request: Request):
            # Force trailing slash for relative assets to work
            path = request.url.path
            if not path.endswith("/"):
                return RedirectResponse(url=path + "/")
            # If already has slash, serve index (should be handled by @router.get("/"))
            return await read_extension_root()

        @self.router.get("/", include_in_schema=False)
        async def read_extension_root():
            # Redirect root to website
            website_index = os.path.join(
                os.path.dirname(__file__), "static", "website", "index.html"
            )
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
            dashboard_index = os.path.join(
                os.path.dirname(__file__), "static", "dashboard", "index.html"
            )
            if os.path.exists(dashboard_index):
                return await self.serve_file(dashboard_index)
            return HTMLResponse("Dashboard Not Found", status_code=404)

        @self.router.get("/dashboard/processes/")
        async def read_processes_page():
            processes_index = os.path.join(
                os.path.dirname(__file__), "static", "dashboard", "processes.html"
            )
            if os.path.exists(processes_index):
                return await self.serve_file(processes_index)
            return HTMLResponse("Processes Page Not Found", status_code=404)

        @self.router.get("/health", tags=["Web Health"])
        async def health_check():
            return {"status": "ok"}

        @self.router.get("/config/pages", response_class=JSONResponse)
        async def get_web_pages_config(
            request: Request,
            language: str = Query("en"),
            authorization: Optional[str] = Header(None)
        ):
            """Returns the list of registered web pages for the frontend navigation, filtered by role."""
            if not self.web_module:
                 return []
            
            # Extract roles from Token if provided
            user_roles: List[str] = []
            if authorization and authorization.startswith("Bearer "):
                token = authorization.removeprefix("Bearer ")
                try:
                    from dynastore.modules.apikey.interfaces import IdentityProviderProtocol
                    providers = get_protocols(IdentityProviderProtocol)
                    for provider in providers:
                        try:
                            user_info = await provider.get_user_info(token)
                            if user_info and "roles" in user_info:
                                user_roles = [str(r) for r in user_info["roles"]]
                                break
                        except Exception:
                            continue
                except Exception as e:
                    logger.debug(f"Failed to extract roles from token: {e}")

            pages = await self.web_module.get_web_pages_config(language)
            results = []
            for page in pages:
                roles = page.get("required_roles")
                if not roles or "anonymous" in roles:
                    results.append(page)
                elif user_roles and "sysadmin" in user_roles:
                    results.append(page)
                elif user_roles and any(r in user_roles for r in roles):
                    results.append(page)

            results.sort(key=lambda x: x.get("priority", 0))
            return results


        @self.router.get("/pages/{page_id}", response_class=HTMLResponse)
        async def get_web_page_content(
            page_id: str, request: Request, language: str = Query("en")
        ):
            if not self.web_module:
                raise HTTPException(status_code=500, detail="Web module not available")
            
            content = await self.web_module.get_web_page_content(page_id, request, language)
            if content is None:
                raise HTTPException(status_code=404, detail="Page not found")
            
            return HTMLResponse(content=content)

        # ------------------------------------------------------------------ #
        #  Demo Data Management (sysadmin only)                               #
        # ------------------------------------------------------------------ #

        DEMO_CATALOG_ID = "demo_catalog"
        DEMO_COLLECTION_ID = "demo_collection"

        def _require_sysadmin(request: Request):
            principal = getattr(request.state, "principal", None)
            if not principal or "sysadmin" not in (principal.roles or []):
                raise HTTPException(status_code=403, detail="Requires sysadmin role")

        @self.router.post("/admin/demo/populate", response_class=JSONResponse, tags=["Admin"])
        async def demo_populate(request: Request):
            """Provision demo catalog, collection and sample items (sysadmin only)."""
            from dynastore.models.protocols import CatalogsProtocol as _CatProt
            _require_sysadmin(request)
            cats = get_protocol(_CatProt)
            if not cats:
                raise HTTPException(status_code=500, detail="Catalog service not available")
            try:
                await cats.delete_catalog(DEMO_CATALOG_ID, force=True)
            except Exception:
                pass
            await cats.create_catalog({
                "id": DEMO_CATALOG_ID,
                "title": {"en": "Demo Catalog", "it": "Catalogo Demo"},
                "description": {"en": "Demo catalog for testing purposes.", "it": "Catalogo demo per scopi di test."},
                "keywords": ["demo", "dynastore", "geospatial"],
                "license": "CC-BY-4.0",
            }, lang="*")
            await cats.create_collection(DEMO_CATALOG_ID, {
                "id": DEMO_COLLECTION_ID,
                "title": {"en": "Italy Tile Grid"},
                "description": {"en": "A 2×3 grid of map-tile polygons covering the Italian peninsula."},
                "type": "Feature",
            }, lang="*")
            # 2 columns × 3 rows covering Italy's bounding box
            # lon: 6.6 – 18.5  (col width ≈ 5.95°)
            # lat: 37.9 – 47.1 (row height ≈ 3.07°)
            def _tile_polygon(col: int, row: int) -> dict:
                lon0 = 6.6  + col * 5.95
                lon1 = lon0 + 5.95
                lat0 = 37.9 + row * 3.07
                lat1 = lat0 + 3.07
                return {
                    "type": "Polygon",
                    "coordinates": [[
                        [lon0, lat0], [lon1, lat0],
                        [lon1, lat1], [lon0, lat1],
                        [lon0, lat0],   # close ring
                    ]],
                }
            _row_labels = ["south", "centre", "north"]
            _col_labels = ["west", "east"]
            demo_items = [
                {
                    "id": f"tile_{_col_labels[c]}_{_row_labels[r]}",
                    "type": "Feature",
                    "geometry": _tile_polygon(c, r),
                    "properties": {
                        "name": f"Italy – {_row_labels[r].capitalize()} {_col_labels[c].capitalize()}",
                        "description": f"Map tile column {c} row {r} over Italy",
                        "col": c, "row": r,
                    },
                }
                for r in range(3) for c in range(2)
            ]
            result = await cats.upsert(DEMO_CATALOG_ID, DEMO_COLLECTION_ID, demo_items)
            logger.info(f"Demo data provisioned: {len(result)} items in '{DEMO_CATALOG_ID}'")
            return {"status": "ok", "catalog_id": DEMO_CATALOG_ID,
                    "collection_id": DEMO_COLLECTION_ID, "items": len(result)}

        @self.router.post("/admin/demo/cleanup", response_class=JSONResponse, tags=["Admin"])
        async def demo_cleanup(request: Request):
            """Delete only the demo collection and catalog, leaving all other data intact (sysadmin only)."""
            from dynastore.models.protocols import CatalogsProtocol as _CatProt
            _require_sysadmin(request)
            cats = get_protocol(_CatProt)
            if not cats:
                raise HTTPException(status_code=500, detail="Catalog service not available")
            deleted = []
            errors = []
            # Delete the demo collection (cascades to its items) first
            try:
                await cats.delete_collection(DEMO_CATALOG_ID, DEMO_COLLECTION_ID, force=True)
                deleted.append(f"{DEMO_CATALOG_ID}/{DEMO_COLLECTION_ID}")
                logger.info(f"Demo collection '{DEMO_COLLECTION_ID}' deleted from '{DEMO_CATALOG_ID}'.")
            except Exception as e:
                errors.append(f"collection: {e}")
                logger.warning(f"Demo cleanup — collection: {e}")
            # Then delete the catalog shell (no force needed; collection is already gone)
            try:
                await cats.delete_catalog(DEMO_CATALOG_ID)
                deleted.append(DEMO_CATALOG_ID)
                logger.info(f"Demo catalog '{DEMO_CATALOG_ID}' deleted.")
            except Exception as e:
                errors.append(f"catalog: {e}")
                logger.warning(f"Demo cleanup — catalog: {e}")
            return {"status": "ok" if deleted else "not_found", "deleted": deleted, "errors": errors}

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

                buckets[cat].append(
                    {
                        "id": doc_item["id"],
                        "title": doc_item["title"],
                        "path": doc_item["path"],
                    }
                )

            # Create the final manifest using raw category IDs as keys
            manifest = {}
            for cat, items in buckets.items():
                if not items:
                    continue

                # Sort each category alphabetically by title
                items.sort(key=lambda x: x["title"])

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
                logger.error(
                    "Markdown library not installed. Documentation rendering unavailable."
                )
                return HTMLResponse(
                    "<h1>Documentation renderer (markdown) not installed</h1>",
                    status_code=500,
                )

            try:
                with open(doc_item["path"], "r", encoding="utf-8") as f:
                    md_content = f.read()

                # Convert to HTML
                html_content = markdown.markdown(
                    md_content,
                    extensions=["fenced_code", "tables", "def_list", "nl2br"],
                )
                return HTMLResponse(content=html_content)
            except Exception as e:
                logger.error(f"Error reading doc {doc_id}: {e}")
                raise HTTPException(status_code=500, detail="Error reading document")

        @self.router.get("/dashboard/catalogs", response_class=JSONResponse)
        async def get_dashboard_catalogs(
            request: Request,
            q: Optional[str] = Query(None, description="Search query"),
            limit: int = Query(100, ge=1, le=1000),
            offset: int = Query(0, ge=0),
            authorization: Optional[str] = Header(None),
        ):
            """
            List catalogs visible to the caller.

            - sysadmin  → all catalogs
            - authenticated non-sysadmin → only catalogs where the principal
              holds an admin role (principal_id filter forwarded to the
              CatalogsProtocol when supported)
            - anonymous → empty list
            """
            from dynastore.models.protocols import CatalogsProtocol

            # Resolve caller identity
            user_roles: List[str] = []
            principal_id: Optional[str] = None
            if authorization and authorization.startswith("Bearer "):
                token = authorization[7:]
                # Fast path: system admin key (env var or DB-stored key)
                try:
                    from dynastore.models.protocols.apikey import ApiKeyProtocol
                    apikey_svc = get_protocol(ApiKeyProtocol)
                    if apikey_svc:
                        system_key = await apikey_svc.get_system_admin_key()
                        if token == system_key:
                            user_roles = ["sysadmin"]
                except Exception:
                    pass
                if "sysadmin" not in user_roles:
                    try:
                        from dynastore.modules.apikey.interfaces import IdentityProviderProtocol
                        for idp in get_protocols(IdentityProviderProtocol):
                            try:
                                info = await idp.get_user_info(token)
                                if info:
                                    user_roles = info.get("roles", [])
                                    principal_id = info.get("subject_id") or info.get("principal_id")
                                    break
                            except Exception:
                                continue
                    except Exception as e:
                        logger.debug(f"Dashboard catalogs: could not resolve identity: {e}")

            catalogs_provider: CatalogsProtocol = get_protocol(CatalogsProtocol)
            if not catalogs_provider:
                return []

            if "sysadmin" in user_roles:
                # Sysadmin sees every catalog
                cats = await catalogs_provider.list_catalogs(limit=limit, offset=offset, q=q)
            elif principal_id:
                # Authenticated non-sysadmin: forward principal filter so the
                # protocol can restrict to catalogs the caller administers.
                try:
                    cats = await catalogs_provider.list_catalogs(
                        limit=limit, offset=offset, q=q, principal_id=principal_id
                    )
                except TypeError:
                    # Protocol implementation does not support principal_id filter yet
                    cats = await catalogs_provider.list_catalogs(limit=limit, offset=offset, q=q)
            else:
                return []

            return [c.model_dump() for c in cats]

        @self.router.get(
            "/dashboard/catalogs/{catalog_id}/collections", response_class=JSONResponse
        )
        async def get_dashboard_collections(
            catalog_id: str,
            q: Optional[str] = Query(None, description="Search query"),
            limit: int = Query(100, ge=1, le=1000),
            offset: int = Query(0, ge=0),
        ):
            from dynastore.models.protocols import CollectionsProtocol
            from dynastore.tools.discovery import get_protocol, register_plugin

            collections_provider: CollectionsProtocol = get_protocol(
                CollectionsProtocol
            )
            if collections_provider:
                try:
                    cols = await collections_provider.list_collections(
                        catalog_id=catalog_id, limit=limit, offset=offset, q=q
                    )
                    return [c.model_dump() for c in cols]
                except (ValueError, KeyError):
                    return []
            return []

        @self.router.get("/dashboard/stats", response_class=JSONResponse)
        async def get_dashboard_stats(
            catalog_id: str = Query(
                "_system_", description="Catalog ID to filter stats for."
            ),
            collection_id: Optional[str] = Query(
                None, description="Optional collection ID to filter stats for."
            ),
            principal_id: Optional[str] = Query(
                None, description="Filter by Principal ID."
            ),
            api_key_hash: Optional[str] = Query(
                None, description="Filter by API Key Hash."
            ),
            start_date: Optional[datetime] = Query(
                None, description="Start date for stats aggregation."
            ),
            end_date: Optional[datetime] = Query(
                None, description="End date for stats aggregation."
            ),
            request: Request = None,
        ):
            # Resolve schema using CatalogsProtocol
            from dynastore.modules import get_protocol
            from dynastore.models.protocols import CatalogsProtocol

            catalogs = get_protocol(CatalogsProtocol)
            db_resource = catalogs.engine if catalogs else None

            schema = "catalog"
            if catalog_id and catalog_id != "_system_":
                try:
                    schema = (
                        await catalogs.resolve_physical_schema(
                            catalog_id, db_resource=db_resource
                        )
                        or "catalog"
                    )
                except ValueError:
                    pass

            from dynastore.models.protocols.stats import StatsProtocol
            stats_service = get_protocol(StatsProtocol)
            summary = None
            if stats_service:
                summary = await stats_service.get_summary(
                    schema=schema,
                    catalog_id=catalog_id if catalog_id != "_system_" else None,
                    collection_id=collection_id,
                    principal_id=principal_id,
                    api_key_hash=api_key_hash,
                    start_date=start_date,
                    end_date=end_date,
                )

            return (
                summary.model_dump()
                if summary
                else {"total_requests": 0, "average_latency_ms": 0}
            )

        @self.router.get("/dashboard/tasks", response_class=JSONResponse)
        async def get_dashboard_tasks():
            tasks_ext = getattr(self.app.state, "tasks", None)
            if tasks_ext:
                tasks = await tasks_ext.get_tasks()
                return tasks
            return []

        @self.router.get("/dashboard/logs", response_class=JSONResponse)
        async def get_dashboard_logs(
            catalog_id: str = Query(
                "_system_",
                description="Catalog ID to filter logs for. Defaults to system logs.",
            ),
            collection_id: Optional[str] = Query(
                None, description="Optional collection ID to filter logs for."
            ),
            event_type: Optional[str] = Query(
                None, description="Optional event type to filter logs for."
            ),
            level: Optional[str] = Query(
                None, description="Optional log level (e.g., ERROR, INFO)."
            ),
            limit: int = Query(
                50, ge=1, le=1000, description="Number of logs to return."
            ),
            offset: int = Query(0, ge=0, description="Pagination offset."),
        ):
            from dynastore.models.protocols.logs import LogsProtocol
            from dynastore.tools.discovery import get_protocol, register_plugin

            log_ext = get_protocol(LogsProtocol)
            if log_ext:
                logs = await log_ext.list_logs(
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    event_type=event_type,
                    level=level,
                    limit=limit,
                    offset=offset,
                )
                return [l if isinstance(l, dict) else l.model_dump() for l in logs]
            return []

        @self.router.get("/dashboard/events", response_class=JSONResponse)
        async def get_dashboard_events(
            catalog_id: str = Query(..., description="Catalog ID to fetch events for."),
            collection_id: Optional[str] = Query(
                None, description="Optional collection ID to filter events for."
            ),
            event_type: Optional[str] = Query(
                None, description="Optional event type to filter events for."
            ),
            limit: int = Query(
                50, ge=1, le=1000, description="Number of events to return."
            ),
            offset: int = Query(0, ge=0, description="Pagination offset."),
        ):
            from dynastore.modules.catalog.catalog_module import _module_instance

            catalog_mod = _module_instance
            if catalog_mod and hasattr(catalog_mod, "event_service"):
                from dynastore.tools.protocol_helpers import get_engine

                engine = get_engine()
                events = await catalog_mod.event_service.search_events(
                    engine=engine,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    event_type=event_type,
                    limit=limit,
                    offset=offset,
                )

                # Convert datetime objects to string for JSON serialization
                for event in events:
                    if "created_at" in event and event["created_at"]:
                        event["created_at"] = event["created_at"].isoformat()

                return events
            return []

        @self.router.get("/{prefix}/{filename:path}", include_in_schema=False)
        async def serve_static_content(prefix: str, filename: str):
            # Resolve the provider callable for this prefix
            provider_callable = None

            if self.web_module and prefix in self.web_module.static_providers:
                provider_callable = self.web_module.static_providers[prefix]
            elif prefix == "static":
                # Fallback: serve directly from the static directory if not registered
                provider_callable = self._provide_default_static
            
            if provider_callable is None:
                raise HTTPException(
                    status_code=404, detail=f"No provider found for prefix '{prefix}'"
                )

            try:
                if isinstance(provider_callable, StaticFilesProtocol):
                    allowed_files = await provider_callable.list_static_files()
                else:
                    # Legacy: provider is a callable returning absolute paths
                    allowed_files = provider_callable()
            except Exception as e:
                logger.error(f"Static provider for '{prefix}' raised an error: {e}", exc_info=True)
                raise HTTPException(status_code=500, detail=f"Provider error: {e}")

            if not allowed_files:
                # If the provider returned an empty list, the file doesn't exist under this prefix
                raise HTTPException(status_code=404, detail="File not found")
            
            # Build a relative-path -> absolute-path map using the common root of all files
            try:
                dirs = [os.path.dirname(f) for f in allowed_files]
                common_root = os.path.commonpath(dirs) if len(dirs) > 1 else dirs[0]
            except ValueError as e:
                logger.error(f"commonpath failed for prefix '{prefix}': {e}")
                raise HTTPException(status_code=500, detail="Static file layout error")

            allowed_map = {
                os.path.relpath(f, common_root).replace(os.sep, "/"): f 
                for f in allowed_files
            }
            
            lookup_key = filename.replace(os.sep, "/")
            target_file = allowed_map.get(lookup_key)

            if not target_file:
                logger.warning(
                    f"File '{filename}' not found in allowlist for prefix '{prefix}'. "
                    f"Available: {list(itertools.islice(allowed_map.keys(), 10))}"
                )
                raise HTTPException(status_code=404, detail="File not found")

            return await self.serve_file(target_file)
