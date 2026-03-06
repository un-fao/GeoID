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
import inspect
import os
import re
from pathlib import Path
from typing import Dict, Any, Callable, List, Optional, Union
from contextlib import asynccontextmanager

from dynastore.tools.language_utils import resolve_localized_field
from .models import WebPageConfig

logger = logging.getLogger(__name__)


class WebModule:
    """
    The WebModule acts as the central registry and logic provider for
    Dynastore's web interface capabilities. It manages:
    - Registered Web Pages (from extensions)
    - Static Content Providers
    - Documentation Discovery
    - Localization resolution
    """

    def __init__(self):
        # Registry for pluggable web pages: { page_id: {config, handler} }
        self.web_pages: Dict[str, Dict[str, Any]] = {}

        # Registry for static content providers: { prefix: provider_callable }
        self.static_providers: Dict[str, Callable[[], List[str]]] = {}

        # Documentation registry
        self.docs_registry: Dict[str, Dict[str, Any]] = {}

        # Application directories for scanning
        self.app_dirs: List[str] = []
        self.project_root: Optional[str] = None

    @asynccontextmanager
    async def lifespan(self):
        """Module lifecycle management."""
        self._initialize_discovery()
        yield

    def _initialize_discovery(self):
        """Initialize project structure discovery."""
        self.project_root = self._find_project_root(
            __file__, ["setup.py", "pyproject.toml", "main.py"]
        )
        if self.project_root:
            self.app_dirs = self._discover_app_dirs(self.project_root)
            self.docs_registry = self._scan_for_documentation()
        else:
            logger.warning(
                "WebModule: Could not find project root. Docs scanning will be limited."
            )

    def register_web_page(self, config: Dict[str, Any], provider: Callable[[], Any]):
        """Registers a web page handler."""
        page_id = config["id"]
        if page_id in self.web_pages:
            logger.warning(f"WebModule: Overwriting web page '{page_id}'")

        self.web_pages[page_id] = {
            "config": WebPageConfig(**config),  # Validate with model
            "handler": provider,
        }
        logger.info(f"WebModule: Registered web page '{page_id}'")

    def register_static_provider(self, prefix: str, provider: Callable[[], List[str]]):
        """Registers a static file provider."""
        if prefix in self.static_providers:
            logger.warning(
                f"WebModule: Overwriting static provider for prefix '{prefix}'"
            )
        self.static_providers[prefix] = provider
        logger.info(f"WebModule: Registered static provider for prefix '{prefix}'")

    def scan_and_register_providers(self, instance: Any):
        """Scans an object instance for decorated methods (@expose_web_page, @expose_static)."""
        for name, method in inspect.getmembers(instance, predicate=inspect.ismethod):
            if hasattr(method, "_web_static_prefix"):
                prefix = getattr(method, "_web_static_prefix").strip("/")
                self.register_static_provider(prefix, method)

            if hasattr(method, "_web_page_config"):
                config = getattr(method, "_web_page_config")
                self.register_web_page(config, method)

    def get_web_pages_config(self, language: str = "en") -> List[Dict[str, Any]]:
        """
        Returns the list of registered web pages with localized titles and descriptions.
        """
        results = []
        for p in self.web_pages.values():
            config: WebPageConfig = p["config"]

            title = resolve_localized_field(config.title, language)
            description = resolve_localized_field(config.description, language)

            results.append(
                {
                    "id": config.id,
                    "title": title,
                    "icon": config.icon,
                    "description": description,
                }
            )
        return results

    def get_docs_manifest(self) -> Dict[str, List[Dict[str, str]]]:
        """Returns the organized documentation manifest."""
        buckets = {"root": [], "modules": [], "extensions": [], "tasks": []}

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

        manifest = {}
        for cat, items in buckets.items():
            if items:
                items.sort(key=lambda x: x["title"])
                manifest[cat] = items

        return manifest

    def get_doc_path(self, doc_id: str) -> Optional[str]:
        """Resolves a doc_id to a file path."""
        item = self.docs_registry.get(doc_id)
        return item["path"] if item else None

    # --- Internal Logic (Moved from web extension) ---

    def _find_project_root(self, start_path: str, markers: List[str]) -> Optional[str]:
        """Walks up from start_path to find a directory containing one of the marker files."""
        try:
            start = Path(start_path).resolve()
            if start.is_file():
                start = start.parent

            for current_path in [start, *start.parents]:
                for marker in markers:
                    marker_path = current_path / marker
                    if marker_path.exists():
                        if marker.lower() in ("pyproject.toml", "setup.py"):
                            return str(current_path)
                        if self._has_app_structure(current_path):
                            return str(current_path)
            return None
        except Exception as e:
            logger.debug(f"Error resolving project root: {e}")
            return None

    def _has_app_structure(self, root_dir: Path) -> bool:
        """Helper to verify if a directory looks like a Dynastore app."""
        comps = ["modules", "extensions", "tasks"]
        # Check direct children
        for comp in comps:
            if (root_dir / comp).is_dir():
                return True
        # Check src directory
        src_dir = root_dir / "src"
        if src_dir.is_dir():
            for child in src_dir.iterdir():
                if child.is_dir():
                    for comp in comps:
                        if (child / comp).is_dir():
                            return True
        return False

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

        # Deduplicate
        seen = set()
        result: List[str] = []
        for p in candidates:
            if p not in seen:
                seen.add(p)
                result.append(p)
        return result

    def _scan_for_documentation(self) -> Dict[str, Dict[str, Any]]:
        """Scans for documentation recursively."""
        registry = {}

        def process_doc_file(
            fpath: Path,
            category: str,
            component_name: Optional[str],
            root_reference: Path,
        ):
            try:
                # ID Generation Logic
                id_parts = []
                if category != "root":
                    id_parts.append(category)

                if component_name:
                    id_parts.append(component_name)

                is_readme = fpath.stem.lower() == "readme"
                if not is_readme:
                    stem = fpath.stem.lower()
                    if component_name and stem.startswith(component_name):
                        clean_stem = stem[len(component_name) :].lstrip("_-")
                        if clean_stem:
                            id_parts.append(clean_stem)
                    else:
                        id_parts.append(stem)

                if not id_parts:
                    id_parts.append(fpath.stem.lower())

                doc_id = ":".join(id_parts)
                final_id = doc_id
                counter = 1
                while final_id in registry:
                    final_id = f"{doc_id}_{counter}"
                    counter += 1
                doc_id = final_id

                # Title Extraction Logic
                if is_readme:
                    title = fpath.parent.name.replace("_", " ").title()
                else:
                    title = fpath.stem.replace("_", " ").title()

                try:
                    with open(fpath, "r", encoding="utf-8", errors="ignore") as f:
                        for _ in range(50):
                            line = f.readline()
                            if not line:
                                break
                            stripped = line.strip()
                            if stripped.startswith("#"):
                                raw_title = stripped.lstrip("#").strip()
                                if raw_title:
                                    clean_title = re.sub(
                                        r"\[([^\]]+)\]\([^)]+\)", r"\1", raw_title
                                    )
                                    title = re.sub(r"[*_`]", "", clean_title).strip()
                                    break
                except Exception:
                    pass

                if component_name and category != "root":
                    comp_title = component_name.replace("_", " ").title()
                    if (
                        title.lower() != comp_title.lower()
                        and not title.lower().startswith(comp_title.lower())
                    ):
                        title = f"{comp_title}: {title}"

                registry[doc_id] = {
                    "id": doc_id,
                    "title": title,
                    "path": str(fpath.resolve()),
                    "category": category,
                }
            except Exception as e:
                logger.error(f"Error processing doc file {fpath}: {e}")

        # Scanning
        if self.project_root:
            root_path = Path(self.project_root)
            for item in root_path.glob("*.md"):
                process_doc_file(item, "root", None, root_path)

            docs_folder = root_path / "docs"
            if docs_folder.is_dir():
                for item in docs_folder.glob("*.md"):
                    process_doc_file(item, "root", None, docs_folder)

            categories = ["modules", "extensions", "tasks"]
            for app_dir_str in self.app_dirs:
                app_path = Path(app_dir_str)
                app_docs = app_path / "docs"
                if app_docs.is_dir():
                    for item in app_docs.glob("*.md"):
                        process_doc_file(item, "root", None, app_docs)

                for cat in categories:
                    cat_dir = app_path / cat
                    if not cat_dir.is_dir():
                        continue
                    for component_dir in cat_dir.iterdir():
                        if not component_dir.is_dir():
                            continue
                        for root, dirs, files in os.walk(component_dir):
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

        return registry
