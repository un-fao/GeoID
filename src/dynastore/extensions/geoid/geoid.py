import os
import logging
import itertools
from typing import List, Any, Dict, Optional, Callable

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from dynastore.extensions import ExtensionProtocol
from dynastore.models.protocols.web import WebOverrideProtocol, WebPageProtocol, StaticFilesProtocol

# Use the module-level decorators so Web discovers pages and static files automatically
from dynastore.modules.web.decorators import expose_static, expose_web_page

import user_agents  # Enforces installation-driven discovery

logger = logging.getLogger(__name__)


class Geoid(ExtensionProtocol, WebOverrideProtocol, WebPageProtocol, StaticFilesProtocol):
    """
    GeoID extension — a content provider that plugs into the Web extension's
    routing infrastructure.

    Responsibilities:
    - Override the platform home section with GeoID branding (is_embed page).
    - Provide the /web/pages/geoid service page.
    - Serve geoid static assets under the "geoid" prefix.
    - Declare style overrides for landing-page rebranding.

    Everything else (middleware, root redirects, /web/config/pages, /web/docs-*,
    /web/health, /{prefix}/{filename} static dispatch) is handled by the Web
    extension, which discovers this class through the StaticFilesProtocol,
    WebPageProtocol and WebOverrideProtocol at configure_app time.
    """

    priority: int = -10  # Load before Web so policies are ready

    def __init__(self, app: FastAPI):
        self.app = app

        self.static_dir = os.path.join(os.path.dirname(__file__), "static")

    # ------------------------------------------------------------------ #
    #  StaticFilesProtocol                                                 #
    # ------------------------------------------------------------------ #

    def get_static_prefix(self) -> str:
        """Prefix under which geoid static files are served: /web/geoid/<filename>."""
        return "geoid"

    async def is_file_provided(self, path: str) -> bool:
        """Return True if *path* (relative to the geoid static dir) exists on disk."""
        if not self.static_dir:
            return False
        full_path = os.path.join(self.static_dir, path.lstrip("/"))
        return os.path.isfile(full_path)

    async def list_static_files(
        self, query: Optional[str] = None, limit: int = 100, offset: int = 0
    ) -> List[str]:
        """Return absolute paths of geoid static files, optionally filtered and paginated."""
        if not self.static_dir or not os.path.isdir(self.static_dir):
            return []

        files: List[str] = []
        for root, _, filenames in os.walk(self.static_dir):
            for filename in filenames:
                full_path = os.path.join(root, filename)
                rel_path = os.path.relpath(full_path, self.static_dir)
                if query and query.lower() not in rel_path.lower():
                    continue
                files.append(full_path)

        files.sort()
        return list(itertools.islice(files, offset, offset + limit))

    # Static files are registered via the StaticFilesProtocol methods above
    # (get_static_prefix / list_static_files / is_file_provided), discovered by
    # WebModule's global protocol scan.  No @expose_static decorator needed here.

    # ------------------------------------------------------------------ #
    #  WebPageProtocol                                                     #
    # ------------------------------------------------------------------ #

    def get_web_page_config(self) -> Dict[str, Any]:
        """
        Metadata for the Geoid service page, used by WebModule for navigation
        and role-based visibility.  Served at /web/pages/geoid via render_page().
        """
        return {
            "id": "geoid",
            "title": {
                "en": "Geoid Service",
                "es": "Servicio de Geoide",
                "fr": "Service géoïde",
            },
            "icon": "fa-earth-americas",
            "description": {
                "en": "Geoid height calculation service.",
                "es": "Cálculo de altura del geoide.",
                "fr": "Calcul de la hauteur du géoïde.",
            },
            "priority": 0,
            "is_embed": False,
        }

    async def render_page(self, request: Any, language: str = "en") -> Any:
        """
        Called by Web's /web/pages/geoid handler.  Delegates to geoid_page()
        so all rendering logic lives in one place.
        """
        return self.geoid_page(language=language)

    def geoid_page(self, language: str = "en") -> HTMLResponse:
        """
        Serve the language-specific Geoid SPA entry point.

        Lookup order:
          1. static/index_{lang}.html  (e.g. index_es.html)
          2. static/index.html         (language-neutral fallback)
          3. 404 response if neither exists
        """
        lang_code = language.lower().split("-")[0]
        if lang_code not in ["en", "es", "fr"]:
            lang_code = "en"

        index_path = os.path.join(self.static_dir, f"index_{lang_code}.html")
        if not os.path.exists(index_path):
            index_path = os.path.join(self.static_dir, "index.html")

        if os.path.exists(index_path):
            with open(index_path, "r", encoding="utf-8") as f:
                return HTMLResponse(content=f.read())
        return HTMLResponse("Geoid service page not found", status_code=404)

    # ------------------------------------------------------------------ #
    #  WebOverrideProtocol                                                 #
    # ------------------------------------------------------------------ #

    def get_landing_page_override(self) -> Optional[Dict[str, Any]]:
        """
        Rebrand the platform landing page with GeoID / UN-FAO identity.
        Consumed by WebModule.get_landing_page_override() aggregators.
        """
        return {
            "title": {
                "en": "GeoID - UN-FAO",
                "es": "GeoID - UN-FAO",
                "fr": "GeoID - UN-FAO",
            },
            "description": {
                "en": "Global Geoid Height Service",
                "es": "Servicio Global de Altura del Geoide",
                "fr": "Service mondial de hauteur du géoïde",
            },
            "icon": "fa-globe",
        }

    def get_style_overrides(self) -> List[str]:
        """CSS paths injected into the platform shell for GeoID branding."""
        return ["/web/geoid/static/custom_style.css"]

    def get_component_overrides(self) -> Dict[str, Callable]:
        """Reserved for future UI component substitutions; currently empty."""
        return {}

    # ------------------------------------------------------------------ #
    #  Home embed — injected into the Web platform's home page            #
    # ------------------------------------------------------------------ #

    @expose_web_page(
        page_id="geoid_home",   # unique id — NOT "home"
        title="GeoID Home",
        icon="fa-home",
        priority=-500,
        section="home",         # injects content into the home page via section mechanism
        is_embed=True,          # not a standalone nav item
    )
    async def geoid_home_page(self, language: str = "en") -> str:
        """
        GeoID-branded content embedded in the platform home page.

        Uses section="home" so WebModule registers this provider both under its
        own page_id ("geoid_home") AND as an embed provider for the "home" page.
        This avoids overwriting the home page's navigation config while still
        injecting content when /web/pages/home is rendered.
        """
        lang = language.lower().split("-")[0]

        titles = {
            "en": "GeoID — UN-FAO",
            "es": "GeoID — UN-FAO",
            "fr": "GeoID — UN-FAO",
        }
        subtitles = {
            "en": "Global Federated Places Service",
            "es": "Servicio Global de Lugares Federados",
            "fr": "Service mondial de lieux fédérés",
        }
        descriptions = {
            "en": "A Digital Public Good to manage, store, retrieve and share billions of geospatial locations — OGC-compliant, open-source, freely replicable.",
            "es": "Un Bien Público Digital para gestionar, almacenar, recuperar y compartir miles de millones de ubicaciones geoespaciales.",
            "fr": "Un bien public numérique pour gérer, stocker, récupérer et partager des milliards de localisations géospatiales.",
        }

        title = titles.get(lang, titles["en"])
        subtitle = subtitles.get(lang, subtitles["en"])
        desc = descriptions.get(lang, descriptions["en"])

        explore_label = {"en": "Explore Places", "es": "Explorar Lugares", "fr": "Explorer les lieux"}.get(lang, "Explore Places")
        launch_label = {"en": "Launch Geoid Tool", "es": "Iniciar Herramienta", "fr": "Lancer l'outil"}.get(lang, "Launch Geoid Tool")

        phase1_label = {"en": "Phase 1 — Anonymous Places Service", "es": "Fase 1 — Servicio Anónimo de Lugares", "fr": "Phase 1 — Service de lieux anonyme"}.get(lang, "Phase 1 — Anonymous Places Service")
        phase2_label = {"en": "Phase 2 — Identity, Authority &amp; Expanded Services", "es": "Fase 2 — Identidad, Autoridad y Servicios Ampliados", "fr": "Phase 2 — Identité, Autorité et Services Étendus"}.get(lang, "Phase 2 — Identity, Authority &amp; Expanded Services")

        return f"""
        <script>
            document.getElementById('home-title').innerText = '{title}';
            document.getElementById('home-subtitle').innerText = '{subtitle}';
            document.getElementById('home-description').innerText = '{desc}';
            document.querySelectorAll('.ds-default-home').forEach(el => el.style.display = 'none');
            const actions = document.getElementById('home-actions');
            if (actions && !document.getElementById('action-geoid')) {{
                const btn = document.createElement('button');
                btn.id = 'action-geoid';
                btn.onclick = () => switchTab('geoid');
                btn.className = 'px-6 py-3 rounded-lg bg-blue-600 hover:bg-blue-500 text-white font-medium shadow-lg shadow-blue-500/20 transition-all';
                btn.innerText = '{explore_label}';
                actions.appendChild(btn);

                const btn2 = document.createElement('button');
                btn2.id = 'action-geoid-tool';
                btn2.onclick = () => switchTab('geoid');
                btn2.className = 'px-6 py-3 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 font-medium border border-white/10 transition-all';
                btn2.innerText = '{launch_label}';
                actions.appendChild(btn2);
            }}
        </script>

        <!-- GeoID Project Overview -->
        <div class="mt-16 space-y-10">

          <!-- Hero card -->
          <div class="glass-panel p-8 rounded-3xl border border-white/5 bg-gradient-to-br from-blue-500/5 to-emerald-500/5">
            <div class="flex flex-col md:flex-row items-center gap-8">
              <div class="w-20 h-20 rounded-2xl bg-blue-500/10 flex items-center justify-center text-blue-400 shrink-0">
                <i class="fa-solid fa-earth-americas text-4xl"></i>
              </div>
              <div class="flex-1">
                <h2 class="text-2xl font-bold text-white mb-2">Global Federated Places Service</h2>
                <p class="text-slate-400 mb-4">
                  GeoID is a <strong class="text-white">Digital Public Good</strong> designed to manage, safely store,
                  retrieve and share billions of geospatial locations (points, areas and lines) efficiently.
                  The core service is open-source, the base place data is open-data and freely copyable,
                  enabling federated collaboration and widespread use.
                </p>
                <div class="flex flex-wrap gap-2">
                  <span class="px-3 py-1 rounded-full bg-blue-500/10 border border-blue-500/20 text-xs text-blue-300">OGC API — Features</span>
                  <span class="px-3 py-1 rounded-full bg-emerald-500/10 border border-emerald-500/20 text-xs text-emerald-300">Apache Iceberg</span>
                  <span class="px-3 py-1 rounded-full bg-purple-500/10 border border-purple-500/20 text-xs text-purple-300">GeoParquet</span>
                  <span class="px-3 py-1 rounded-full bg-yellow-500/10 border border-yellow-500/20 text-xs text-yellow-300">H3 / S2 / Geohash</span>
                  <span class="px-3 py-1 rounded-full bg-slate-700/50 border border-white/10 text-xs text-slate-300">STAC</span>
                  <span class="px-3 py-1 rounded-full bg-slate-700/50 border border-white/10 text-xs text-slate-300">Decentralized Identifiers (DIDs)</span>
                </div>
              </div>
            </div>
          </div>

          <!-- Project Benefits -->
          <div>
            <h3 class="text-lg font-semibold text-slate-300 mb-4 uppercase tracking-wider text-sm">Project Benefits</h3>
            <div class="grid md:grid-cols-3 gap-4">
              <div class="glass-panel p-5 rounded-xl border border-white/5">
                <i class="fa-solid fa-share-nodes text-blue-400 text-xl mb-3"></i>
                <h4 class="font-semibold text-white mb-1">Share Location Data</h4>
                <p class="text-slate-400 text-sm">Easily and efficiently share location data, both privately and publicly, for scientific, analytical, and operational use.</p>
              </div>
              <div class="glass-panel p-5 rounded-xl border border-white/5">
                <i class="fa-solid fa-bolt text-yellow-400 text-xl mb-3"></i>
                <h4 class="font-semibold text-white mb-1">Faster Insights</h4>
                <p class="text-slate-400 text-sm">High-performance access and efficient query tools — analyze geospatial data and make decisions faster with O(log N) spatial indexing.</p>
              </div>
              <div class="glass-panel p-5 rounded-xl border border-white/5">
                <i class="fa-solid fa-shield-halved text-emerald-400 text-xl mb-3"></i>
                <h4 class="font-semibold text-white mb-1">Data Integrity</h4>
                <p class="text-slate-400 text-sm">Long-term, immutable storage with data provenance tracking. Unique URIs and cryptographic hashes for every place.</p>
              </div>
              <div class="glass-panel p-5 rounded-xl border border-white/5">
                <i class="fa-solid fa-users text-purple-400 text-xl mb-3"></i>
                <h4 class="font-semibold text-white mb-1">Drive Collaboration</h4>
                <p class="text-slate-400 text-sm">Open, collaborative platform where data is copyable and reusable, enabling a wide community to contribute and benefit.</p>
              </div>
              <div class="glass-panel p-5 rounded-xl border border-white/5">
                <i class="fa-solid fa-coins text-orange-400 text-xl mb-3"></i>
                <h4 class="font-semibold text-white mb-1">Cut Operational Costs</h4>
                <p class="text-slate-400 text-sm">One common, centralised registry for location data — stop duplicating efforts and systems across projects.</p>
              </div>
              <div class="glass-panel p-5 rounded-xl border border-white/5">
                <i class="fa-solid fa-layer-group text-sky-400 text-xl mb-3"></i>
                <h4 class="font-semibold text-white mb-1">Build for the Future</h4>
                <p class="text-slate-400 text-sm">Cloud-native architecture on open standards, ready to integrate with future advancements in spatial intelligence and federation.</p>
              </div>
            </div>
          </div>

          <!-- Roadmap -->
          <div class="grid md:grid-cols-2 gap-6">
            <!-- Phase 1 -->
            <div class="glass-panel p-6 rounded-2xl border border-blue-500/20">
              <div class="flex items-center gap-3 mb-4">
                <span class="w-8 h-8 rounded-full bg-blue-600 flex items-center justify-center text-white text-sm font-bold">1</span>
                <h3 class="font-semibold text-white">{phase1_label}</h3>
              </div>
              <ul class="space-y-2 text-sm text-slate-400">
                <li class="flex gap-2"><i class="fa-solid fa-check text-blue-400 mt-0.5 shrink-0"></i> Store single geospatial places with Decentralized Identifiers (DIDs)</li>
                <li class="flex gap-2"><i class="fa-solid fa-check text-blue-400 mt-0.5 shrink-0"></i> Bulk ingestion &amp; download (GeoJSON, GeoParquet, Shapefile)</li>
                <li class="flex gap-2"><i class="fa-solid fa-check text-blue-400 mt-0.5 shrink-0"></i> Query / search API and webapp for place discovery</li>
                <li class="flex gap-2"><i class="fa-solid fa-check text-blue-400 mt-0.5 shrink-0"></i> Integration with FAO Agro-informatics Platform &amp; Ground app</li>
                <li class="flex gap-2"><i class="fa-solid fa-check text-blue-400 mt-0.5 shrink-0"></i> Asset Registry 1.0, Ground &amp; AIP Well-Known-Areas ingestion</li>
                <li class="flex gap-2"><i class="fa-solid fa-check text-blue-400 mt-0.5 shrink-0"></i> CI/CD pipeline, backup &amp; disaster recovery</li>
                <li class="flex gap-2"><i class="fa-solid fa-check text-blue-400 mt-0.5 shrink-0"></i> Public open-source release</li>
              </ul>
            </div>
            <!-- Phase 2 -->
            <div class="glass-panel p-6 rounded-2xl border border-purple-500/20">
              <div class="flex items-center gap-3 mb-4">
                <span class="w-8 h-8 rounded-full bg-purple-600 flex items-center justify-center text-white text-sm font-bold">2</span>
                <h3 class="font-semibold text-white">{phase2_label}</h3>
              </div>
              <ul class="space-y-2 text-sm text-slate-400">
                <li class="flex gap-2"><i class="fa-solid fa-circle text-purple-400 mt-0.5 shrink-0 text-xs"></i> Organization / User registration, authentication &amp; authorization</li>
                <li class="flex gap-2"><i class="fa-solid fa-circle text-purple-400 mt-0.5 shrink-0 text-xs"></i> Authority URIs, metadata &amp; "my data" view for authorized users</li>
                <li class="flex gap-2"><i class="fa-solid fa-circle text-purple-400 mt-0.5 shrink-0 text-xs"></i> Authorized bulk access services for data modeling</li>
                <li class="flex gap-2"><i class="fa-solid fa-circle text-purple-400 mt-0.5 shrink-0 text-xs"></i> Extensible auxiliary data linkage service</li>
                <li class="flex gap-2"><i class="fa-solid fa-circle text-purple-400 mt-0.5 shrink-0 text-xs"></i> Survey, Cadastral Register &amp; Ground Truth dataset ingestion</li>
                <li class="flex gap-2"><i class="fa-solid fa-circle text-purple-400 mt-0.5 shrink-0 text-xs"></i> Federation design with OpenStreetMap &amp; Overture</li>
                <li class="flex gap-2"><i class="fa-solid fa-circle text-purple-400 mt-0.5 shrink-0 text-xs"></i> Zero-Knowledge Proofs &amp; Decentralized Anonymous Credentials</li>
              </ul>
            </div>
          </div>

          <!-- Tech stack footer -->
          <div class="glass-panel p-5 rounded-xl border border-white/5 flex flex-col md:flex-row items-center gap-4">
            <div class="text-slate-500 text-xs uppercase tracking-widest font-semibold shrink-0">Technology Stack</div>
            <div class="flex flex-wrap gap-3 justify-center md:justify-start">
              <span class="flex items-center gap-2 px-3 py-1 rounded-lg bg-slate-800/80 text-xs text-slate-300 border border-white/5"><i class="fa-solid fa-database text-blue-400"></i> Apache Iceberg</span>
              <span class="flex items-center gap-2 px-3 py-1 rounded-lg bg-slate-800/80 text-xs text-slate-300 border border-white/5"><i class="fa-solid fa-file-arrow-down text-emerald-400"></i> GeoParquet</span>
              <span class="flex items-center gap-2 px-3 py-1 rounded-lg bg-slate-800/80 text-xs text-slate-300 border border-white/5"><i class="fa-solid fa-hexagon-nodes text-yellow-400"></i> H3 / S2 / Geohash</span>
              <span class="flex items-center gap-2 px-3 py-1 rounded-lg bg-slate-800/80 text-xs text-slate-300 border border-white/5"><i class="fa-solid fa-globe text-sky-400"></i> OGC API Features</span>
              <span class="flex items-center gap-2 px-3 py-1 rounded-lg bg-slate-800/80 text-xs text-slate-300 border border-white/5"><i class="fa-solid fa-image text-purple-400"></i> STAC</span>
              <span class="flex items-center gap-2 px-3 py-1 rounded-lg bg-slate-800/80 text-xs text-slate-300 border border-white/5"><i class="fa-solid fa-fingerprint text-orange-400"></i> DIDs &amp; URIs</span>
              <span class="flex items-center gap-2 px-3 py-1 rounded-lg bg-slate-800/80 text-xs text-slate-300 border border-white/5"><i class="fa-brands fa-python text-blue-300"></i> FastAPI / DynaStore</span>
            </div>
          </div>

        </div>
        """
