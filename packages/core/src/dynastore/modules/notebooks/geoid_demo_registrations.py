#    Copyright 2026 FAO
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

"""Platform notebook registrations for the GeoID customer-facing demos.

Lives in core (rather than the geoid extension) because the catalog
service runs without ``extension_geoid`` installed but is the surface
that serves JupyterLite. Loaded by ``NotebooksModule.lifespan`` via the
hardcoded module-path list.
"""
from pathlib import Path

from .example_registry import register_platform_notebook

_HERE = Path(__file__).parent / "notebooks"
_REG = "geoid_demo"


register_platform_notebook(
    notebook_id="nb07_public_vs_private_presets",
    registered_by=_REG,
    notebook_path=_HERE / "nb07_public_vs_private_presets.ipynb",
    title={"en": "Public vs Private Collections — preset-driven demo"},
    description={
        "en": (
            "Two-scenario walkthrough driven entirely by the presets API. "
            "Scenario A applies the public_open_data composite preset and "
            "proves both authenticated and anonymous STAC search return "
            "ingested items. Scenario B applies the private_tenant composite "
            "preset and proves anonymous search is denied while the "
            "authenticated user sees all items. Includes catalog-readiness "
            "polls, OUTBOX drain polls, and a PG-hint fallback probe. "
            "No inline routing or driver config payloads."
        )
    },
    tags=["geoid", "stac", "presets", "privacy", "demo"],
)
register_platform_notebook(
    notebook_id="nb08_geoid_lookup",
    registered_by=_REG,
    notebook_path=_HERE / "nb08_geoid_lookup.ipynb",
    title={"en": "GeoID Lookup — preset-driven anonymous access demo"},
    description={
        "en": (
            "Demonstrates the lookup-only access model enabled by the geoid "
            "catalog-tier preset. Setup applies the geoid preset (sysadmin "
            "token) and ingests two items. The anonymous section shows that "
            "POST /search/catalogs/{cat}/geoid-search (by geoid or external_id) "
            "and exact-item GET succeed without a token, while collection list, "
            "items list, and STAC search return 401/403. Route used: "
            "POST /search/catalogs/{cat}/geoid-search; response field: results."
        )
    },
    tags=["geoid", "lookup", "anonymous", "iam", "presets", "demo"],
)
