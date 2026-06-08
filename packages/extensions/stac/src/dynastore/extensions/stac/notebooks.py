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

"""NotebookContributorProtocol contributions for the STAC extension.

Picked up via ``NotebooksModule.lifespan`` -> ``get_protocols(
NotebookContributorProtocol)``. ``STACService.get_notebooks`` calls
:func:`build_contributions` here.

No import-time registration and no hard dependency on the notebooks
module — imports are lazy.
"""
from pathlib import Path

_HERE = Path(__file__).parent / "notebooks"
_REG = "dynastore.extensions.stac"


def build_contributions():
    try:
        from dynastore.modules.notebooks.contribution import NotebookContribution
        from dynastore.modules.notebooks.folder_discovery import discover_notebooks
    except Exception:
        return []

    # Explicit entries with rich titles/descriptions take precedence.
    explicit = [
        NotebookContribution(
            notebook_id="stac_catalog_collection_lifecycle",
            title={"en": "Catalog / Collection Lifecycle — STAC"},
            description={
                "en": (
                    "Walks the full STAC lifecycle: create catalog, create "
                    "collection with inline schema/layer_config/write_policy, "
                    "localized update (en + es), round-trip, soft-delete, and "
                    "the zero-config variant where every config falls back to "
                    "code defaults."
                )
            },
            tags=["stac", "catalog", "collection", "lifecycle", "demo"],
            notebook_path=_HERE / "catalog_collection_lifecycle.ipynb",
            registered_by=_REG,
        ),
        NotebookContribution(
            notebook_id="stac_virtual_asset_collections",
            title={"en": "Virtual Asset Collections — STAC"},
            description={
                "en": (
                    "End-to-end loop using the /virtual/assets/... STAC endpoints "
                    "to expose a single uploaded asset across every collection it "
                    "belongs to. Demonstrates registering one asset_id under two "
                    "collections and listing membership via the new virtual route."
                )
            },
            tags=["stac", "virtual", "assets", "demo"],
            notebook_path=_HERE / "virtual_asset_collections.ipynb",
            registered_by=_REG,
        ),
    ]
    explicit_paths = {c.notebook_path for c in explicit}

    # Auto-discover anything else in the folder (e.g. se01_gdal_to_stac_…).
    discovered = [
        c for c in discover_notebooks(_HERE, prefix="stac")
        if c.notebook_path not in explicit_paths
    ]
    return explicit + discovered
