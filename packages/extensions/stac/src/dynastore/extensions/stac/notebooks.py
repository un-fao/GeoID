"""NotebookContributorProtocol contributions for the STAC extension.

Picked up at runtime via ``NotebooksModule.lifespan`` -> ``get_protocols(
NotebookContributorProtocol)``. The extension class's ``get_notebooks``
calls :func:`build_contributions` here.

No import-time registration and no hard dependency on the notebooks
module — ``NotebookContribution`` is imported lazily so the extension
stays loadable in SCOPEs that don't include the notebooks module.
"""
from pathlib import Path

_HERE = Path(__file__).parent / "notebooks"


def build_contributions():
    from dynastore.modules.notebooks.contribution import NotebookContribution

    return [
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
        ),
    ]
