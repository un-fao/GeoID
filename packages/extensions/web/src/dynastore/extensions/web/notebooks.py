"""Platform notebook registrations for the Web extension.

General-purpose UI-builder walkthroughs and cross-cutting demos that
don't belong to a single domain extension live here. Imported during
WebService.lifespan so they land in the platform notebook table before
NotebooksModule seeds them.
"""
from pathlib import Path

from dynastore.modules.notebooks.example_registry import register_platform_notebook

_HERE = Path(__file__).parent / "notebooks"
_REG = "dynastore.extensions.web"


# UI walkthrough series — alternative-UI integration, four steps.
# Order matters at the call-site: 01 stands up the catalog/collection
# that 02–04 reuse via RUN_ID/CATALOG_ID/COLLECTION_ID env vars.
register_platform_notebook(
    notebook_id="ui_walkthrough_01_setup_collection",
    registered_by=_REG,
    notebook_path=_HERE / "01_ui_setup_collection.ipynb",
    title={"en": "UI Walkthrough 01 — Setup Collection"},
    description={
        "en": (
            "Create catalog + collection, GDAL schema introspection, "
            "columnar geometry stats (area + length materialized as "
            "columns), write policy refusing duplicate asset_id. Each "
            "PATCH is followed by a configured-vs-effective delta cell."
        )
    },
    tags=["ui", "walkthrough", "catalog", "collection", "schema", "demo"],
)

register_platform_notebook(
    notebook_id="ui_walkthrough_02_upload_with_reporter",
    registered_by=_REG,
    notebook_path=_HERE / "02_ui_upload_with_reporter.ipynb",
    title={"en": "UI Walkthrough 02 — Upload with Reporter"},
    description={
        "en": (
            "Signed-URL init-upload, PUT bytes, asset registration "
            "(auto via Pub/Sub on remote, manual on local), ingestion "
            "process driven by GcsDetailedReporter writing JSONLines "
            "to gs://{catalog_bucket}/{collection_id}/reports/. Re-ingest "
            "demonstrates write-policy refusal flowing into the report."
        )
    },
    tags=["ui", "walkthrough", "upload", "ingestion", "reporter", "demo"],
)

register_platform_notebook(
    notebook_id="ui_walkthrough_03_read_search_features_tiles",
    registered_by=_REG,
    notebook_path=_HERE / "03_ui_read_search_features_tiles.ipynb",
    title={"en": "UI Walkthrough 03 — Read, Search, Features, Tiles"},
    description={
        "en": (
            "POST /search (cursor pagination, ES-primary), GET "
            "/features/.../items (CQL2 + bbox + sort, PG-backed), and "
            "GET .../tiles/{tms}/{z}/{x}/{y}.mvt — the three read "
            "surfaces a UI builder needs."
        )
    },
    tags=["ui", "walkthrough", "search", "features", "tiles", "demo"],
)

register_platform_notebook(
    notebook_id="ui_walkthrough_04_dwh_join_async_export",
    registered_by=_REG,
    notebook_path=_HERE / "04_ui_dwh_join_async_export.ipynb",
    title={"en": "UI Walkthrough 04 — DWH Join + Async Export"},
    description={
        "en": (
            "Async dwh_join Process (id dwh_join, scope COLLECTION, "
            "ASYNC_EXECUTE) using the canonical SELECT * FROM dwh "
            "fixture. Submit, poll, fetch result. Last cell shows the "
            "synchronous /dwh/join endpoint for inline joins."
        )
    },
    tags=["ui", "walkthrough", "dwh", "join", "async", "process", "demo"],
)
