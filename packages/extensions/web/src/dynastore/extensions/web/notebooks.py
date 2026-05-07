"""NotebookContributorProtocol contributions for the Web extension.

General-purpose UI-builder walkthroughs and cross-cutting demos that
don't belong to a single domain extension live here. Picked up at
runtime via ``NotebooksModule.lifespan`` -> ``get_protocols(
NotebookContributorProtocol)``. ``Web.get_notebooks`` calls
:func:`build_contributions` here.

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
            notebook_id="ui_walkthrough_01_setup_collection",
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
            notebook_path=_HERE / "01_ui_setup_collection.ipynb",
        ),
        NotebookContribution(
            notebook_id="ui_walkthrough_02_upload_with_reporter",
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
            notebook_path=_HERE / "02_ui_upload_with_reporter.ipynb",
        ),
        NotebookContribution(
            notebook_id="ui_walkthrough_03_read_search_features_tiles",
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
            notebook_path=_HERE / "03_ui_read_search_features_tiles.ipynb",
        ),
        NotebookContribution(
            notebook_id="ui_walkthrough_04_dwh_join_async_export",
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
            notebook_path=_HERE / "04_ui_dwh_join_async_export.ipynb",
        ),
    ]
