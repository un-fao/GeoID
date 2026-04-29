"""Regression test — worker SCOPEs must include the modules their tasks need.

Each Cloud Run Job is built with a `worker_task_<name>` extras group from
`pyproject.toml`. If a worker's SCOPE omits a module its task needs, the
container's module-discovery loop leaves the corresponding Protocol
unresolved (e.g. `Catalogs: None`) and the task crashes at runtime — long
after the image has been built and deployed.

Audit confirmed (2026-04-29) for `worker_task_elasticsearch_indexer`:
    Module discovery left core protocols UNRESOLVED: ['Catalogs']
    protocol_resolvers={'Storage': ..., 'Catalogs': None, ...}

Follow-up audit (same day) of the other workers:
  - tiles_preseed: tasks/tiles_preseed/task.py:65 calls
    `get_protocol(CatalogsProtocol)` → needs module_catalog.
  - gdal: tasks/gdal/asset_process.py:25 + gdalinfo_task.py:32,40 import
    `from dynastore.modules.catalog.asset_service` → needs module_catalog.
  - dimensions_materialize: tasks/dimensions_materialize/task.py:67 calls
    `get_protocol(CatalogsProtocol)` → needs module_catalog.
  - dwh_join, export_features: use raw SQLAlchemy `get_engine()` and bind
    directly to the catalog's PG schema via SQL — never call CatalogsProtocol
    and never import from `dynastore.modules.catalog.*`. Correctly excluded.
"""
import re
from pathlib import Path


_PYPROJECT = Path(__file__).parent.parent.parent.parent.parent / "pyproject.toml"


def _scope_definition(scope: str) -> str:
    """Return the line defining ``<scope> = [...]`` from pyproject.toml."""
    text = _PYPROJECT.read_text()
    for line in text.splitlines():
        if line.startswith(f"{scope} = "):
            return line
    raise AssertionError(f"SCOPE '{scope}' not found in pyproject.toml")


def _extract_dynastore_extras(line: str) -> set:
    """Pull the comma-separated extras from any `dynastore[...]` clause(s)
    in a pyproject.toml line."""
    extras = set()
    for match in re.finditer(r"dynastore\[([^\]]+)\]", line):
        for x in match.group(1).split(","):
            extras.add(x.strip())
    return extras


def _assert_scope_has_module_catalog(scope: str, why: str) -> None:
    line = _scope_definition(scope)
    extras = _extract_dynastore_extras(line)
    assert "module_catalog" in extras, (
        f"{scope} SCOPE is missing module_catalog. {why} Current extras: {extras}"
    )


def test_elasticsearch_indexer_scope_includes_catalog() -> None:
    """B6 regression: SCOPE must include module_catalog so CatalogsProtocol
    has an implementor in the Cloud Run Job container."""
    _assert_scope_has_module_catalog(
        "worker_task_elasticsearch_indexer",
        "Without it, BulkCatalog/CollectionReindexTask crashes when calling "
        "get_protocol(CatalogsProtocol).",
    )


def test_tiles_preseed_scope_includes_catalog() -> None:
    """B6 follow-up: TilesPreseedTask reads catalog metadata via
    get_protocol(CatalogsProtocol) at tasks/tiles_preseed/task.py:65."""
    _assert_scope_has_module_catalog(
        "worker_task_tiles_preseed",
        "Without it, TilesPreseedTask crashes when calling "
        "get_protocol(CatalogsProtocol).",
    )


def test_gdal_scope_includes_catalog() -> None:
    """B6 follow-up: GDAL task imports from dynastore.modules.catalog.*
    (asset_process.py:25, gdalinfo_task.py:32,40); without module_catalog
    those imports fail at task startup."""
    _assert_scope_has_module_catalog(
        "worker_task_gdal",
        "Without it, GDAL task imports from dynastore.modules.catalog.* fail "
        "at startup (asset_service, asset_tasks_spi).",
    )


def test_dimensions_materialize_scope_includes_catalog() -> None:
    """B6 follow-up: DimensionsMaterializeTask reads catalog metadata via
    get_protocol(CatalogsProtocol) at tasks/dimensions_materialize/task.py:67."""
    _assert_scope_has_module_catalog(
        "worker_task_dimensions_materialize",
        "Without it, DimensionsMaterializeTask crashes when calling "
        "get_protocol(CatalogsProtocol).",
    )


def test_dwh_join_scope_includes_catalog() -> None:
    """Phase H follow-up: DwhJoinExportTask doesn't call CatalogsProtocol
    directly, but its entry-point load chain transitively pulls shapely via
    extensions/dwh/models.py → tools/geospatial. Without module_catalog
    (which provides geospatial_core → shapely), the dispatcher logs
    `Skipping plugin 'dwh_join': No module named 'shapely'` and the task
    never registers. Confirmed by `dynastore-dwh-join-export-job-24476`
    container log on 2026-04-29."""
    _assert_scope_has_module_catalog(
        "worker_task_dwh_join",
        "Without it, DwhJoinExportTask entry-point load fails with "
        "ImportError: No module named 'shapely' (transitively via "
        "extensions/dwh/models.py → tools/geospatial).",
    )
