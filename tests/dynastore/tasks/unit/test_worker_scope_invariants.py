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


def test_dwh_join_scope_includes_crs() -> None:
    """Phase H second follow-up: even with module_catalog (PR #141), the
    dwh_join entry-point load chain transitively imports pyproj via
    extensions/dwh/dwh.py → modules/tiles/tms_definitions.py:22
    `from pyproj import CRS`. module_crs provides pyproj. Without it,
    the dispatcher registers a DefinitionOnlyTask placeholder and
    main_task.py raises 'Task has run method without payload annotation'
    (misleading error — actual cause is the placeholder having no `run`).
    Confirmed by `dynastore-dwh-join-export-job-snq2n` container log on
    2026-04-29."""
    line = _scope_definition("worker_task_dwh_join")
    extras = _extract_dynastore_extras(line)
    assert "module_crs" in extras, (
        f"worker_task_dwh_join SCOPE is missing module_crs. Without it the "
        f"entry-point load fails with `No module named 'pyproj'` and dwh_join "
        f"runs as a no-run-method placeholder. Current extras: {extras}"
    )


def test_export_features_scope_includes_gcp() -> None:
    """Phase H third follow-up: ExportFeaturesTask writes its output bytes
    to GCS via dynastore.modules.gcp clients. Without module_gcp the
    worker container raises at run time: 'GCPModule has not been
    initialized or failed to create a storage client.' Confirmed by
    019dda8e-a239-7bd8-... OGC poll on 2026-04-29."""
    line = _scope_definition("worker_task_export_features")
    extras = _extract_dynastore_extras(line)
    assert "module_gcp" in extras, (
        f"worker_task_export_features SCOPE is missing module_gcp. Without "
        f"it the task crashes at run time when trying to create a GCS "
        f"storage client to write the export output. Current extras: {extras}"
    )


# ---------------------------------------------------------------------------
# Mapping invariants — tighten the scope ↔ entry-point ↔ meta-extra wiring
# so a typo or missing entry-point breaks CI before it breaks deploy.
# ---------------------------------------------------------------------------


def _all_worker_task_scope_names() -> list[str]:
    """Return every ``worker_task_<name>`` extras key from pyproject.toml."""
    text = _PYPROJECT.read_text()
    names: list[str] = []
    for line in text.splitlines():
        if line.startswith("worker_task_") and " = " in line:
            names.append(line.split(" = ", 1)[0].strip())
    return names


def _all_dynastore_tasks_entry_points() -> dict[str, str]:
    """Return ``{entry_point_name: target_str}`` for the ``dynastore.tasks``
    entry-point group declared in pyproject.toml."""
    text = _PYPROJECT.read_text()
    in_group = False
    out: dict[str, str] = {}
    for line in text.splitlines():
        stripped = line.strip()
        if stripped == '[project.entry-points."dynastore.tasks"]':
            in_group = True
            continue
        if in_group:
            if stripped.startswith("[") and stripped.endswith("]"):
                break  # next TOML section
            if " = " in stripped and not stripped.startswith("#"):
                k, v = stripped.split(" = ", 1)
                out[k.strip()] = v.strip().strip('"')
    return out


def _meta_extra_definition(name: str) -> str:
    """Return the line defining ``<name> = [...]`` for a meta-extra."""
    return _scope_definition(name)


def test_every_worker_task_scope_has_matching_entry_point() -> None:
    """Each `worker_task_<name>` extras key must correspond to a
    `dynastore.tasks.<name>` entry-point. Catches the class of bug where
    a SCOPE is defined but the task entry-point was renamed/removed (or
    vice versa) — Cloud Run Job container would deploy fine then crash
    at first dispatch with `Task '<name>' not found`."""
    scope_names = _all_worker_task_scope_names()
    entry_points = _all_dynastore_tasks_entry_points()

    missing: list[str] = []
    for scope in scope_names:
        short = scope.removeprefix("worker_task_")
        if short not in entry_points:
            missing.append(short)

    assert not missing, (
        f"worker_task_<name> SCOPEs without a matching `dynastore.tasks` "
        f"entry-point: {missing}. Either add the entry-point or remove the "
        f"orphan SCOPE. Available entry-points: {sorted(entry_points)}"
    )


def test_worker_service_and_scope_worker_agree_on_task_membership() -> None:
    """`worker_service` (runtime composition) and `scope_worker` (Cloud Run
    image SCOPE) must reference the same set of `worker_task_*` extras.

    Drift between them means the deployed image carries different task code
    than the local development worker — bugs reproduce in one and not the
    other. (This is a *consistency* check, not a *completeness* check —
    some `worker_task_*` are intentionally excluded from both because they
    only run as their own dedicated Cloud Run Job images, e.g.
    `worker_task_elasticsearch_indexer` and `worker_task_ingestion`.)"""
    scope_names = set(_all_worker_task_scope_names())
    worker_service_extras = _extract_dynastore_extras(
        _meta_extra_definition("worker_service")
    ) & scope_names
    scope_worker_extras = _extract_dynastore_extras(
        _meta_extra_definition("scope_worker")
    ) & scope_names

    only_in_service = worker_service_extras - scope_worker_extras
    only_in_scope = scope_worker_extras - worker_service_extras

    assert not only_in_service and not only_in_scope, (
        f"worker_service and scope_worker disagree on which worker_task_* "
        f"to include.\n"
        f"  Only in worker_service: {sorted(only_in_service)}\n"
        f"  Only in scope_worker:   {sorted(only_in_scope)}\n"
        f"Either add the missing entries to align them, or document the "
        f"intentional asymmetry by relaxing this test."
    )


def test_every_dynastore_tasks_entry_point_resolves_to_existing_module() -> None:
    """Each `dynastore.tasks` entry-point's `module:Class` target must point
    at a module file that exists on disk. Catches typos in the entry-point
    declaration (path drift after a rename refactor)."""
    entry_points = _all_dynastore_tasks_entry_points()
    src_root = _PYPROJECT.parent / "src"

    bad: list[str] = []
    for ep_name, ep_target in entry_points.items():
        module_path = ep_target.split(":")[0]
        # dynastore.tasks.foo.bar -> src/dynastore/tasks/foo/bar.py OR
        #                            src/dynastore/tasks/foo/bar/__init__.py
        rel = module_path.replace(".", "/")
        as_file = src_root / f"{rel}.py"
        as_pkg = src_root / rel / "__init__.py"
        if not (as_file.exists() or as_pkg.exists()):
            bad.append(f"{ep_name} → {ep_target} (no such module)")

    assert not bad, (
        "dynastore.tasks entry-points pointing at non-existent modules:\n  "
        + "\n  ".join(bad)
    )
