"""Regression test — worker SCOPEs must include the modules their tasks need.

Each Cloud Run Job is built with a `worker_task_<name>` extras group from
`pyproject.toml`. If a worker's SCOPE omits a module its task needs, the
container's module-discovery loop leaves the corresponding Protocol
unresolved (e.g. `Catalogs: None`) and the task crashes at runtime — long
after the image has been built and deployed.

Audit confirmed (2026-04-29) for `worker_task_elasticsearch_indexer`:
    Module discovery left core protocols UNRESOLVED: ['Catalogs']
    protocol_resolvers={'Storage': ..., 'Catalogs': None, ...}

Per-task audit of the other 5 workers found 3 more (`gdal`, `tiles_preseed`,
`dimensions_materialize`) also using CatalogsProtocol or importing
`modules.catalog`; fixed prophylactically in the same PR.

The remaining 2 (`dwh_join`, `export_features`) use raw SQLAlchemy via
`get_engine()` and never touch CatalogsProtocol — they intentionally do
NOT include `module_catalog`. The negative-test case below pins this so
nobody accidentally widens them under cargo-cult pressure.
"""
import re
from pathlib import Path

import pytest


_PYPROJECT = Path(__file__).parent.parent.parent.parent.parent / "pyproject.toml"


# Workers whose tasks call `get_protocol(CatalogsProtocol)` or import from
# `dynastore.modules.catalog` at runtime. These MUST include `module_catalog`
# (directly or transitively).
_CATALOG_DEPENDENT_WORKERS = (
    "worker_task_elasticsearch_indexer",
    "worker_task_gdal",
    "worker_task_tiles_preseed",
    "worker_task_dimensions_materialize",
    "worker_task_ingestion",  # via catalog_grp → catalog_data_grp → catalog
)

# Workers whose tasks use raw SQLAlchemy engine and DO NOT call CatalogsProtocol.
# Adding `module_catalog` here would inflate the image without functional gain.
_CATALOG_FREE_WORKERS = (
    "worker_task_dwh_join",
    "worker_task_export_features",
)


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


def _resolve_extras_recursively(line: str, max_depth: int = 4) -> set:
    """Resolve the dynastore[...] extras chain transitively, so umbrella
    aliases (e.g. catalog_grp → catalog_data_grp → catalog → module_catalog)
    get expanded."""
    text = _PYPROJECT.read_text()
    seen = _extract_dynastore_extras(line)
    for _ in range(max_depth):
        new = set(seen)
        for extra in list(seen):
            for src_line in text.splitlines():
                if src_line.startswith(f"{extra} = "):
                    new |= _extract_dynastore_extras(src_line)
        if new == seen:
            break
        seen = new
    return seen


@pytest.mark.parametrize("worker", _CATALOG_DEPENDENT_WORKERS)
def test_catalog_dependent_worker_includes_module_catalog(worker: str) -> None:
    """B6 regression: each catalog-dependent worker SCOPE must include
    `module_catalog` (directly or transitively via an umbrella alias).
    Without it, `get_protocol(CatalogsProtocol)` returns None at runtime
    in the Cloud Run Job container and the task crashes."""
    line = _scope_definition(worker)
    extras = _resolve_extras_recursively(line)
    assert "module_catalog" in extras, (
        f"{worker} SCOPE does not transitively include module_catalog. "
        f"Tasks that call get_protocol(CatalogsProtocol) will crash on the "
        f"deployed Cloud Run Job. Resolved extras: {sorted(extras)}"
    )


@pytest.mark.parametrize("worker", _CATALOG_FREE_WORKERS)
def test_catalog_free_worker_omits_module_catalog(worker: str) -> None:
    """Negative case: workers whose tasks use raw `get_engine()` SQL and
    never call CatalogsProtocol must NOT include `module_catalog` directly.
    Adding it would inflate the image (pulls geospatial_core → shapely +
    geoalchemy2) without functional gain. This pins the audit decision so
    a future contributor doesn't cargo-cult the addition."""
    line = _scope_definition(worker)
    direct_extras = _extract_dynastore_extras(line)
    assert "module_catalog" not in direct_extras, (
        f"{worker} added module_catalog directly. Per the 2026-04-29 audit, "
        f"this worker's task uses raw SQLAlchemy via get_engine() and never "
        f"calls CatalogsProtocol — adding the module is dead weight. If the "
        f"task has changed to need it, update _CATALOG_FREE_WORKERS in this "
        f"test and re-audit. Direct extras: {sorted(direct_extras)}"
    )
