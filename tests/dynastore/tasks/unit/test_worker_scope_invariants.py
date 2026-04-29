"""Regression test — worker SCOPEs must include the modules their tasks need.

Each Cloud Run Job is built with a `worker_task_<name>` extras group from
`pyproject.toml`. If a worker's SCOPE omits a module its task needs, the
container's module-discovery loop leaves the corresponding Protocol
unresolved (e.g. `Catalogs: None`) and the task crashes at runtime — long
after the image has been built and deployed.

Audit confirmed (2026-04-29) for `worker_task_elasticsearch_indexer`:
    Module discovery left core protocols UNRESOLVED: ['Catalogs']
    protocol_resolvers={'Storage': ..., 'Catalogs': None, ...}

The five other worker SCOPEs (`dwh_join`, `export_features`, `gdal`,
`tiles_preseed`, `dimensions_materialize`) also omit `module_catalog` but
their failure mode hasn't been proven yet — flagged here as suspects to
verify post-deploy.
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


def test_elasticsearch_indexer_scope_includes_catalog() -> None:
    """B6 regression: SCOPE must include module_catalog so CatalogsProtocol
    has an implementor in the Cloud Run Job container."""
    line = _scope_definition("worker_task_elasticsearch_indexer")
    extras = _extract_dynastore_extras(line)
    assert "module_catalog" in extras, (
        f"worker_task_elasticsearch_indexer SCOPE is missing module_catalog. "
        f"Without it, BulkCatalog/CollectionReindexTask crashes when calling "
        f"get_protocol(CatalogsProtocol). Current extras: {extras}"
    )
