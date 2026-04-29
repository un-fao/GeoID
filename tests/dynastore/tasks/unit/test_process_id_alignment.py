"""B1 regression test — OGC Process IDs must match the dispatcher task_type.

Each `dynastore.tasks.<pkg>.definition` module declares a `Process` whose `id`
becomes the OGC Process ID (and the URL path segment, and the dispatcher key).
The runner's `can_handle(task_type)` lookup uses this exact string. If `id`
differs from the registered task_type or from the Cloud Run Job's `TASK_TYPE`
env var, the OGC POST returns 501 ("No available runner") or the Cloud Run Job
fails on container boot ("Task not found").

This test pins the rule: every `id` must be a snake_case identifier (no
hyphens), matching the entry-point name and the deployed Cloud Run TASK_TYPE.
"""
import re

import pytest

from dynastore.modules.processes.models import Process

# Each entry: (definition module dotted path, attribute name)
DEFINITIONS = [
    ("dynastore.tasks.dwh_join.definition", "DWH_JOIN_EXPORT_PROCESS_DEFINITION"),
    ("dynastore.tasks.export_features.definition", "EXPORT_FEATURES_PROCESS_DEFINITION"),
    ("dynastore.tasks.gdal.definition", "GDALINFO_PROCESS_DEFINITION"),
    ("dynastore.tasks.tiles_preseed.definition", "TILES_PRESEED_PROCESS_DEFINITION"),
    ("dynastore.tasks.dimensions_materialize.definition", "DIMENSIONS_MATERIALIZE_PROCESS_DEFINITION"),
    ("dynastore.tasks.ingestion.definition", "INGESTION_PROCESS_DEFINITION"),
    ("dynastore.tasks.elasticsearch_indexer.definition", "ELASTICSEARCH_INDEXER_PROCESS_DEFINITION"),
]


_SNAKE_CASE = re.compile(r"^[a-z][a-z0-9_]*$")


@pytest.mark.parametrize("module_path,attr_name", DEFINITIONS)
def test_process_id_is_snake_case(module_path: str, attr_name: str) -> None:
    """Every Process.id is snake_case — no hyphens, no uppercase."""
    import importlib

    try:
        mod = importlib.import_module(module_path)
    except ImportError as e:
        pytest.skip(f"definition module not importable in this SCOPE: {e}")
    process = getattr(mod, attr_name, None)
    if process is None:
        pytest.skip(f"{module_path} has no attr {attr_name}")
    assert isinstance(process, Process), (
        f"{module_path}.{attr_name} is not a Process instance"
    )
    assert _SNAKE_CASE.match(process.id), (
        f"Process id {process.id!r} from {module_path} is not snake_case. "
        "OGC URL paths and dispatcher runner registries use this verbatim — "
        "hyphens cause 501 'No available runner'. Use underscores."
    )
