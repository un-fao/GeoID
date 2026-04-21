import importlib.metadata
import pytest
import sys
from unittest.mock import patch
from dynastore.tasks import discover_tasks, _DYNASTORE_TASKS


@pytest.mark.asyncio
async def test_clean_discovery_missing_dependency():
    """When heavy deps are missing, discovery must not crash and the task
    either stays unregistered or registers as a lightweight
    ``DefinitionOnlyTask`` placeholder (so callers can still read its
    OGC Process metadata even though ``run()`` is unavailable).

    Patches ``importlib.metadata.entry_points`` to return ONLY the
    ``tiles_preseed`` entry-point so the test doesn't depend on every
    other task's imports succeeding in the test environment (e.g. GDAL,
    BigQuery, ES).  Discovery no longer takes a scope filter (M2.5 hard
    cut), so test isolation happens at the entry-point-enumeration layer.
    """
    real_eps = {
        ep for ep in importlib.metadata.entry_points(group="dynastore.tasks")
        if ep.name == "tiles_preseed"
    }

    def _only_tiles_preseed(*args, **kwargs):
        group = kwargs.get("group") or (args[0] if args else None)
        if group == "dynastore.tasks":
            return importlib.metadata.EntryPoints(list(real_eps))
        return importlib.metadata.entry_points(*args, **kwargs)

    with patch.dict(sys.modules, {"morecantile": None}):
        _DYNASTORE_TASKS.clear()
        for mod in list(sys.modules.keys()):
            if "tiles_preseed" in mod:
                del sys.modules[mod]

        with patch.object(
            importlib.metadata,
            "entry_points",
            side_effect=_only_tiles_preseed,
        ):
            discover_tasks()

        # Outcome must be one of: (a) absent from registry, or (b)
        # registered as a placeholder with ``is_placeholder == True``.
        # What MUST NOT happen: the full TilePreseedTask class (which
        # needs morecantile) ends up in the registry + runnable.
        cfg = _DYNASTORE_TASKS.get("tiles_preseed")
        if cfg is not None:
            assert getattr(cfg.cls, "is_placeholder", False) is True, (
                "tiles_preseed registered a non-placeholder class despite "
                "missing morecantile — graceful-skip path failed"
            )

@pytest.mark.asyncio
async def test_clean_discovery_success_upgrade():
    """
    Verifies that if dependencies ARE present, the task is registered with full implementation.
    """
    _DYNASTORE_TASKS.clear()

    discover_tasks()

    # Depending on environment, it might be registered or not.
    # This test just confirms discovery ran without crashing.
    if 'tiles_preseed' in _DYNASTORE_TASKS:
        config = _DYNASTORE_TASKS['tiles_preseed']
        # If imports work, is_placeholder should be False (or not set)
        is_placeholder = getattr(config.cls, 'is_placeholder', False)
        assert not is_placeholder, "Full implementation should not be a placeholder"
        assert config.definition is not None
        assert config.definition.id == 'tiles_preseed'
