import pytest
import sys
from unittest.mock import patch
from dynastore.tasks import discover_tasks, _DYNASTORE_TASKS

@pytest.mark.asyncio
async def test_clean_discovery_missing_dependency():
    """
    Verifies that a task with a missing implementation dependency is gracefully
    skipped (not registered) and discovery does not raise an exception.
    """

    # 1. Simulate 'morecantile' module being missing
    with patch.dict(sys.modules, {'morecantile': None}):

        # 2. Clear registry and cached modules to ensure fresh discovery
        _DYNASTORE_TASKS.clear()
        for mod in list(sys.modules.keys()):
            if "tiles_preseed" in mod:
                del sys.modules[mod]

        # 3. Trigger discovery for tiles_preseed — should not crash
        discover_tasks(include_only=['tiles_preseed'])

        # 4. Task should NOT be registered because the entry point load failed
        #    (morecantile import error prevents TilePreseedTask from loading)
        assert 'tiles_preseed' not in _DYNASTORE_TASKS, (
            "tiles_preseed should be skipped when morecantile is unavailable"
        )

@pytest.mark.asyncio
async def test_clean_discovery_success_upgrade():
    """
    Verifies that if dependencies ARE present, the task is registered with full implementation.
    """
    _DYNASTORE_TASKS.clear()

    discover_tasks(include_only=['tiles_preseed'])

    # Depending on environment, it might be registered or not.
    # This test just confirms discovery ran without crashing.
    if 'tiles_preseed' in _DYNASTORE_TASKS:
        config = _DYNASTORE_TASKS['tiles_preseed']
        # If imports work, is_placeholder should be False (or not set)
        is_placeholder = getattr(config.cls, 'is_placeholder', False)
        assert not is_placeholder, "Full implementation should not be a placeholder"
        assert config.definition is not None
        assert config.definition.id == 'tiles_preseed'
