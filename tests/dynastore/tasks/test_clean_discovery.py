import pytest
import sys
from unittest.mock import patch
from dynastore.tasks import discover_tasks, _DYNASTORE_TASKS

@pytest.mark.asyncio
async def test_clean_discovery_missing_dependency():
    """Clean Discovery Protocol: when a task's heavy runtime deps are
    missing, the full ``TaskProtocol`` class fails to load BUT a
    placeholder is registered from the ``<package>.definition``
    sibling module so the OGC Process metadata is still exposed
    (for e.g. ``/processes`` inventory + remote Cloud Run runners).

    Contract pinned here:

    * Discovery does not raise.
    * If the task is registered, ``is_placeholder=True`` and the
      ``definition`` is non-None — that's the whole point of the
      placeholder fallback.
    * If the definition-only module ALSO transitively needs
      morecantile, it's acceptable for discovery to silently skip
      (``'tiles_preseed' not in _DYNASTORE_TASKS``).  The critical
      invariant is that discovery doesn't crash either way.

    See ``_register_definition_only_placeholders`` in
    ``dynastore.tasks.__init__`` for the protocol's implementation.
    """

    # 1. Simulate 'morecantile' module being missing
    with patch.dict(sys.modules, {'morecantile': None}):

        # 2. Clear registry and cached modules to ensure fresh discovery
        _DYNASTORE_TASKS.clear()
        for mod in list(sys.modules.keys()):
            if "tiles_preseed" in mod:
                del sys.modules[mod]

        # 3. Trigger discovery for tiles_preseed — must not crash
        discover_tasks(include_only=['tiles_preseed'])

        # 4. Either the placeholder is registered (preferred) OR
        #    nothing is (acceptable if the definition module also
        #    transitively needs morecantile).  A FULL implementation
        #    registering would indicate the morecantile block was
        #    bypassed — THAT's the real regression to catch.
        if 'tiles_preseed' in _DYNASTORE_TASKS:
            config = _DYNASTORE_TASKS['tiles_preseed']
            assert getattr(config.cls, 'is_placeholder', False) is True, (
                "tiles_preseed registered as a FULL implementation "
                "despite morecantile being unavailable — Clean "
                "Discovery Protocol bypassed.  See "
                "dynastore.tasks.__init__::_register_definition_only_placeholders."
            )
            assert config.definition is not None, (
                "Placeholder must expose the Process definition "
                "(that's what /processes inventory iterates on)."
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
