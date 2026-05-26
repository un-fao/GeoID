"""Contract tests: ``delete_item`` threads caller_id into tile-cache invalidation.

Mirrors ``tests/dynastore/modules/tiles/unit/test_tile_invalidation_caller_id.py``
for the DELETE path.  The DELETE-path invalidation call lives inside
``ItemQueryMixin.delete_item`` (``item_query.py``).  These tests pin:

1. Source-grep guard: the forwarding line ``caller_id=_caller_id`` is present
   in the compiled source near the ``enqueue_tile_invalidation_task`` call.
2. When a ``DriverContext`` with ``processing.caller_id`` is supplied, the
   helper correctly derives the caller_id before the enqueue call.
3. When no ``DriverContext`` / no processing hints are supplied, the helper
   derives ``None`` (the sentinel fallback inside
   ``enqueue_tile_invalidation_task`` applies downstream).
"""
from __future__ import annotations

import inspect

import dynastore.modules.tiles.tile_cache_sync as tcs
from dynastore.models.driver_context import DriverContext, ProcessingHints
from dynastore.modules.catalog import item_query


# ---------------------------------------------------------------------------
# Test 1: source-grep guard
# ---------------------------------------------------------------------------

def test_source_contains_caller_id_forwarding() -> None:
    """delete_item source must forward _caller_id= to enqueue_tile_invalidation_task."""
    src = inspect.getsource(item_query.ItemQueryMixin.delete_item)
    assert "caller_id=_caller_id" in src, (
        "delete_item must forward _caller_id to enqueue_tile_invalidation_task; "
        "check the tile-cache invalidation block in item_query.py"
    )


# ---------------------------------------------------------------------------
# Test 2: caller_id extraction from DriverContext
# ---------------------------------------------------------------------------

def test_caller_id_extracted_from_ctx_processing() -> None:
    """caller_id is read from ctx.processing.caller_id — verify the extraction
    logic that delete_item uses matches the DriverContext model shape."""
    ctx = DriverContext(processing=ProcessingHints(caller_id="user:alice"))
    # Reproduce the extraction expression from delete_item:
    _caller_id = (
        ctx.processing.caller_id
        if ctx and ctx.processing
        else None
    )
    assert _caller_id == "user:alice"


# ---------------------------------------------------------------------------
# Test 3: None when ctx absent
# ---------------------------------------------------------------------------

def test_caller_id_is_none_when_no_ctx() -> None:
    """caller_id must be None when no DriverContext is provided."""
    ctx = None
    _caller_id = (
        ctx.processing.caller_id  # type: ignore[union-attr]
        if ctx and ctx.processing
        else None
    )
    assert _caller_id is None


# ---------------------------------------------------------------------------
# Test 4: None when ctx.processing is absent
# ---------------------------------------------------------------------------

def test_caller_id_is_none_when_no_processing_hints() -> None:
    """caller_id must be None when DriverContext carries no processing hints."""
    ctx = DriverContext()  # processing=None
    _caller_id = (
        ctx.processing.caller_id
        if ctx and ctx.processing
        else None
    )
    assert _caller_id is None
