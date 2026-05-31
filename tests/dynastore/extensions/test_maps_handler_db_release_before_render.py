"""Handler-tier regression test for ``MapsService.get_map_tile`` /
``MapsService.get_map`` — #737 item 1 / F1 guard.

PR #736 fixed the F1 issue (#703): a DB connection held across the CPU-bound
``run_in_executor(render_map_image, ...)`` call. The fix moved every DB-touching
step inside ``async with managed_transaction(engine) as conn:`` and ran the
render strictly **after** that block exits. These tests pin that ordering so a
future refactor can't reintroduce the slot-holding pattern.

Assertion shape: a recording wrapper around ``managed_transaction`` flips a
shared flag on ``__aexit__``; the patched ``render_map_image`` asserts the flag
is True at call time. The test fails if the order is ever swapped.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.extensions.maps import maps_service as ms


_TX_EXITED_KEY = "tx_exited_before_render"


def _make_recorder() -> Dict[str, Any]:
    return {_TX_EXITED_KEY: False, "render_called": False, "render_call_count": 0}


def _patch_db_layer(monkeypatch, recorder: Dict[str, Any]) -> None:
    """Replace every DB-touching dependency the handlers reach for.

    ``managed_transaction`` becomes an async CM that flips
    ``recorder[_TX_EXITED_KEY]`` on ``__aexit__``. ``render_map_image`` becomes
    a sync stub that asserts the flag is True (i.e. the CM exited before the
    executor invoked it) and returns dummy PNG bytes.
    """
    from sqlalchemy.ext.asyncio import AsyncConnection
    fake_conn = MagicMock(spec=AsyncConnection, name="async_conn")

    @asynccontextmanager
    async def _fake_managed_transaction(_engine):
        try:
            yield fake_conn
        finally:
            recorder[_TX_EXITED_KEY] = True

    monkeypatch.setattr(ms, "managed_transaction", _fake_managed_transaction)
    monkeypatch.setattr(ms, "get_async_engine", lambda _req: MagicMock(name="engine"))

    layer_cfg = MagicMock(name="layer_config")
    layer_cfg.geometry_storage.target_srid = 4326

    monkeypatch.setattr(
        ms, "_validate_collections_helper", AsyncMock(return_value=["c1"])
    )
    monkeypatch.setattr(ms, "_get_style_to_render", AsyncMock(return_value=None))

    mock_catalogs_svc = MagicMock(name="catalogs_svc")
    mock_catalogs_svc.get_collection_config = AsyncMock(return_value=layer_cfg)
    mock_catalogs_svc.get_catalog_model = AsyncMock(return_value=MagicMock())
    monkeypatch.setattr(ms, "get_protocol", lambda _proto: mock_catalogs_svc)

    monkeypatch.setattr(
        ms.maps_db,
        "get_features_for_rendering",
        AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(
        ms.tms_manager, "get_custom_tms", AsyncMock(return_value=None)
    )
    monkeypatch.setattr(
        ms.tms_manager, "resolve_srid", AsyncMock(return_value=3857)
    )

    def _fake_render_map_image(*_args: Any, **_kwargs: Any) -> bytes:
        # The whole point of #703/#737 item 1: this assertion must hold every
        # time the executor invokes the renderer.
        assert recorder[_TX_EXITED_KEY], (
            "F1 regression: render_map_image invoked while managed_transaction"
            " was still open — DB connection held across run_in_executor."
        )
        recorder["render_called"] = True
        recorder["render_call_count"] += 1
        return b"\x89PNG\r\n\x1a\n"

    # ``run_in_executor`` resolves the name at call time, so patching the
    # module-level binding (the captured free variable in the handler) is
    # enough to intercept.
    monkeypatch.setattr(ms, "render_map_image", _fake_render_map_image)
    # Use the default loop executor (None) — avoids spinning a real
    # ProcessPoolExecutor for what is now a trivial sync stub.
    monkeypatch.setattr(ms.MapsService, "process_pool", None)


def _mock_request() -> MagicMock:
    req = MagicMock(name="request")
    return req


@pytest.mark.asyncio
async def test_get_map_tile_releases_db_before_render(monkeypatch):
    """``get_map_tile``: ``managed_transaction.__aexit__`` must complete before
    ``render_map_image`` is invoked by ``loop.run_in_executor``.
    """
    recorder = _make_recorder()
    _patch_db_layer(monkeypatch, recorder)

    response = await ms.MapsService.get_map_tile(
        request=_mock_request(),
        dataset="cat_a",
        tileMatrixSetId="WebMercatorQuad",
        z="0",
        x=0,
        y=0,
        collections="c1",
        datetime=None,
        subset=None,
        bgcolor=None,
        transparent=True,
        style=None,
    )

    assert recorder["render_called"], (
        "render_map_image was never invoked — patch wiring is wrong."
    )
    assert recorder[_TX_EXITED_KEY], (
        "managed_transaction CM never exited — the handler completed without"
        " releasing the connection."
    )
    assert response.status_code == 200
    assert response.media_type == "image/png"


@pytest.mark.asyncio
async def test_get_map_releases_db_before_render(monkeypatch):
    """``get_map`` (non-tiled): same F1 invariant as ``get_map_tile``."""
    recorder = _make_recorder()
    _patch_db_layer(monkeypatch, recorder)

    response = await ms.MapsService.get_map(
        dataset="cat_a",
        request=_mock_request(),
        collections="c1",
        bbox="-180,-90,180,90",
        bbox_crs=None,
        crs="EPSG:3857",
        width=256,
        height=256,
        style=None,
        bgcolor=None,
        transparent=True,
        datetime=None,
        subset=None,
        f="png",
    )

    assert recorder["render_called"], (
        "render_map_image was never invoked — patch wiring is wrong."
    )
    assert recorder[_TX_EXITED_KEY], (
        "managed_transaction CM never exited — the handler completed without"
        " releasing the connection."
    )
    # png passthrough returns a Response with media_type "image/png".
    assert response.media_type == "image/png"


@pytest.mark.asyncio
async def test_buggy_shape_demo_render_inside_tx_is_caught(monkeypatch):
    """Inverse demo: if the handler ever rendered *inside* the
    ``managed_transaction`` CM, the ``assert`` inside the patched
    ``render_map_image`` would trip. This test pins the assertion shape — it
    proves the test would catch a regression rather than silently passing.
    """
    recorder = _make_recorder()
    _patch_db_layer(monkeypatch, recorder)

    # Simulate the bug by calling render_map_image directly *before* the
    # managed_transaction CM has exited (recorder flag is still False).
    with pytest.raises(AssertionError, match="F1 regression"):
        ms.render_map_image(  # type: ignore[arg-type]
            256, 256, [0, 0, 1, 1], "EPSG:3857", 4326, [], None, True, None,
        )
