"""Tests for `TilesCachingConfig` and its caching-config helper.

Issue #475 routes the bucket-backed tile cache's ``key_prefix`` and
``ttl_seconds`` through the PluginConfig waterfall so operators can
edit them via ``PUT /configs/plugins/tiles_caching_config`` without a
restart. Defaults must match the pre-#475 hardcoded values
(``tiles/collections`` / ``31536000``) so a missing config layer is a
no-op; live overrides must take effect; and a protocol failure must
fall back to defaults rather than crash tile I/O.
"""
from __future__ import annotations

from typing import Optional, Type

import pytest

from dynastore.modules.db_config.platform_config_service import PluginConfig
from dynastore.modules.tiles.tiles_config import TilesCachingConfig


def test_defaults_preserve_pre_issue_475_behavior():
    cfg = TilesCachingConfig()
    assert cfg.key_prefix == "tiles/collections"
    assert cfg.ttl_seconds == 31536000


@pytest.mark.parametrize("bad_prefix", [
    "",                  # empty
    "/leading-slash",
    "trailing-slash/",
    "spa ces",
    "../escape",
    "a" * 200,           # max_length=128
])
def test_key_prefix_rejects_bad_values(bad_prefix):
    with pytest.raises(Exception):
        TilesCachingConfig(key_prefix=bad_prefix)


@pytest.mark.parametrize("ttl", [-1, 31536001, 10**12])
def test_ttl_seconds_out_of_range_rejected(ttl):
    with pytest.raises(Exception):
        TilesCachingConfig(ttl_seconds=ttl)


def test_ttl_zero_allowed_disables_cache_control_max_age():
    """``ttl_seconds=0`` is a documented "disable browser/CDN cache" knob."""
    cfg = TilesCachingConfig(ttl_seconds=0)
    assert cfg.ttl_seconds == 0


def test_key_prefix_accepts_multi_segment_path():
    cfg = TilesCachingConfig(key_prefix="cache/v2/tiles")
    assert cfg.key_prefix == "cache/v2/tiles"


class _StubPlatformConfigsProtocol:
    is_platform_manager = True

    def __init__(self, cfg: Optional[TilesCachingConfig]) -> None:
        self._cfg = cfg

    async def get_config(
        self,
        config_cls: Type[PluginConfig],
        ctx=None,
    ) -> PluginConfig:
        if self._cfg is None or config_cls is not TilesCachingConfig:
            return TilesCachingConfig()
        return self._cfg

    async def set_config(self, *a, **kw) -> None: ...
    async def list_configs(self): return {}


@pytest.fixture
def install_stub(monkeypatch):
    def _install(cfg: Optional[TilesCachingConfig]):
        stub = _StubPlatformConfigsProtocol(cfg)
        from dynastore.models.protocols import platform_configs as pc_mod
        from dynastore.tools import discovery

        def fake_get_protocol(proto, *a, **kw):
            if proto is pc_mod.PlatformConfigsProtocol:
                return stub
            return None

        monkeypatch.setattr(discovery, "get_protocol", fake_get_protocol)
        return stub

    return _install


@pytest.mark.asyncio
async def test_loader_falls_back_when_protocol_missing(monkeypatch):
    """Cold-boot / unit-test path: no PlatformConfigsProtocol registered."""
    from dynastore.tools import discovery
    from dynastore.modules.gcp.tiles_storage import _load_caching_config

    monkeypatch.setattr(discovery, "get_protocol", lambda *a, **kw: None)
    cfg = await _load_caching_config()
    assert cfg.key_prefix == "tiles/collections"
    assert cfg.ttl_seconds == 31536000


@pytest.mark.asyncio
async def test_loader_returns_live_config(install_stub):
    install_stub(TilesCachingConfig(key_prefix="cache/v2", ttl_seconds=3600))
    from dynastore.modules.gcp.tiles_storage import _load_caching_config

    cfg = await _load_caching_config()
    assert cfg.key_prefix == "cache/v2"
    assert cfg.ttl_seconds == 3600


@pytest.mark.asyncio
async def test_loader_falls_back_on_protocol_error(monkeypatch):
    """An unexpected error inside get_config must NOT crash tile I/O."""
    class Boom:
        is_platform_manager = True
        async def get_config(self, *a, **kw):
            raise RuntimeError("boom")
        async def set_config(self, *a, **kw): ...
        async def list_configs(self): return {}

    from dynastore.tools import discovery
    from dynastore.models.protocols import platform_configs as pc_mod
    from dynastore.modules.gcp.tiles_storage import _load_caching_config

    monkeypatch.setattr(
        discovery,
        "get_protocol",
        lambda p, *a, **kw: Boom() if p is pc_mod.PlatformConfigsProtocol else None,
    )
    cfg = await _load_caching_config()
    assert cfg.key_prefix == "tiles/collections"
    assert cfg.ttl_seconds == 31536000


def test_build_blob_path_shape():
    from dynastore.modules.gcp.tiles_storage import _build_blob_path

    path = _build_blob_path("tiles/collections", "admin0", "WebMercatorQuad", 5, 17, 11, "mvt")
    assert path == "tiles/collections/admin0/WebMercatorQuad/5/17/11.mvt"


def test_build_blob_path_honors_custom_prefix():
    from dynastore.modules.gcp.tiles_storage import _build_blob_path

    path = _build_blob_path("cache/v2", "admin0", "WebMercatorQuad", 5, 17, 11, "mvt")
    assert path == "cache/v2/admin0/WebMercatorQuad/5/17/11.mvt"
