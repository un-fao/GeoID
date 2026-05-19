"""Routing-preset admin endpoint (#847)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from dynastore.extensions.admin.admin_service import AdminService
from dynastore.modules.storage.routing_config import (
    CatalogRoutingConfig,
    CollectionRoutingConfig,
    ItemsRoutingConfig,
)


def _app() -> FastAPI:
    app = FastAPI()
    app.include_router(AdminService.router)
    return app


def test_list_routing_presets_returns_registered_names():
    client = TestClient(_app())
    resp = client.get("/admin/presets")
    assert resp.status_code == 200
    body = resp.json()
    names = {p["name"] for p in body["presets"]}
    assert {"public_catalog", "private_catalog"}.issubset(names)
    for entry in body["presets"]:
        assert entry["description"]


@pytest.fixture
def _patched_protocols(monkeypatch):
    """Stub ``CatalogsProtocol`` (so _assert_catalog_exists passes) and
    ``ConfigsProtocol`` (so set_config records calls)."""
    catalogs_mock = MagicMock()
    catalogs_mock.get_catalog_model = AsyncMock(return_value=MagicMock())

    configs_mock = MagicMock()
    configs_mock.set_config = AsyncMock(return_value=None)

    def _fake_get_protocol(proto):
        from dynastore.models.protocols.catalogs import CatalogsProtocol
        from dynastore.models.protocols.configs import ConfigsProtocol

        if proto is CatalogsProtocol:
            return catalogs_mock
        if proto is ConfigsProtocol:
            return configs_mock
        return None

    monkeypatch.setattr(
        "dynastore.extensions.admin.admin_service.get_protocol",
        _fake_get_protocol,
    )
    return configs_mock


def test_apply_public_catalog_preset_walks_all_three_routing_tiers(_patched_protocols):
    client = TestClient(_app())
    resp = client.post("/admin/catalogs/cat-pub/presets/public_catalog/apply")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["preset"] == "public_catalog"
    assert body["catalog_id"] == "cat-pub"
    assert body["applied"] == [
        "catalog_routing",
        "collection_template",
        "items_template",
    ]

    classes = [call.args[0] for call in _patched_protocols.set_config.await_args_list]
    assert classes == [
        CatalogRoutingConfig,
        CollectionRoutingConfig,
        ItemsRoutingConfig,
    ]
    for call in _patched_protocols.set_config.await_args_list:
        assert call.kwargs["catalog_id"] == "cat-pub"


def test_apply_private_catalog_preset_includes_only_routing_no_audiences(_patched_protocols):
    client = TestClient(_app())
    resp = client.post("/admin/catalogs/cat-priv/presets/private_catalog/apply")
    assert resp.status_code == 200, resp.text
    assert resp.json()["applied"] == [
        "catalog_routing",
        "collection_template",
        "items_template",
    ]


def test_apply_unknown_preset_returns_404(_patched_protocols):
    client = TestClient(_app())
    resp = client.post("/admin/catalogs/cat-x/presets/does_not_exist/apply")
    assert resp.status_code == 404
    assert "does_not_exist" in resp.json()["detail"]
