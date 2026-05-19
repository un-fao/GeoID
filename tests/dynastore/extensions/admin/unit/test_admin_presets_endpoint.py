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
    resp = client.post("/admin/catalogs/cat-pub/presets/public_catalog")
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
    resp = client.post("/admin/catalogs/cat-priv/presets/private_catalog")
    assert resp.status_code == 200, resp.text
    assert resp.json()["applied"] == [
        "catalog_routing",
        "collection_template",
        "items_template",
    ]


def test_apply_unknown_preset_returns_404(_patched_protocols):
    client = TestClient(_app())
    resp = client.post("/admin/catalogs/cat-x/presets/does_not_exist")
    assert resp.status_code == 404
    assert "does_not_exist" in resp.json()["detail"]


def test_apply_geoid_preset_flows_audience_configs_through_set_config(_patched_protocols):
    """Geoid preset emits two audience configs in addition to the three
    routing tiers. Pin that the apply loop walks the audience dict and
    invokes ``set_config`` for each — otherwise the operator-PUT path
    silently skips the anonymous opt-ins and the catalog ships with a
    private-only posture.
    """
    # Geoid preset must be registered (extension auto-registers on import).
    import dynastore.extensions.geoid  # noqa: F401
    from dynastore.modules.iam.audience_configs import (
        CatalogLookupAudience,
        CollectionWriteAudience,
    )

    client = TestClient(_app())
    resp = client.post("/admin/catalogs/cat-fao/presets/geoid")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["applied"] == [
        "catalog_routing",
        "collection_template",
        "items_template",
        "audience:catalog_lookup_audience",
        "audience:collection_write_audience",
    ]

    classes = [call.args[0] for call in _patched_protocols.set_config.await_args_list]
    assert CatalogLookupAudience in classes
    assert CollectionWriteAudience in classes

    # All audience set_config calls must scope to the catalog tier.
    audience_calls = [
        c for c in _patched_protocols.set_config.await_args_list
        if c.args[0] in (CatalogLookupAudience, CollectionWriteAudience)
    ]
    for call in audience_calls:
        assert call.kwargs["catalog_id"] == "cat-fao"
        assert "collection_id" not in call.kwargs


def test_apply_preset_is_idempotent_under_repeated_calls(_patched_protocols):
    """Re-applying the same preset must succeed — the notebook re-runs
    section 2a and operators may rerun apply during incident triage.
    The endpoint defers idempotency to ``set_config`` itself; this test
    just pins that two back-to-back applies don't raise."""
    client = TestClient(_app())
    r1 = client.post("/admin/catalogs/cat-pub/presets/public_catalog")
    r2 = client.post("/admin/catalogs/cat-pub/presets/public_catalog")
    assert r1.status_code == 200
    assert r2.status_code == 200
    # Six set_config calls total — three per apply.
    assert _patched_protocols.set_config.await_count == 6


# ---------------------------------------------------------------------------
# DELETE /admin/catalogs/{catalog_id}/presets/{preset_name} — #971 rollback
# ---------------------------------------------------------------------------


@pytest.fixture
def _patched_protocols_with_persistence(monkeypatch):
    """Like ``_patched_protocols`` but also models ``get_persisted_config``
    and ``delete_config`` against an in-memory ``{class: dict}`` store so
    DELETE can drive the byte-for-byte equality check end-to-end."""
    catalogs_mock = MagicMock()
    catalogs_mock.get_catalog_model = AsyncMock(return_value=MagicMock())

    store: dict[type, dict] = {}

    configs_mock = MagicMock()
    configs_mock._store = store

    async def _set_config(config_cls, config, **kwargs):
        store[config_cls] = config.model_dump(mode="json")
        return config

    async def _get_persisted(config_cls, **kwargs):
        return store.get(config_cls)

    async def _delete(config_cls, **kwargs):
        store.pop(config_cls, None)

    configs_mock.set_config = AsyncMock(side_effect=_set_config)
    configs_mock.get_persisted_config = AsyncMock(side_effect=_get_persisted)
    configs_mock.delete_config = AsyncMock(side_effect=_delete)

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


def test_delete_unknown_preset_returns_404(_patched_protocols_with_persistence):
    client = TestClient(_app())
    resp = client.delete("/admin/catalogs/cat-x/presets/does_not_exist")
    assert resp.status_code == 404
    assert "does_not_exist" in resp.json()["detail"]


def test_delete_preset_with_no_persisted_rows_returns_empty_deleted_list(
    _patched_protocols_with_persistence,
):
    """No persisted rows at the catalog scope → nothing to delete, but
    the call still succeeds (idempotent rollback)."""
    client = TestClient(_app())
    resp = client.delete("/admin/catalogs/cat-pub/presets/public_catalog")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["preset"] == "public_catalog"
    assert body["catalog_id"] == "cat-pub"
    assert body["deleted"] == []
    _patched_protocols_with_persistence.delete_config.assert_not_awaited()


def test_delete_after_apply_round_trip_clears_all_slots(
    _patched_protocols_with_persistence,
):
    """Apply public_catalog → every slot is persisted → DELETE removes
    every slot and reports them in leaf-first order."""
    client = TestClient(_app())
    apply_resp = client.post("/admin/catalogs/cat-pub/presets/public_catalog")
    assert apply_resp.status_code == 200, apply_resp.text

    del_resp = client.delete("/admin/catalogs/cat-pub/presets/public_catalog")
    assert del_resp.status_code == 200, del_resp.text
    body = del_resp.json()
    assert body["deleted"] == [
        "items_template",
        "collection_template",
        "catalog_routing",
    ]
    # All three slots were removed from the in-memory store.
    assert _patched_protocols_with_persistence._store == {}


def test_delete_geoid_preset_walks_audiences_leaf_first(
    _patched_protocols_with_persistence,
):
    """Geoid preset emits audience configs in addition to the three
    routing tiers. Rollback must also unapply the audiences and they
    must trail the routing tiers in the response."""
    import dynastore.extensions.geoid  # noqa: F401

    client = TestClient(_app())
    apply_resp = client.post("/admin/catalogs/cat-fao/presets/geoid")
    assert apply_resp.status_code == 200, apply_resp.text

    del_resp = client.delete("/admin/catalogs/cat-fao/presets/geoid")
    assert del_resp.status_code == 200, del_resp.text
    deleted = del_resp.json()["deleted"]
    # Routing tiers leaf-first, then audiences in bundle order.
    assert deleted[:3] == [
        "items_template",
        "collection_template",
        "catalog_routing",
    ]
    assert set(deleted[3:]) == {
        "audience:catalog_lookup_audience",
        "audience:collection_write_audience",
    }
    assert _patched_protocols_with_persistence._store == {}


def test_delete_preset_with_diverged_row_returns_409_and_keeps_store(
    _patched_protocols_with_persistence,
):
    """Persisted row that no longer matches the preset bundle blocks
    rollback with 409 and the store is left fully intact — partial
    rollback would leave the catalog in a half-preset state."""
    from dynastore.modules.storage.routing_config import (
        CatalogRoutingConfig,
        ItemsRoutingConfig,
    )

    client = TestClient(_app())
    apply_resp = client.post("/admin/catalogs/cat-drift/presets/public_catalog")
    assert apply_resp.status_code == 200, apply_resp.text

    # Manually corrupt the items_template row to simulate operator drift.
    store = _patched_protocols_with_persistence._store
    drifted = dict(store[ItemsRoutingConfig])
    drifted["__drift_marker__"] = "operator-edit"
    store[ItemsRoutingConfig] = drifted
    pre_snapshot = {k: dict(v) for k, v in store.items()}

    del_resp = client.delete("/admin/catalogs/cat-drift/presets/public_catalog")
    assert del_resp.status_code == 409
    detail = del_resp.json()["detail"]
    assert "cannot be rolled back" in detail["message"]
    diverged = detail["diverged"]
    assert len(diverged) == 1
    assert diverged[0]["slot"] == "items_template"
    assert diverged[0]["class"] == "ItemsRoutingConfig"

    # Nothing was deleted — the matching catalog_routing + collection_template
    # rows are still present.
    assert store == pre_snapshot
    _patched_protocols_with_persistence.delete_config.assert_not_awaited()
    # Sanity: catalog_routing was a matching slot but stayed put.
    assert CatalogRoutingConfig in store


def test_delete_preset_skips_missing_slots(_patched_protocols_with_persistence):
    """Operator partially removed some preset rows manually; rollback
    silently skips missing slots and deletes what remains."""
    from dynastore.modules.storage.routing_config import (
        CatalogRoutingConfig,
        CollectionRoutingConfig,
        ItemsRoutingConfig,
    )

    client = TestClient(_app())
    apply_resp = client.post("/admin/catalogs/cat-partial/presets/public_catalog")
    assert apply_resp.status_code == 200

    # Drop the catalog_routing row to simulate a partial manual cleanup.
    _patched_protocols_with_persistence._store.pop(CatalogRoutingConfig)

    del_resp = client.delete("/admin/catalogs/cat-partial/presets/public_catalog")
    assert del_resp.status_code == 200, del_resp.text
    assert del_resp.json()["deleted"] == ["items_template", "collection_template"]
    assert ItemsRoutingConfig not in _patched_protocols_with_persistence._store
    assert CollectionRoutingConfig not in _patched_protocols_with_persistence._store
