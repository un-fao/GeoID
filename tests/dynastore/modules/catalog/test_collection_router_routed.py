"""Tests for config-driven driver resolution in collection_router.

Verifies that when ``resolve_routed`` returns entries the router uses the
config-ordered drivers, and that it falls back to ``_resolve_drivers``
discovery when ``resolve_routed`` returns an empty list (e.g. early boot
with ConfigsProtocol absent).
"""

from __future__ import annotations

from dynastore.models.protocols.entity_store import EntityStoreCapability


class _RecordingDriver:
    """Minimal CollectionStore stand-in that records calls."""

    capabilities = frozenset({
        EntityStoreCapability.READ,
        EntityStoreCapability.WRITE,
        EntityStoreCapability.SEARCH,
    })

    def __init__(self, name: str):
        self.name = name
        self.calls: list[str] = []

    async def get_metadata(self, catalog_id, collection_id, *, context=None, db_resource=None):
        self.calls.append("get")
        return {"id": collection_id, "_src": self.name}

    async def upsert_metadata(self, catalog_id, collection_id, metadata, *, db_resource=None):
        self.calls.append("upsert")

    async def delete_metadata(self, catalog_id, collection_id, *, soft=False, db_resource=None):
        self.calls.append("delete")

    async def search_metadata(
        self,
        catalog_id,
        *,
        q=None,
        bbox=None,
        datetime_range=None,
        filter_cql=None,
        limit=100,
        offset=0,
        context=None,
        db_resource=None,
    ):
        self.calls.append("search")
        return ([{"id": "c1", "_src": self.name}], 1)


async def test_read_uses_config_resolved_drivers(monkeypatch):
    from dynastore.modules.catalog import collection_router
    from dynastore.modules.storage.routing_config import Operation, OperationDriverEntry

    pg = _RecordingDriver("pg")

    async def _fake_resolve(rpc, operation, catalog_id, collection_id=None, *, db_resource=None):
        if operation == Operation.READ:
            return [(OperationDriverEntry(driver_ref="collection_postgresql_driver"), pg)]
        return []

    monkeypatch.setattr(collection_router, "resolve_routed", _fake_resolve)
    result = await collection_router.get_collection_metadata("cat", "coll")
    assert result == {"id": "coll", "_src": "pg"}
    assert pg.calls == ["get"]


async def test_read_falls_back_to_discovery_when_resolver_empty(monkeypatch):
    from dynastore.modules.catalog import collection_router

    disco = _RecordingDriver("disco")

    async def _empty_resolve(*a, **kw):
        return []

    monkeypatch.setattr(collection_router, "resolve_routed", _empty_resolve)
    monkeypatch.setattr(collection_router, "_resolve_drivers", lambda: [disco])
    result = await collection_router.get_collection_metadata("cat", "coll")
    assert result == {"id": "coll", "_src": "disco"}


async def test_write_fans_out_to_config_resolved_drivers(monkeypatch):
    from dynastore.modules.catalog import collection_router
    from dynastore.modules.storage.routing_config import Operation, OperationDriverEntry

    pg = _RecordingDriver("pg")

    async def _fake_resolve(rpc, operation, catalog_id, collection_id=None, *, db_resource=None):
        if operation == Operation.WRITE:
            return [(OperationDriverEntry(driver_ref="collection_postgresql_driver"), pg)]
        return []

    # no-op INDEX dispatch — exercised in Task 6
    async def _noop_dispatch(*a, **kw):
        return None

    monkeypatch.setattr(collection_router, "resolve_routed", _fake_resolve)
    monkeypatch.setattr(collection_router, "_dispatch_collection_index", _noop_dispatch)
    await collection_router.upsert_collection_metadata("cat", "coll", {"id": "coll"})
    assert pg.calls == ["upsert"]


async def test_search_uses_first_config_resolved_driver(monkeypatch):
    from dynastore.modules.catalog import collection_router
    from dynastore.modules.storage.routing_config import Operation, OperationDriverEntry

    es = _RecordingDriver("es")
    pg = _RecordingDriver("pg")

    async def _fake_resolve(rpc, operation, catalog_id, collection_id=None, *, db_resource=None):
        if operation == Operation.SEARCH:
            return [
                (OperationDriverEntry(driver_ref="collection_elasticsearch_driver"), es),
                (OperationDriverEntry(driver_ref="collection_postgresql_driver"), pg),
            ]
        return []

    monkeypatch.setattr(collection_router, "resolve_routed", _fake_resolve)
    rows, total = await collection_router.search_collection_metadata("cat", q="x")
    assert total == 1 and rows[0]["_src"] == "es"
    assert es.calls == ["search"] and pg.calls == []
