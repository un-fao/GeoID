import pytest

from dynastore.extensions.joins.joins_service import JoinsService


def _build_service():
    return JoinsService()


@pytest.mark.asyncio
async def test_describe_lists_registered_driver():
    from unittest.mock import MagicMock
    from fastapi import Request

    svc = _build_service()
    req = MagicMock(spec=Request)
    req.url = "http://ex/join/catalogs/c/collections/l/join"
    payload = await svc.describe_join("c", "l", req)
    assert "registered" in payload["supported_secondary_drivers"]
    assert "bigquery" in payload["supported_secondary_drivers"]
    assert payload["primary"]["catalog"] == "c"




@pytest.mark.asyncio
async def test_execute_join_bigquery_materializes_secondary(monkeypatch):
    from unittest.mock import AsyncMock, MagicMock
    from fastapi import Request
    from dynastore.extensions.joins.joins_service import JoinsService
    from dynastore.modules.joins.models import (
        BigQuerySecondarySpec, JoinRequest, JoinSpec,
    )
    from dynastore.modules.storage.drivers.bigquery_models import BigQueryTarget

    # Patch the bq_secondary streamer so we don't hit real BigQuery.
    async def fake_stream(spec, *, secondary_column, **kwargs):
        from dynastore.models.ogc import Feature
        yield Feature(type="Feature", id="r1", geometry=None,
                      properties={"user_id": "alice", "score": 42})

    import dynastore.extensions.joins.joins_service as svc_mod
    monkeypatch.setattr(svc_mod, "stream_bigquery_secondary", fake_stream)

    # Provide a primary driver so the request runs through run_join; use a
    # non-matching primary so the join yields zero features but the
    # secondary-materialization counter is still reported.
    class _EmptyPrimary:
        async def read_entities(self, *args, **kwargs):
            if False:
                yield  # pragma: no cover — empty stream

    fake_resolved = type("R", (), {"driver": _EmptyPrimary()})()
    import dynastore.extensions.joins.joins_service as svc_mod
    monkeypatch.setattr(
        svc_mod, "resolve_drivers", AsyncMock(return_value=[fake_resolved]),
    )

    svc = JoinsService()
    req = MagicMock(spec=Request)
    body = JoinRequest(
        secondary=BigQuerySecondarySpec(
            target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
        ),
        join=JoinSpec(primary_column="uid", secondary_column="user_id"),
    )
    resp = await svc.execute_join("c", "l", req, body=body)
    assert resp["type"] == "FeatureCollection"
    assert resp["_join_meta"]["secondary_rows_materialized"] == 1


@pytest.mark.asyncio
async def test_execute_join_bigquery_end_to_end(monkeypatch):
    """Primary stream + BQ secondary materialization + dict join → real FeatureCollection."""
    from unittest.mock import AsyncMock, MagicMock
    from fastapi import Request
    from dynastore.extensions.joins.joins_service import JoinsService
    from dynastore.modules.joins.models import (
        BigQuerySecondarySpec, JoinRequest, JoinSpec,
    )
    from dynastore.modules.storage.drivers.bigquery_models import BigQueryTarget
    from dynastore.models.ogc import Feature

    # Fake BQ secondary stream.
    async def fake_bq_stream(spec, *, secondary_column, **kwargs):
        yield Feature(type="Feature", id="bq1", geometry=None,
                      properties={"user_id": "alice", "score": 42})
        yield Feature(type="Feature", id="bq2", geometry=None,
                      properties={"user_id": "bob", "score": 7})

    import dynastore.extensions.joins.joins_service as svc_mod
    monkeypatch.setattr(svc_mod, "stream_bigquery_secondary", fake_bq_stream)

    # Fake primary driver returning two features matching one BQ row.
    class _FakePrimaryDriver:
        async def read_entities(self, *args, **kwargs):
            yield Feature(type="Feature", id="p1", geometry=None,
                          properties={"uid": "alice", "name": "Alice"})
            yield Feature(type="Feature", id="p2", geometry=None,
                          properties={"uid": "carol", "name": "Carol"})

    fake_resolved = type("R", (), {"driver": _FakePrimaryDriver()})()
    import dynastore.extensions.joins.joins_service as svc_mod
    monkeypatch.setattr(
        svc_mod, "resolve_drivers", AsyncMock(return_value=[fake_resolved]),
    )

    svc = JoinsService()
    req = MagicMock(spec=Request)
    body = JoinRequest(
        secondary=BigQuerySecondarySpec(
            target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
        ),
        join=JoinSpec(primary_column="uid", secondary_column="user_id"),
    )
    resp = await svc.execute_join("c", "l", req, body=body)
    assert resp["type"] == "FeatureCollection"
    assert len(resp["features"]) == 1  # only Alice matches
    assert resp["features"][0]["properties"]["score"] == 42
    assert resp["_join_meta"]["secondary_rows_materialized"] == 2
    assert resp["_join_meta"]["joined_features"] == 1


@pytest.mark.asyncio
async def test_execute_join_bigquery_returns_404_when_no_primary_driver(monkeypatch):
    from unittest.mock import AsyncMock, MagicMock
    from fastapi import HTTPException, Request
    from dynastore.extensions.joins.joins_service import JoinsService
    from dynastore.modules.joins.models import (
        BigQuerySecondarySpec, JoinRequest, JoinSpec,
    )
    from dynastore.modules.storage.drivers.bigquery_models import BigQueryTarget

    async def fake_bq_stream(spec, *, secondary_column, **kwargs):
        if False:
            yield  # empty

    import dynastore.extensions.joins.joins_service as svc_mod
    monkeypatch.setattr(svc_mod, "stream_bigquery_secondary", fake_bq_stream)
    monkeypatch.setattr(svc_mod, "resolve_drivers", AsyncMock(return_value=[]))

    svc = JoinsService()
    req = MagicMock(spec=Request)
    body = JoinRequest(
        secondary=BigQuerySecondarySpec(
            target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
        ),
        join=JoinSpec(primary_column="uid", secondary_column="user_id"),
    )
    with pytest.raises(HTTPException) as exc:
        await svc.execute_join("c", "l", req, body=body)
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_execute_join_named_secondary_resolves_via_registry(monkeypatch):
    """Both primary and secondary resolved via resolve_drivers; join executes."""
    from unittest.mock import MagicMock
    from fastapi import Request
    from dynastore.extensions.joins.joins_service import JoinsService
    from dynastore.modules.joins.models import (
        JoinRequest, JoinSpec, NamedSecondarySpec,
    )
    from dynastore.models.ogc import Feature

    class _PrimaryDriver:
        async def read_entities(self, *args, **kwargs):
            yield Feature(type="Feature", id="p1", geometry=None,
                          properties={"uid": "alice", "color": "red"})

    class _SecondaryDriver:
        async def read_entities(self, *args, **kwargs):
            yield Feature(type="Feature", id="s1", geometry=None,
                          properties={"user_id": "alice", "score": 99})

    primary_resolved = type("R", (), {"driver": _PrimaryDriver()})()
    secondary_resolved = type("R", (), {"driver": _SecondaryDriver()})()

    # resolve_drivers is called twice: once for the secondary (with secondary
    # collection_id) and once for the primary. Use a side_effect that returns
    # the right driver per call.
    calls = []
    async def fake_resolve(operation, catalog_id, collection_id=None, **kwargs):
        calls.append((catalog_id, collection_id))
        if collection_id == "the-other-collection":
            return [secondary_resolved]
        return [primary_resolved]

    import dynastore.extensions.joins.joins_service as svc_mod
    monkeypatch.setattr(svc_mod, "resolve_drivers", fake_resolve)

    svc = JoinsService()
    req = MagicMock(spec=Request)
    body = JoinRequest(
        secondary=NamedSecondarySpec(ref="the-other-collection"),
        join=JoinSpec(primary_column="uid", secondary_column="user_id"),
    )
    resp = await svc.execute_join("c", "l", req, body=body)
    assert resp["type"] == "FeatureCollection"
    assert len(resp["features"]) == 1
    assert resp["features"][0]["properties"]["score"] == 99
    assert resp["_join_meta"]["secondary_ref"] == "the-other-collection"


@pytest.mark.asyncio
async def test_execute_join_named_404_when_secondary_missing(monkeypatch):
    from unittest.mock import AsyncMock, MagicMock
    from fastapi import HTTPException, Request
    from dynastore.extensions.joins.joins_service import JoinsService
    from dynastore.modules.joins.models import (
        JoinRequest, JoinSpec, NamedSecondarySpec,
    )

    import dynastore.extensions.joins.joins_service as svc_mod
    monkeypatch.setattr(
        svc_mod, "resolve_drivers", AsyncMock(return_value=[]),
    )

    svc = JoinsService()
    req = MagicMock(spec=Request)
    body = JoinRequest(
        secondary=NamedSecondarySpec(ref="missing"),
        join=JoinSpec(primary_column="uid", secondary_column="user_id"),
    )
    with pytest.raises(HTTPException) as exc:
        await svc.execute_join("c", "l", req, body=body)
    assert exc.value.status_code == 404
