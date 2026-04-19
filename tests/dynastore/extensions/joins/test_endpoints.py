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
async def test_execute_join_pr1_returns_empty_feature_collection():
    from unittest.mock import MagicMock
    from fastapi import Request
    from dynastore.modules.joins.models import (
        JoinRequest,
        JoinSpec,
        NamedSecondarySpec,
    )

    svc = _build_service()
    req = MagicMock(spec=Request)
    body = JoinRequest(
        secondary=NamedSecondarySpec(ref="some-collection"),
        join=JoinSpec(primary_column="uid", secondary_column="user_id"),
    )
    resp = await svc.execute_join("c", "l", req, body=body)
    assert resp["type"] == "FeatureCollection"
    assert resp["features"] == []


@pytest.mark.asyncio
async def test_execute_join_bigquery_materializes_secondary(monkeypatch):
    from unittest.mock import MagicMock
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

    import dynastore.modules.joins.bq_secondary as bq_mod
    monkeypatch.setattr(bq_mod, "stream_bigquery_secondary", fake_stream)

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
    assert "Secondary materialized: 1 rows" in resp["_phase4b_pr2_note"]


@pytest.mark.asyncio
async def test_execute_join_named_still_returns_pr1_stub():
    from unittest.mock import MagicMock
    from fastapi import Request
    from dynastore.extensions.joins.joins_service import JoinsService
    from dynastore.modules.joins.models import (
        JoinRequest, JoinSpec, NamedSecondarySpec,
    )

    svc = JoinsService()
    req = MagicMock(spec=Request)
    body = JoinRequest(
        secondary=NamedSecondarySpec(ref="some-collection"),
        join=JoinSpec(primary_column="uid", secondary_column="user_id"),
    )
    resp = await svc.execute_join("c", "l", req, body=body)
    assert resp["type"] == "FeatureCollection"
    assert "_phase4b_pr1_note" in resp
