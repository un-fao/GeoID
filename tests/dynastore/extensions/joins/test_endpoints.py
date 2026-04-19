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
