import pytest

from dynastore.models.ogc import Feature
from dynastore.modules.joins.executor import index_secondary, run_join
from dynastore.modules.joins.models import (
    JoinRequest,
    JoinSpec,
    NamedSecondarySpec,
    PagingSpec,
    ProjectionSpec,
)


def _feat(fid, **props):
    return Feature(type="Feature", id=fid, geometry=None, properties=props)


async def _astream(items):
    for it in items:
        yield it


def _req(**overrides):
    base = dict(
        secondary=NamedSecondarySpec(ref="x"),
        join=JoinSpec(primary_column="uid", secondary_column="user_id"),
    )
    base.update(overrides)
    return JoinRequest(**base)


@pytest.mark.asyncio
async def test_inner_join_drops_unmatched_features():
    primary = _astream([
        _feat("1", uid="a"),
        _feat("2", uid="b"),
        _feat("3", uid="missing"),
    ])
    secondary = {"a": {"user_id": "a", "score": 1}, "b": {"user_id": "b", "score": 2}}
    out = [f async for f in run_join(_req(), primary_stream=primary, secondary_index=secondary)]
    assert [f.id for f in out] == ["1", "2"]
    assert out[0].properties["score"] == 1


@pytest.mark.asyncio
async def test_enrichment_false_passes_features_unmodified():
    primary = _astream([_feat("1", uid="a")])
    secondary = {"a": {"user_id": "a", "score": 99}}
    req = _req(join=JoinSpec(primary_column="uid", secondary_column="user_id", enrichment=False))
    out = [f async for f in run_join(req, primary_stream=primary, secondary_index=secondary)]
    assert "score" not in out[0].properties


@pytest.mark.asyncio
async def test_projection_attributes_are_filtered():
    primary = _astream([_feat("1", uid="a", color="red", size=10)])
    secondary = {"a": {"score": 1, "name": "alice"}}
    req = _req(projection=ProjectionSpec(with_geometry=False, attributes=["color", "score"]))
    out = [f async for f in run_join(req, primary_stream=primary, secondary_index=secondary)]
    assert set(out[0].properties.keys()) == {"uid", "color", "score"}  # join key kept
    assert out[0].geometry is None


@pytest.mark.asyncio
async def test_paging_limit_and_offset():
    primary = _astream([_feat(str(i), uid=str(i)) for i in range(10)])
    secondary = {str(i): {"user_id": str(i)} for i in range(10)}
    req = _req(paging=PagingSpec(limit=3, offset=2))
    out = [f async for f in run_join(req, primary_stream=primary, secondary_index=secondary)]
    assert [f.id for f in out] == ["2", "3", "4"]


@pytest.mark.asyncio
async def test_index_secondary_drops_rows_with_null_key():
    secondary = _astream([
        _feat("a", user_id="alpha", score=1),
        _feat("b", user_id=None, score=2),
        _feat("c", user_id="gamma", score=3),
    ])
    idx = await index_secondary(secondary, secondary_column="user_id")
    assert set(idx.keys()) == {"alpha", "gamma"}
    assert idx["alpha"]["score"] == 1
