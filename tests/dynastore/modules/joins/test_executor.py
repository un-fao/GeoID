#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

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


@pytest.mark.asyncio
async def test_run_join_matches_when_join_key_only_in_model_extra():
    """Regression for #1818: PG-path features arrive with join-column values
    only in model_extra (properties={}). run_join must match them after
    normalize_feature_attributes lifts model_extra into properties."""
    # Simulate PG-driver output: properties empty, join column in model_extra.
    # Use dict-spread so pyright does not flag unknown extra kwargs on Feature.
    primary = _astream([
        Feature(type="Feature", id="f1", geometry=None, properties={}, **{"uid": "a"}),
        Feature(type="Feature", id="f2", geometry=None, properties={}, **{"uid": "missing"}),
    ])
    secondary = {"a": {"user_id": "a", "score": 42}}
    out = [f async for f in run_join(_req(), primary_stream=primary, secondary_index=secondary)]
    # Only "f1" matches; "f2" is correctly dropped (inner-join semantics).
    assert [f.id for f in out] == ["f1"]
    assert (out[0].properties or {})["score"] == 42
    # The join key itself must be visible in the output properties too.
    assert (out[0].properties or {})["uid"] == "a"
