import pytest

from dynastore.modules.storage.drivers.bigquery_stream import row_to_feature


def test_row_without_geometry_column_becomes_feature_with_null_geom():
    row = {"id": "a", "name": "Alice", "value": 42}
    feat = row_to_feature(row, id_column="id", geometry_column=None)
    assert feat.id == "a"
    assert feat.properties == {"name": "Alice", "value": 42}
    assert feat.geometry is None


def test_row_with_geojson_geometry_column_is_preserved():
    row = {
        "id": "a",
        "name": "Alice",
        "geom": {"type": "Point", "coordinates": [1.0, 2.0]},
    }
    feat = row_to_feature(row, id_column="id", geometry_column="geom")
    assert feat.geometry is not None
    dumped = feat.geometry.model_dump()
    assert dumped["type"] == "Point"
    # geojson_pydantic normalises coordinates to a tuple
    assert tuple(dumped["coordinates"]) == (1.0, 2.0)
    assert "geom" not in (feat.properties or {})


def test_row_with_wkt_geometry_is_parsed():
    row = {"id": "a", "geom": "POINT(1 2)"}
    feat = row_to_feature(row, id_column="id", geometry_column="geom")
    assert feat.geometry is not None
    dumped = feat.geometry.model_dump()
    assert dumped["type"] == "Point"
    assert tuple(dumped["coordinates"]) == (1.0, 2.0)


def test_missing_id_column_raises():
    with pytest.raises(KeyError):
        row_to_feature({"name": "nope"}, id_column="id", geometry_column=None)


@pytest.mark.asyncio
async def test_paged_stream_calls_query_once_per_page():
    from dynastore.modules.storage.drivers.bigquery_stream import paged_feature_stream

    calls = []

    async def fake_execute_query(query, project_id):
        calls.append(query)
        if "OFFSET 0" in query:
            return [{"id": "a"}, {"id": "b"}]
        return []

    feats = []
    async for feat in paged_feature_stream(
        fake_execute_query,
        project_id="p",
        base_query="SELECT id FROM p.d.t",
        id_column="id",
        geometry_column=None,
        page_size=2,
        max_items=100,
    ):
        feats.append(feat)

    assert [f.id for f in feats] == ["a", "b"]
    assert any("OFFSET 0" in c for c in calls)
    assert any("OFFSET 2" in c for c in calls)


@pytest.mark.asyncio
async def test_paged_stream_honors_max_items():
    from dynastore.modules.storage.drivers.bigquery_stream import paged_feature_stream

    async def fake(query, project_id):
        return [{"id": "x"}]

    feats = [
        f
        async for f in paged_feature_stream(
            fake,
            project_id="p",
            base_query="SELECT id FROM t",
            id_column="id",
            geometry_column=None,
            page_size=1,
            max_items=3,
        )
    ]
    assert len(feats) == 3
