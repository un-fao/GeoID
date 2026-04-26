"""Unit tests for modules.dggs.aggregator."""

import pytest

h3 = pytest.importorskip("h3", reason="h3 package not installed")


from dynastore.modules.dggs.aggregator import aggregate_features, _extract_centroid, _get_properties
from dynastore.modules.dggs.models import DGGSFeatureCollection


def _point_feature(lat: float, lng: float, props: dict = None):
    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [lng, lat]},
        "properties": props or {},
    }


def test_aggregate_empty_list():
    result = aggregate_features([], resolution=5)
    assert isinstance(result, DGGSFeatureCollection)
    assert result.features == []
    assert result.numberReturned == 0


def test_aggregate_single_feature():
    features = [_point_feature(41.88, 12.48, {"value": 10.0})]
    result = aggregate_features(features, resolution=5)
    assert len(result.features) == 1
    zone = result.features[0]
    assert zone.properties.count == 1
    assert "value" in zone.properties.values
    assert zone.properties.values["value"] == pytest.approx(10.0)


def test_aggregate_multiple_features_same_cell():
    # Two points very close together should map to the same H3 cell at resolution 5
    features = [
        _point_feature(41.88, 12.48, {"yield": 100.0}),
        _point_feature(41.881, 12.481, {"yield": 200.0}),
    ]
    result = aggregate_features(features, resolution=5)
    # Both should land in the same cell
    assert len(result.features) == 1
    zone = result.features[0]
    assert zone.properties.count == 2
    assert zone.properties.values["yield"] == pytest.approx(150.0)


def test_aggregate_different_cells():
    # Points far apart should land in different cells at resolution 5
    features = [
        _point_feature(0.0, 0.0),
        _point_feature(45.0, 90.0),
    ]
    result = aggregate_features(features, resolution=5)
    assert len(result.features) == 2


def test_aggregate_parameter_filter():
    features = [_point_feature(41.88, 12.48, {"area": 1000.0, "population": 500.0})]
    result = aggregate_features(features, resolution=5, parameter_names={"area"})
    zone = result.features[0]
    assert "area" in zone.properties.values
    assert "population" not in zone.properties.values


def test_aggregate_non_numeric_properties_ignored():
    features = [_point_feature(41.88, 12.48, {"name": "Rome", "code": "IT"})]
    result = aggregate_features(features, resolution=5)
    zone = result.features[0]
    assert zone.properties.count == 1
    assert zone.properties.values == {}


def test_aggregate_feature_without_geometry():
    feature = {"type": "Feature", "geometry": None, "properties": {}}
    result = aggregate_features([feature], resolution=5)
    assert result.features == []


def test_aggregate_dggs_id_and_level_in_response():
    features = [_point_feature(10.0, 10.0)]
    result = aggregate_features(features, resolution=7, dggs_id="H3")
    assert result.dggsId == "H3"
    assert result.zoneLevel == 7


def test_extract_centroid_point():
    feat = _point_feature(41.88, 12.48)
    lat, lng = _extract_centroid(feat)
    assert lat == pytest.approx(41.88)
    assert lng == pytest.approx(12.48)


def test_extract_centroid_none_geometry():
    feat = {"type": "Feature", "geometry": None, "properties": {}}
    assert _extract_centroid(feat) is None


def test_get_properties_dict():
    feat = _point_feature(0, 0, {"a": 1})
    assert _get_properties(feat) == {"a": 1}


def test_get_properties_missing():
    assert _get_properties({"type": "Feature"}) == {}


# ---------------------------------------------------------------------------
# Sidecar integration: _cell_from_stored_index (H3)
# ---------------------------------------------------------------------------

def test_cell_from_stored_index_h3_valid_bigint():
    from dynastore.modules.dggs.aggregator import _cell_from_stored_index
    from dynastore.modules.dggs.h3_indexer import latlng_to_cell, cell_str_to_int

    resolution = 5
    cell_str = latlng_to_cell(41.88, 12.48, resolution)
    cell_int = cell_str_to_int(cell_str)
    feature = {"type": "Feature", "geometry": None, "properties": {f"h3_res{resolution}": cell_int}}
    result = _cell_from_stored_index(feature, resolution, "H3")
    assert result == cell_str


def test_cell_from_stored_index_h3_missing_key():
    from dynastore.modules.dggs.aggregator import _cell_from_stored_index
    feature = {"type": "Feature", "geometry": None, "properties": {}}
    assert _cell_from_stored_index(feature, 5, "H3") is None


def test_cell_from_stored_index_h3_invalid_value():
    from dynastore.modules.dggs.aggregator import _cell_from_stored_index
    feature = {"type": "Feature", "geometry": None, "properties": {"h3_res5": "not-a-number"}}
    assert _cell_from_stored_index(feature, 5, "H3") is None


def test_aggregate_h3_uses_stored_index_when_present():
    """When h3_res{N} is in properties, the pre-stored index takes precedence over geometry."""
    from dynastore.modules.dggs.h3_indexer import latlng_to_cell, cell_str_to_int

    resolution = 5
    cell_str = latlng_to_cell(41.88, 12.48, resolution)
    cell_int = cell_str_to_int(cell_str)

    # Feature has wrong geometry (North Pole) but correct h3_res5 stored
    feature = {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [0.0, 90.0]},  # North Pole
        "properties": {f"h3_res{resolution}": cell_int},
    }
    result = aggregate_features([feature], resolution=resolution, dggs_id="H3")
    assert len(result.features) == 1
    assert result.features[0].id == cell_str  # stored index wins over geometry


# ---------------------------------------------------------------------------
# Sidecar integration: _cell_from_stored_index (S2)
# ---------------------------------------------------------------------------

s2sphere = pytest.importorskip("s2sphere", reason="s2sphere package not installed")


def test_cell_from_stored_index_s2_valid_bigint():
    from dynastore.modules.dggs.aggregator import _cell_from_stored_index
    from dynastore.modules.dggs.s2_indexer import latlng_to_cell as s2_latlng_to_cell
    from dynastore.modules.dggs.s2_indexer import cell_str_to_int as s2_cell_str_to_int

    level = 10
    cell_str = s2_latlng_to_cell(41.88, 12.48, level)
    cell_int = s2_cell_str_to_int(cell_str)
    feature = {"type": "Feature", "geometry": None, "properties": {f"s2_res{level}": cell_int}}
    result = _cell_from_stored_index(feature, level, "S2")
    assert result == cell_str


def test_cell_from_stored_index_s2_missing_key():
    from dynastore.modules.dggs.aggregator import _cell_from_stored_index
    feature = {"type": "Feature", "geometry": None, "properties": {}}
    assert _cell_from_stored_index(feature, 10, "S2") is None


def test_cell_from_stored_index_s2_wrong_prefix():
    """An h3_res key is ignored when dggs_id='S2'."""
    from dynastore.modules.dggs.aggregator import _cell_from_stored_index
    from dynastore.modules.dggs.h3_indexer import latlng_to_cell, cell_str_to_int

    resolution = 5
    cell_str = latlng_to_cell(41.88, 12.48, resolution)
    cell_int = cell_str_to_int(cell_str)
    feature = {"type": "Feature", "geometry": None, "properties": {f"h3_res{resolution}": cell_int}}
    # dggs_id=S2 → looks for s2_res5, not h3_res5
    assert _cell_from_stored_index(feature, resolution, "S2") is None


def test_aggregate_s2_single_point():
    """S2 aggregation via centroid computation."""
    features = [_point_feature(41.88, 12.48, {"value": 42.0})]
    result = aggregate_features(features, resolution=10, dggs_id="S2")
    assert len(result.features) == 1
    zone = result.features[0]
    assert zone.properties.count == 1
    assert zone.properties.values["value"] == pytest.approx(42.0)
    assert result.dggsId == "S2"
    assert result.zoneLevel == 10


def test_aggregate_s2_uses_stored_index_when_present():
    """When s2_res{N} is in properties, the pre-stored index takes precedence over geometry."""
    from dynastore.modules.dggs.s2_indexer import latlng_to_cell as s2_cell, cell_str_to_int as s2_int

    level = 10
    cell_str = s2_cell(41.88, 12.48, level)
    cell_int = s2_int(cell_str)

    feature = {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [0.0, 90.0]},  # North Pole
        "properties": {f"s2_res{level}": cell_int},
    }
    result = aggregate_features([feature], resolution=level, dggs_id="S2")
    assert len(result.features) == 1
    assert result.features[0].id == cell_str
