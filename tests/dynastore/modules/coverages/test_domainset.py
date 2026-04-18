from dynastore.modules.coverages.domainset import build_domainset


_STAC_ITEM = {
    "id": "ndvi-2024-01",
    "bbox": [-10.0, 40.0, 0.0, 50.0],
    "properties": {
        "proj:epsg": 4326,
        "proj:transform": [0.01, 0, -10.0, 0, -0.01, 50.0],
        "proj:shape": [1000, 1000],
    },
}


def test_builds_geo_axes_from_proj_fields():
    ds = build_domainset(_STAC_ITEM)
    assert ds["type"] == "DomainSet"
    axes = {a["axisLabel"]: a for a in ds["generalGrid"]["axis"]}
    assert "Lon" in axes and "Lat" in axes
    assert axes["Lon"]["lowerBound"] == -10.0
    assert axes["Lon"]["upperBound"] == 0.0
    assert axes["Lat"]["lowerBound"] == 40.0
    assert axes["Lat"]["upperBound"] == 50.0


def test_falls_back_to_bbox_when_proj_missing():
    item = {"id": "x", "bbox": [0, 0, 1, 1], "properties": {}}
    ds = build_domainset(item)
    axes = {a["axisLabel"]: a for a in ds["generalGrid"]["axis"]}
    assert axes["Lon"]["lowerBound"] == 0 and axes["Lon"]["upperBound"] == 1


def test_adds_time_axis_when_datetime_present():
    item = {
        **_STAC_ITEM,
        "properties": {**_STAC_ITEM["properties"], "datetime": "2024-01-15T00:00:00Z"},
    }
    ds = build_domainset(item)
    axes = {a["axisLabel"]: a for a in ds["generalGrid"]["axis"]}
    assert "Time" in axes
    assert axes["Time"]["lowerBound"] == "2024-01-15T00:00:00Z"


def test_returns_none_when_item_missing():
    assert build_domainset(None) is None
