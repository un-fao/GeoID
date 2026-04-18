from dynastore.modules.coverages.rangetype import build_rangetype


_ITEM_WITH_BANDS = {
    "assets": {
        "data": {
            "raster:bands": [
                {"name": "red",   "data_type": "uint16", "unit": "dn", "nodata": 0},
                {"name": "green", "data_type": "uint16", "unit": "dn", "nodata": 0},
            ]
        }
    }
}


def test_builds_two_field_record():
    rt = build_rangetype(_ITEM_WITH_BANDS)
    assert rt["type"] == "DataRecord"
    names = [f["name"] for f in rt["field"]]
    assert names == ["red", "green"]
    assert rt["field"][0]["definition"].endswith("#uint16") or "unsignedShort" in rt["field"][0]["definition"]
    assert rt["field"][0]["nilValues"][0]["value"] == "0"
    assert rt["field"][0]["uom"]["code"] == "dn"


def test_empty_when_no_bands():
    assert build_rangetype({"assets": {}}) == {"type": "DataRecord", "field": []}


def test_none_item_returns_none():
    assert build_rangetype(None) is None
