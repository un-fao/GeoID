_ITEM = {
    "id": "it", "bbox": [0, 0, 1, 1],
    "properties": {"proj:epsg": 4326},
    "assets": {"data": {"raster:bands": [{"name": "b1", "data_type": "uint8"}]}},
}


def test_domainset_helper_returns_generalgrid():
    from dynastore.extensions.coverages.coverages_service import _extract_domainset
    ds = _extract_domainset(_ITEM)
    assert ds["type"] == "DomainSet"


def test_rangetype_helper_returns_datarecord():
    from dynastore.extensions.coverages.coverages_service import _extract_rangetype
    rt = _extract_rangetype(_ITEM)
    assert rt["type"] == "DataRecord"
    assert rt["field"][0]["name"] == "b1"
