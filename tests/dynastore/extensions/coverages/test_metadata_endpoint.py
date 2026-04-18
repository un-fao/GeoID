def test_metadata_response_shape():
    from dynastore.extensions.coverages.coverages_service import (
        _build_metadata_response,
    )
    item = {
        "id": "it",
        "bbox": [-10, 40, 0, 50],
        "properties": {"proj:epsg": 4326, "datetime": "2024-01-15T00:00:00Z"},
        "assets": {"data": {"raster:bands": [
            {"name": "b1", "data_type": "uint16", "nodata": 0},
        ]}},
    }
    resp = _build_metadata_response(
        item=item,
        base_url="http://ex",
        catalog_id="cat",
        collection_id="col",
        default_style_id="ndvi",
    )
    assert resp["title"] == "it"
    assert resp["domainset"]["type"] == "DomainSet"
    assert resp["rangetype"]["field"][0]["name"] == "b1"
    rels = {lk["rel"] for lk in resp["links"]}
    assert "self" in rels and "data" in rels and "styles" in rels
