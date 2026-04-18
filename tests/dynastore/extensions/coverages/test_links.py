from dynastore.extensions.coverages.links import build_coverage_links


def test_emits_core_rels_without_default_style():
    links = build_coverage_links(
        base_url="http://ex",
        catalog_id="cat", collection_id="col",
        default_style_id=None,
    )
    rels = [link["rel"] for link in links]
    assert "self" in rels
    assert "data" in rels
    assert "describedby" in rels
    assert "styles" in rels
    assert "style" not in rels


def test_emits_default_style_links_when_provided():
    links = build_coverage_links(
        base_url="http://ex",
        catalog_id="cat", collection_id="col",
        default_style_id="ndvi",
    )
    style_links = [link for link in links if link["rel"] == "style"]
    assert len(style_links) >= 2
    map_links = [
        link for link in links
        if link["rel"] == "http://www.opengis.net/def/rel/ogc/1.0/map"
    ]
    assert map_links
    assert "style=ndvi" in map_links[0]["href"]
