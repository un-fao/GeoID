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
