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

"""Unit tests for OGC API - 3D GeoVolumes 22-029 wire models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _imports():
    from dynastore.extensions.geovolumes.geovolumes_models import (
        ThreeDContainer,
        ContentLink,
        ContentExtent,
        ChildRef,
    )
    return ThreeDContainer, ContentLink, ContentExtent, ChildRef


# ---------------------------------------------------------------------------
# ContentExtent
# ---------------------------------------------------------------------------


def test_content_extent_defaults_crs():
    _, _, ContentExtent, _ = _imports()
    ext = ContentExtent(bbox=[4.27, 52.06, 0.0, 4.32, 52.09, 80.0])
    assert ext.crs == "http://www.opengis.net/def/crs/OGC/0/CRS84h"


def test_content_extent_custom_crs():
    _, _, ContentExtent, _ = _imports()
    ext = ContentExtent(
        bbox=[0.0, 0.0, 0.0, 1.0, 1.0, 100.0],
        crs="http://www.opengis.net/def/crs/EPSG/0/4979",
    )
    assert ext.crs == "http://www.opengis.net/def/crs/EPSG/0/4979"


def test_content_extent_rejects_4_element_bbox():
    _, _, ContentExtent, _ = _imports()
    with pytest.raises(ValidationError):
        ContentExtent(bbox=[4.27, 52.06, 4.32, 52.09])


def test_content_extent_rejects_7_element_bbox():
    _, _, ContentExtent, _ = _imports()
    with pytest.raises(ValidationError):
        ContentExtent(bbox=[4.27, 52.06, 0.0, 4.32, 52.09, 80.0, 99.9])


def test_content_extent_rejects_empty_bbox():
    _, _, ContentExtent, _ = _imports()
    with pytest.raises(ValidationError):
        ContentExtent(bbox=[])


def test_content_extent_wire_shape():
    _, _, ContentExtent, _ = _imports()
    ext = ContentExtent(bbox=[4.27, 52.06, 0.0, 4.32, 52.09, 80.0])
    wire = ext.model_dump(exclude_none=True)
    assert len(wire["bbox"]) == 6
    assert wire["bbox"][2] == 0.0
    assert wire["bbox"][5] == 80.0
    assert "crs" in wire


# ---------------------------------------------------------------------------
# ContentLink
# ---------------------------------------------------------------------------


def test_content_link_required_fields():
    _, ContentLink, _, _ = _imports()
    link = ContentLink(
        rel="http://www.opengis.net/def/rel/ogc/1.0/3dtiles",
        href="https://x/tileset.json",
        type="application/json+3dtiles",
    )
    assert link.rel.startswith("http://www.opengis.net")
    assert link.type == "application/json+3dtiles"


def test_content_link_optional_title_absent_by_default():
    _, ContentLink, _, _ = _imports()
    link = ContentLink(
        rel="alternate",
        href="https://x/tileset.json",
        type="application/json",
    )
    wire = link.model_dump(exclude_none=True)
    assert "title" not in wire


def test_content_link_with_title():
    _, ContentLink, _, _ = _imports()
    link = ContentLink(
        rel="alternate",
        href="https://x/tileset.json",
        type="application/json",
        title="My tileset",
    )
    wire = link.model_dump(exclude_none=True)
    assert "title" in wire


# ---------------------------------------------------------------------------
# ChildRef
# ---------------------------------------------------------------------------


def test_child_ref_minimal():
    _, _, _, ChildRef = _imports()
    ref = ChildRef(id="quarter-north", collectionType="3dcontainer")
    assert ref.id == "quarter-north"
    assert ref.collectionType == "3dcontainer"


def test_child_ref_title_optional():
    _, _, _, ChildRef = _imports()
    ref = ChildRef(id="quarter-north", collectionType="3dcontainer")
    wire = ref.model_dump(exclude_none=True)
    assert "title" not in wire


def test_child_ref_with_title():
    _, _, _, ChildRef = _imports()
    ref = ChildRef(id="quarter-north", title="Northern Quarter", collectionType="3dcontainer")
    wire = ref.model_dump(exclude_none=True)
    assert wire["title"] == "Northern Quarter"


# ---------------------------------------------------------------------------
# ThreeDContainer
# ---------------------------------------------------------------------------


def test_container_wire_shape():
    ThreeDContainer, ContentLink, ContentExtent, _ = _imports()
    c = ThreeDContainer(
        id="denhaag",
        title="Den Haag LoD2",
        collectionType="3dcontainer",
        contentExtent=ContentExtent(bbox=[4.27, 52.06, 0.0, 4.32, 52.09, 80.0]),
        content=[
            ContentLink(
                rel="http://www.opengis.net/def/rel/ogc/1.0/3dtiles",
                href="https://x/tileset.json",
                type="application/json+3dtiles",
            )
        ],
        links=[],
        children=[],
    )
    wire = c.model_dump(exclude_none=True)
    assert wire["collectionType"] == "3dcontainer"
    assert wire["contentExtent"]["bbox"][2] == 0.0
    assert len(wire["contentExtent"]["bbox"]) == 6
    assert wire["contentExtent"]["crs"] == "http://www.opengis.net/def/crs/OGC/0/CRS84h"


def test_container_defaults():
    ThreeDContainer, _, _, _ = _imports()
    c = ThreeDContainer(id="test")
    assert c.collectionType == "3dcontainer"
    assert c.content == []
    assert c.children == []
    assert c.links == []


def test_container_title_optional():
    ThreeDContainer, _, _, _ = _imports()
    c = ThreeDContainer(id="test")
    wire = c.model_dump(exclude_none=True)
    assert "title" not in wire
    assert "contentExtent" not in wire


def test_container_exclude_none_omits_unset_optionals():
    ThreeDContainer, ContentLink, ContentExtent, _ = _imports()
    c = ThreeDContainer(
        id="minimal",
        collectionType="3dcontainer",
    )
    wire = c.model_dump(exclude_none=True)
    # Only non-None keys present
    assert "id" in wire
    assert "collectionType" in wire
    # Optional fields with None values absent
    assert "title" not in wire
    assert "contentExtent" not in wire


def test_container_with_children():
    ThreeDContainer, _, _, ChildRef = _imports()
    c = ThreeDContainer(
        id="city",
        children=[
            ChildRef(id="district-a", title="District A", collectionType="3dcontainer"),
            ChildRef(id="district-b", collectionType="3dcontainer"),
        ],
    )
    assert len(c.children) == 2
    wire = c.model_dump(exclude_none=True)
    assert wire["children"][0]["id"] == "district-a"
    # district-b has no title; exclude_none should omit it
    assert "title" not in wire["children"][1]


def test_container_links_use_shared_link_model():
    """The links field reuses dynastore.models.shared_models.Link."""
    ThreeDContainer, _, _, _ = _imports()
    from dynastore.models.shared_models import Link

    c = ThreeDContainer(
        id="test",
        links=[Link(rel="self", href="https://example.com/geovolumes/test")],
    )
    assert len(c.links) == 1
    assert c.links[0].rel == "self"
