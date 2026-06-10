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

"""Unit tests for OGC API 3D GeoVolumes response models (Task 2.1)."""

from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# Task 2.1 — ThreeDContainer wire shape (spec test from the task description)
# ---------------------------------------------------------------------------


def test_container_wire_shape():
    from dynastore.extensions.geovolumes.geovolumes_models import (
        ContentExtent,
        ContentLink,
        ThreeDContainer,
    )

    c = ThreeDContainer(
        id="denhaag",
        title="Den Haag LoD2",
        collectionType="3dcontainer",
        contentExtent=ContentExtent(
            bbox=[4.27, 52.06, 0.0, 4.32, 52.09, 80.0]
        ),
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


def test_content_extent_default_crs():
    from dynastore.extensions.geovolumes.geovolumes_models import ContentExtent

    ext = ContentExtent(bbox=[0.0, 0.0, 0.0, 1.0, 1.0, 10.0])
    assert ext.crs == "http://www.opengis.net/def/crs/OGC/0/CRS84h"


def test_content_extent_custom_crs():
    from dynastore.extensions.geovolumes.geovolumes_models import ContentExtent

    ext = ContentExtent(
        bbox=[0.0, 0.0, 0.0, 1.0, 1.0, 10.0],
        crs="http://www.opengis.net/def/crs/EPSG/0/4979",
    )
    assert ext.crs == "http://www.opengis.net/def/crs/EPSG/0/4979"


def test_content_link_required_fields():
    from dynastore.extensions.geovolumes.geovolumes_models import ContentLink

    lk = ContentLink(
        rel="alternate",
        href="https://example.com/data.city.jsonl",
        type="application/city+json",
    )
    wire = lk.model_dump(exclude_none=True)
    assert wire["rel"] == "alternate"
    assert wire["href"] == "https://example.com/data.city.jsonl"
    assert "title" not in wire  # None excluded


def test_child_ref_wire_shape():
    from dynastore.extensions.geovolumes.geovolumes_models import ChildRef

    ref = ChildRef(id="amsterdam", title="Amsterdam", collectionType="3dcontainer")
    wire = ref.model_dump(exclude_none=True)
    assert wire["id"] == "amsterdam"
    assert wire["collectionType"] == "3dcontainer"


def test_threedcontainer_optional_fields_excluded():
    from dynastore.extensions.geovolumes.geovolumes_models import (
        ContentExtent,
        ThreeDContainer,
    )

    c = ThreeDContainer(
        id="x",
        collectionType="3dcontainer",
        contentExtent=ContentExtent(bbox=[0.0, 0.0, 0.0, 1.0, 1.0, 5.0]),
    )
    wire = c.model_dump(exclude_none=True)
    assert "title" not in wire
    assert "children" not in wire
    assert "content" not in wire


# ---------------------------------------------------------------------------
# Bbox helper — _parse_bbox
# ---------------------------------------------------------------------------


def test_parse_bbox_4_numbers():
    from dynastore.extensions.geovolumes.geovolumes_models import _parse_bbox

    result = _parse_bbox("1.0,2.0,3.0,4.0")
    assert result == (1.0, 2.0, None, 3.0, 4.0, None)


def test_parse_bbox_6_numbers():
    from dynastore.extensions.geovolumes.geovolumes_models import _parse_bbox

    result = _parse_bbox("1.0,2.0,0.0,3.0,4.0,10.0")
    assert result == (1.0, 2.0, 0.0, 3.0, 4.0, 10.0)


def test_parse_bbox_malformed_raises():
    from dynastore.extensions.geovolumes.geovolumes_models import _parse_bbox

    with pytest.raises(ValueError):
        _parse_bbox("1.0,2.0,3.0")  # arity 3 is invalid

    with pytest.raises(ValueError):
        _parse_bbox("a,b,c,d")  # non-numeric


def test_bbox_intersects_2d():
    from dynastore.extensions.geovolumes.geovolumes_models import _bbox_intersects

    # [minx, miny, zmin, maxx, maxy, zmax] — None means 2D filter
    assert _bbox_intersects(
        container_bbox=[0.0, 0.0, 0.0, 10.0, 10.0, 50.0],
        filter_bbox=(5.0, 5.0, None, 15.0, 15.0, None),
    )
    assert not _bbox_intersects(
        container_bbox=[0.0, 0.0, 0.0, 1.0, 1.0, 5.0],
        filter_bbox=(5.0, 5.0, None, 10.0, 10.0, None),
    )


def test_bbox_intersects_3d():
    from dynastore.extensions.geovolumes.geovolumes_models import _bbox_intersects

    # Overlapping horizontally but z ranges don't intersect
    assert not _bbox_intersects(
        container_bbox=[0.0, 0.0, 0.0, 10.0, 10.0, 5.0],
        filter_bbox=(0.0, 0.0, 10.0, 10.0, 10.0, 20.0),
    )
    # Fully overlapping in 3D
    assert _bbox_intersects(
        container_bbox=[0.0, 0.0, 0.0, 10.0, 10.0, 20.0],
        filter_bbox=(5.0, 5.0, 5.0, 8.0, 8.0, 15.0),
    )
