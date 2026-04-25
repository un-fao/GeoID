"""Unit tests for modules/styles/db.py — pure helper functions (no DB required)."""

import uuid
from unittest.mock import MagicMock

import pytest

from dynastore.modules.styles.db import _enrich_style_from_row
from dynastore.modules.styles.models import Style, StyleSheet


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

MAPBOX_CONTENT = {
    "format": "MapboxGL",
    "version": 8,
    "sources": {"src": {"type": "geojson", "data": {}}},
    "layers": [],
}

SLD_CONTENT = {
    "format": "SLD_1.1",
    "sld_body": (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<StyledLayerDescriptor version="1.1.0" xmlns="http://www.opengis.net/sld"/>'
    ),
}


def _row(
    *,
    catalog_id: str = "cat1",
    collection_id: str = "col1",
    style_id: str = "style1",
    title: str = "Test Style",
    stylesheets=None,
) -> dict:
    if stylesheets is None:
        stylesheets = [{"content": MAPBOX_CONTENT}]
    return {
        "id": uuid.uuid4(),
        "catalog_id": catalog_id,
        "collection_id": collection_id,
        "style_id": style_id,
        "title": title,
        "description": None,
        "keywords": None,
        "stylesheets": stylesheets,
        "created_at": "2025-01-01T00:00:00Z",
        "updated_at": "2025-01-01T00:00:00Z",
    }


# ---------------------------------------------------------------------------
# _enrich_style_from_row — href correctness (Bug 1 regression)
# ---------------------------------------------------------------------------


def test_enrich_stylesheet_href_points_to_stylesheet_endpoint():
    """Stylesheet href must end in /stylesheet, not /styleSheet{i}."""
    row = _row()
    style = _enrich_style_from_row(row, root_url="http://ex")
    assert style is not None
    for sheet in style.stylesheets:
        assert "/stylesheet" in sheet.link.href
        assert "styleSheet0" not in sheet.link.href
        assert "styleSheet1" not in sheet.link.href


def test_enrich_stylesheet_href_full_path():
    row = _row(catalog_id="cat1", collection_id="col1", style_id="s1")
    style = _enrich_style_from_row(row, root_url="http://ex")
    expected = "http://ex/styles/catalogs/cat1/collections/col1/styles/s1/stylesheet"
    for sheet in style.stylesheets:
        assert sheet.link.href == expected


def test_enrich_self_link():
    row = _row(catalog_id="cat1", collection_id="col1", style_id="s1")
    style = _enrich_style_from_row(row, root_url="http://ex")
    self_links = [lnk for lnk in style.links if lnk.rel == "self"]
    assert len(self_links) == 1
    assert self_links[0].href == "http://ex/styles/catalogs/cat1/collections/col1/styles/s1"


def test_enrich_empty_row_returns_none():
    assert _enrich_style_from_row({}) is None
    assert _enrich_style_from_row(None) is None


def test_enrich_multiple_stylesheets_all_point_to_same_endpoint():
    """All encodings share one content-negotiated /stylesheet endpoint."""
    row = _row(
        stylesheets=[
            {"content": MAPBOX_CONTENT},
            {"content": SLD_CONTENT},
        ]
    )
    style = _enrich_style_from_row(row, root_url="")
    hrefs = {sheet.link.href for sheet in style.stylesheets}
    # Both must point to the same /stylesheet endpoint (content negotiation handles format)
    assert len(hrefs) == 1
    assert "/stylesheet" in hrefs.pop()


def test_enrich_without_root_url():
    row = _row()
    style = _enrich_style_from_row(row)
    assert style is not None
    for sheet in style.stylesheets:
        assert sheet.link.href.endswith("/stylesheet")


def test_enrich_returns_style_instance():
    row = _row()
    result = _enrich_style_from_row(row)
    assert isinstance(result, Style)


def test_enrich_preserves_title():
    row = _row(title="My Layer Style")
    style = _enrich_style_from_row(row)
    assert style.title == "My Layer Style"


def test_enrich_sld_content():
    row = _row(stylesheets=[{"content": SLD_CONTENT}])
    style = _enrich_style_from_row(row)
    assert len(style.stylesheets) == 1
    from dynastore.modules.styles.models import SLDContent
    assert isinstance(style.stylesheets[0].content, SLDContent)
