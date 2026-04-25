"""Unit tests for modules/styles/models.py — Pydantic model validation."""

import pytest
from pydantic import ValidationError

from dynastore.modules.styles.models import (
    MapboxContent,
    SLDContent,
    StyleCreate,
    StyleFormatEnum,
    StyleSheet,
    StyleSheetCreate,
    StyleUpdate,
)


# ---------------------------------------------------------------------------
# SLDContent
# ---------------------------------------------------------------------------

VALID_SLD = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    '<StyledLayerDescriptor version="1.1.0" xmlns="http://www.opengis.net/sld"/>'
)


def test_sld_content_valid():
    content = SLDContent(sld_body=VALID_SLD)
    assert content.format == StyleFormatEnum.SLD_1_1
    assert "StyledLayerDescriptor" in content.sld_body


def test_sld_content_invalid_xml():
    with pytest.raises(ValidationError):
        SLDContent(sld_body="<not closed")


def test_sld_content_empty_body():
    with pytest.raises(ValidationError):
        SLDContent(sld_body="")


def test_sld_content_html_entities_decoded():
    escaped = VALID_SLD.replace('"', "&quot;")
    content = SLDContent(sld_body=escaped)
    assert '"' in content.sld_body


# ---------------------------------------------------------------------------
# MapboxContent
# ---------------------------------------------------------------------------

VALID_MAPBOX = {
    "format": "MapboxGL",
    "version": 8,
    "sources": {"my-source": {"type": "geojson", "data": {}}},
    "layers": [{"id": "my-layer", "type": "fill", "source": "my-source"}],
}


def test_mapbox_content_valid():
    content = MapboxContent(**VALID_MAPBOX)
    assert content.format == StyleFormatEnum.MAPBOX_GL
    assert content.version == 8
    assert "my-source" in content.sources
    assert len(content.layers) == 1


def test_mapbox_content_extra_fields_allowed():
    data = {**VALID_MAPBOX, "metadata": {"author": "test"}}
    content = MapboxContent(**data)
    assert content.model_dump().get("metadata") == {"author": "test"}


def test_mapbox_content_missing_sources():
    with pytest.raises(ValidationError):
        MapboxContent(format="MapboxGL", version=8, layers=[])


# ---------------------------------------------------------------------------
# StyleSheetCreate — discriminated union
# ---------------------------------------------------------------------------


def test_stylesheet_create_sld_discriminated():
    ss = StyleSheetCreate(content={"format": "SLD_1.1", "sld_body": VALID_SLD})
    assert isinstance(ss.content, SLDContent)


def test_stylesheet_create_mapbox_discriminated():
    ss = StyleSheetCreate(content=VALID_MAPBOX)
    assert isinstance(ss.content, MapboxContent)


def test_stylesheet_create_unknown_format_raises():
    with pytest.raises(ValidationError):
        StyleSheetCreate(content={"format": "UNKNOWN", "data": "x"})


# ---------------------------------------------------------------------------
# StyleCreate
# ---------------------------------------------------------------------------


def test_style_create_valid():
    style = StyleCreate(
        style_id="my-style",
        title="My Style",
        stylesheets=[{"content": VALID_MAPBOX}],
    )
    assert style.style_id == "my-style"
    assert len(style.stylesheets) == 1


def test_style_create_empty_stylesheets_raises():
    with pytest.raises(ValidationError):
        StyleCreate(style_id="x", stylesheets=[])


# ---------------------------------------------------------------------------
# StyleUpdate
# ---------------------------------------------------------------------------


def test_style_update_partial():
    update = StyleUpdate(title="New Title")
    dumped = update.model_dump(exclude_unset=True)
    assert "title" in dumped
    assert "description" not in dumped


def test_style_update_all_optional():
    update = StyleUpdate()
    assert update.title is None
    assert update.description is None
    assert update.stylesheets is None
