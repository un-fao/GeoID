import pytest

from dynastore.models.protocols.asset_contrib import ResourceRef
from dynastore.modules.styles.encodings import (
    MEDIA_TYPE_MAPBOX_GL,
    MEDIA_TYPE_SLD_11,
)
from dynastore.modules.styles.link_contrib import StylesLinkContributor


def _ref(**overrides) -> ResourceRef:
    defaults = dict(
        catalog_id="cat1",
        collection_id="col1",
        item_id=None,
        base_url="http://ex",
    )
    defaults.update(overrides)
    return ResourceRef(**defaults)


async def _loader_none_none(ref):
    return ({}, None, None)


async def _loader_with_default(ref):
    # Two stylesheets on one style, coverages config selects it as default.
    return (
        {
            "style-a": [
                {"content": {"format": "MapboxGL"}},
                {"content": {"format": "SLD_1.1"}},
            ],
        },
        "style-a",   # coverages config default
        None,
    )


@pytest.mark.asyncio
async def test_styles_list_link_is_always_emitted():
    contributor = StylesLinkContributor(loader=_loader_none_none)
    links = [l async for l in contributor.contribute_links(_ref())]
    rels = [l.rel for l in links]
    assert "styles" in rels
    styles_link = next(l for l in links if l.rel == "styles")
    assert styles_link.anchor == "resource_root"
    assert styles_link.href.endswith(
        "/styles/catalogs/cat1/collections/col1/styles"
    )


@pytest.mark.asyncio
async def test_no_style_link_when_no_default():
    contributor = StylesLinkContributor(loader=_loader_none_none)
    links = [l async for l in contributor.contribute_links(_ref())]
    assert [l.rel for l in links] == ["styles"]  # only the list link


@pytest.mark.asyncio
async def test_one_style_link_per_encoding_when_default_set():
    contributor = StylesLinkContributor(loader=_loader_with_default)
    links = [l async for l in contributor.contribute_links(_ref())]
    style_links = [l for l in links if l.rel == "style"]
    assert len(style_links) == 2
    assert {l.media_type for l in style_links} == {
        MEDIA_TYPE_MAPBOX_GL, MEDIA_TYPE_SLD_11,
    }
    # All style links anchor at data_asset (OGC_STYLES.md recommendation).
    assert all(l.anchor == "data_asset" for l in style_links)


@pytest.mark.asyncio
async def test_style_link_href_points_at_style_doc():
    contributor = StylesLinkContributor(loader=_loader_with_default)
    links = [l async for l in contributor.contribute_links(_ref())]
    style_links = [l for l in links if l.rel == "style"]
    expected = "http://ex/styles/catalogs/cat1/collections/col1/styles/style-a"
    assert all(l.href == expected for l in style_links)


@pytest.mark.asyncio
async def test_unknown_format_is_skipped():
    async def loader(ref):
        return (
            {
                "style-a": [
                    {"content": {"format": "NotARealFormat"}},
                    {"content": {"format": "MapboxGL"}},
                ],
            },
            "style-a",
            None,
        )
    contributor = StylesLinkContributor(loader=loader)
    links = [l async for l in contributor.contribute_links(_ref())]
    style_links = [l for l in links if l.rel == "style"]
    # Only the known format becomes a link.
    assert len(style_links) == 1
    assert style_links[0].media_type == MEDIA_TYPE_MAPBOX_GL


@pytest.mark.asyncio
async def test_loader_exception_keeps_list_link():
    async def broken_loader(ref):
        raise RuntimeError("boom")
    contributor = StylesLinkContributor(loader=broken_loader)
    links = [l async for l in contributor.contribute_links(_ref())]
    # Styles list link survives; no style links but no crash either.
    assert [l.rel for l in links] == ["styles"]


@pytest.mark.asyncio
async def test_priority_attribute_present():
    contributor = StylesLinkContributor(loader=_loader_none_none)
    assert contributor.priority == 100
