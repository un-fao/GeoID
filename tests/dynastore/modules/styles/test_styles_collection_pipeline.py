import pytest

from dynastore.modules.styles.collection_pipeline import StylesCollectionPipeline
from dynastore.modules.styles.encodings import MEDIA_TYPE_MAPBOX_GL


async def _loader_empty(catalog_id, collection_id):
    return ({}, None, None)


async def _loader_default_coverages(catalog_id, collection_id):
    return (
        {"style-a": [{"content": {"format": "MapboxGL"}}]},
        "style-a",  # coverages default
        None,
    )


async def _broken_loader(catalog_id, collection_id):
    raise RuntimeError("boom")


@pytest.mark.asyncio
async def test_no_merge_when_no_default():
    pipeline = StylesCollectionPipeline(
        loader=_loader_empty, base_url="http://ex",
    )
    coll = {"id": "col", "title": "X"}
    out = await pipeline.apply("cat1", "col", coll, {})
    assert out == coll
    assert "item_assets" not in (out or {})


@pytest.mark.asyncio
async def test_adds_default_style_when_none_existing():
    pipeline = StylesCollectionPipeline(
        loader=_loader_default_coverages, base_url="http://ex",
    )
    coll = {"id": "col"}
    out = await pipeline.apply("cat1", "col", coll, {})
    assert out is not None
    asset = out["item_assets"]["default_style"]
    assert asset["id"] == "style-a"
    assert asset["type"] == MEDIA_TYPE_MAPBOX_GL
    assert "style" in asset["roles"]
    assert asset["href"].endswith("/styles/cat1/collections/col/styles/style-a") \
        or asset["href"].endswith("/styles/catalogs/cat1/collections/col/styles/style-a")


@pytest.mark.asyncio
async def test_existing_default_is_preserved():
    pipeline = StylesCollectionPipeline(
        loader=_loader_default_coverages, base_url="http://ex",
    )
    coll = {
        "id": "col",
        "item_assets": {"default_style": {"preserve": "me"}},
    }
    out = await pipeline.apply("cat1", "col", coll, {})
    assert out is not None
    # Caller-provided default survives.
    assert out["item_assets"]["default_style"] == {"preserve": "me"}


@pytest.mark.asyncio
async def test_does_not_mutate_input():
    pipeline = StylesCollectionPipeline(
        loader=_loader_default_coverages, base_url="http://ex",
    )
    coll = {"id": "col"}
    await pipeline.apply("cat1", "col", coll, {})
    assert coll == {"id": "col"}  # untouched


@pytest.mark.asyncio
async def test_loader_failure_returns_input_unchanged():
    pipeline = StylesCollectionPipeline(
        loader=_broken_loader, base_url="http://ex",
    )
    coll = {"id": "col"}
    out = await pipeline.apply("cat1", "col", coll, {})
    assert out == coll


@pytest.mark.asyncio
async def test_unknown_format_does_not_produce_default():
    async def loader(cat, col):
        return (
            {"style-bogus": [{"content": {"format": "NotReal"}}]},
            "style-bogus",
            None,
        )
    pipeline = StylesCollectionPipeline(loader=loader, base_url="http://ex")
    coll = {"id": "col"}
    out = await pipeline.apply("cat1", "col", coll, {})
    # default_style_id resolves to style-bogus but no encoding maps — skip.
    assert "item_assets" not in out


@pytest.mark.asyncio
async def test_caller_item_assets_id_feeds_precedence():
    # Collection already declares default_style with an id. Loader reports
    # neither a coverages default nor its own item_assets hint. The
    # resolver should see the caller's id and keep it as-is.
    async def loader(cat, col):
        return (
            {"style-caller": [{"content": {"format": "MapboxGL"}}]},
            None,  # no coverages default
            None,  # no loader item_assets default
        )
    pipeline = StylesCollectionPipeline(loader=loader, base_url="http://ex")
    coll = {
        "id": "col",
        "item_assets": {"default_style": {"id": "style-caller", "href": "old"}},
    }
    out = await pipeline.apply("cat1", "col", coll, {})
    # Caller's default_style entry survives untouched (additive merge).
    assert out["item_assets"]["default_style"]["href"] == "old"
