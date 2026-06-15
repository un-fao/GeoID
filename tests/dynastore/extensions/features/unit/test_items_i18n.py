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

"""OGC Features get_items / get_item link-title localization.

These tests verify that ``Link.title`` values (stored as ``LocalizedText``
dicts internally) are resolved to a single language string on the wire when
a specific ``lang`` is requested, and that the full multi-language dict is
preserved when ``lang='*'``.

Wire-contract guard: resolving link titles must ONLY change the ``title``
value inside each link — it must never add or drop top-level members on a
``Feature`` or ``FeatureCollection``.
"""
from __future__ import annotations

from dynastore.models.localization import LocalizedText
from dynastore.models.shared_models import Link
from dynastore.extensions.tools.response_i18n import resolve_localized, resolve_links, localize_model


# ---------------------------------------------------------------------------
# resolve_localized
# ---------------------------------------------------------------------------


def test_resolve_localized_default_en():
    lt = LocalizedText(en="Items list", fr="Liste des entités")
    result = resolve_localized(lt, "en")
    assert result == "Items list"


def test_resolve_localized_requested_language():
    lt = LocalizedText(en="Items list", fr="Liste des entités")
    result = resolve_localized(lt, "fr")
    assert result == "Liste des entités"


def test_resolve_localized_fallback_to_en_on_unknown_lang():
    lt = LocalizedText(en="Items list", fr="Liste des entités")
    result = resolve_localized(lt, "de")
    assert result == "Items list"


def test_resolve_localized_wildcard_returns_full_dict():
    lt = LocalizedText(en="Items list", fr="Liste des entités")
    result = resolve_localized(lt, "*")
    assert isinstance(result, dict)
    assert result["en"] == "Items list"
    assert result["fr"] == "Liste des entités"


def test_resolve_localized_plain_str_passthrough():
    """A plain string is returned unchanged — it was never localized."""
    assert resolve_localized("already a string", "en") == "already a string"


def test_resolve_localized_none_returns_none():
    assert resolve_localized(None, "en") is None


def test_resolve_localized_dict_input():
    """Accepts a raw dict (e.g., the output of model_dump) as well as a model."""
    d = {"en": "Items list", "fr": "Liste des entités"}
    assert resolve_localized(d, "fr") == "Liste des entités"
    assert resolve_localized(d, "*") == d


# ---------------------------------------------------------------------------
# resolve_links
# ---------------------------------------------------------------------------


def _make_link(title_en: str, title_fr: str | None = None) -> Link:
    lt = LocalizedText(en=title_en, fr=title_fr) if title_fr else LocalizedText(en=title_en)
    return Link(href="https://example.com/items", rel="self", title=lt)


def test_resolve_links_default_lang_produces_plain_str():
    links = [_make_link("This document as HTML")]
    resolved = resolve_links(links, "en")
    assert resolved is not None
    dumped = [lnk.model_dump(exclude_none=True) if hasattr(lnk, "model_dump") else lnk for lnk in resolved]
    assert dumped[0]["title"] == "This document as HTML"


def test_resolve_links_requested_lang_fr():
    links = [_make_link("Items list", "Liste des entités")]
    resolved = resolve_links(links, "fr")
    assert resolved is not None
    dumped = [lnk.model_dump(exclude_none=True) if hasattr(lnk, "model_dump") else lnk for lnk in resolved]
    assert dumped[0]["title"] == "Liste des entités"


def test_resolve_links_wildcard_preserves_full_dict():
    links = [_make_link("Items list", "Liste des entités")]
    resolved = resolve_links(links, "*")
    assert resolved is not None
    dumped = [lnk.model_dump(exclude_none=True) if hasattr(lnk, "model_dump") else lnk for lnk in resolved]
    assert isinstance(dumped[0]["title"], dict)
    assert dumped[0]["title"]["en"] == "Items list"
    assert dumped[0]["title"]["fr"] == "Liste des entités"


def test_resolve_links_no_title_link_unchanged():
    link = Link(href="https://example.com", rel="collection")
    resolved = resolve_links([link], "en")
    assert resolved is not None
    assert resolved[0] is link


def test_resolve_links_empty_returns_empty():
    assert resolve_links([], "en") == []


def test_resolve_links_none_returns_none():
    assert resolve_links(None, "en") is None


def test_resolve_links_dict_input_resolved():
    """resolve_links also handles pre-dumped dicts (the stream_ogc_features path)."""
    d = {"href": "https://example.com/items?f=html", "rel": "alternate", "title": {"en": "HTML view", "fr": "Vue HTML"}}
    resolved = resolve_links([d], "fr")
    assert resolved is not None
    assert resolved[0]["title"] == "Vue HTML"


def test_resolve_links_dict_wildcard():
    d = {"href": "https://example.com/items?f=html", "rel": "alternate", "title": {"en": "HTML view", "fr": "Vue HTML"}}
    resolved = resolve_links([d], "*")
    assert resolved is not None
    assert resolved[0]["title"] == {"en": "HTML view", "fr": "Vue HTML"}


# ---------------------------------------------------------------------------
# localize_model — Feature / FeatureCollection wire-contract guard
# ---------------------------------------------------------------------------


def test_localize_model_resolves_links_title():
    model_dict = {
        "type": "FeatureCollection",
        "features": [],
        "links": [
            {"href": "https://example.com/items", "rel": "self"},
            {"href": "https://example.com/items?f=html", "rel": "alternate", "title": {"en": "HTML view"}},
        ],
    }
    result = localize_model(model_dict, "en")
    assert result["links"][1]["title"] == "HTML view"


def test_localize_model_does_not_add_top_level_members_feature():
    """Wire-contract guard: localize_model must not add or drop top-level members."""
    feature_dict = {
        "type": "Feature",
        "id": "item-1",
        "geometry": {"type": "Point", "coordinates": [12.0, 41.0]},
        "properties": {"datetime": "2024-01-01T00:00:00Z"},
        "links": [
            {"href": "https://example.com/items/item-1", "rel": "self"},
            {"href": "https://example.com/collections/col", "rel": "collection", "title": {"en": "My collection"}},
        ],
    }
    keys_before = set(feature_dict.keys())
    result = localize_model(feature_dict, "en")
    assert set(result.keys()) == keys_before, "top-level members must not change"
    assert result["type"] == "Feature"
    assert result["id"] == "item-1"
    assert result["geometry"] == feature_dict["geometry"]
    assert result["properties"] == feature_dict["properties"]


def test_localize_model_does_not_add_top_level_members_feature_collection():
    """Wire-contract guard for FeatureCollection."""
    fc_dict = {
        "type": "FeatureCollection",
        "features": [{"type": "Feature", "id": "f1", "geometry": None, "properties": {}}],
        "numberMatched": 1,
        "numberReturned": 1,
        "links": [
            {"href": "https://example.com/items", "rel": "self"},
            {"href": "https://example.com/items?f=html", "rel": "alternate", "title": {"en": "HTML view", "fr": "Vue HTML"}},
        ],
    }
    keys_before = set(fc_dict.keys())
    result = localize_model(fc_dict, "fr")
    assert set(result.keys()) == keys_before
    assert result["links"][1]["title"] == "Vue HTML"
    assert result["features"] is fc_dict["features"]
    assert result["numberMatched"] == 1


def test_localize_model_no_links_is_identity():
    d = {"type": "Feature", "id": "x", "geometry": None, "properties": {}}
    result = localize_model(d, "en")
    assert result is d  # unchanged, same object


def test_localize_model_link_without_title_unchanged():
    d = {
        "type": "Feature",
        "links": [{"href": "https://example.com", "rel": "self"}],
    }
    result = localize_model(d, "en")
    assert result["links"][0] == {"href": "https://example.com", "rel": "self"}
