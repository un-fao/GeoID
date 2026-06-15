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

"""Unit tests for response_i18n helpers.

Covers:
  resolve_localized  — plain str, None, LocalizedDTO, language-keyed dict,
                       unknown-lang fallback, lang='*' passthrough
  resolve_links      — list of Link models, lang='*' keeps dict, missing title
  localize_response_dict — top-level text_fields + link_keys, in-place mutation
  localize_model     — round-trip via model_dump + localize_response_dict
"""

from typing import Optional

from pydantic import BaseModel, Field

from dynastore.extensions.tools.response_i18n import (
    localize_model,
    localize_response_dict,
    resolve_links,
    resolve_localized,
)
from dynastore.models.localization import LocalizedText
from dynastore.models.shared_models import Link


# ---------------------------------------------------------------------------
# resolve_localized
# ---------------------------------------------------------------------------


def test_resolve_localized_plain_str_unchanged():
    assert resolve_localized("hello", "en") == "hello"


def test_resolve_localized_none_returns_none():
    assert resolve_localized(None, "en") is None


def test_resolve_localized_wildcard_passthrough_str():
    assert resolve_localized("hello", "*") == "hello"


def test_resolve_localized_wildcard_passthrough_dict():
    d = {"en": "hello", "fr": "bonjour"}
    result = resolve_localized(d, "*")
    assert result == {"en": "hello", "fr": "bonjour"}


def test_resolve_localized_wildcard_passthrough_localized_dto():
    lt = LocalizedText(en="hello", fr="bonjour")
    result = resolve_localized(lt, "*")
    # lang='*' returns value unchanged (round-trip escape hatch) — the caller
    # receives the full LocalizedDTO object; serialisation happens downstream.
    assert result is lt


def test_resolve_localized_localized_dto_default_en():
    lt = LocalizedText(en="hello", fr="bonjour")
    assert resolve_localized(lt, "en") == "hello"


def test_resolve_localized_localized_dto_requested_language():
    lt = LocalizedText(en="hello", fr="bonjour")
    assert resolve_localized(lt, "fr") == "bonjour"


def test_resolve_localized_localized_dto_unknown_lang_falls_back_to_en():
    lt = LocalizedText(en="hello", fr="bonjour")
    # 'de' is not present → fallback to 'en'
    assert resolve_localized(lt, "de") == "hello"


def test_resolve_localized_multilang_dict_exact_match():
    d = {"en": "hello", "fr": "bonjour"}
    assert resolve_localized(d, "fr") == "bonjour"


def test_resolve_localized_multilang_dict_base_lang_fallback():
    d = {"en": "hello", "fr": "bonjour"}
    # 'fr-CA' → base 'fr' → found
    assert resolve_localized(d, "fr-CA") == "bonjour"


def test_resolve_localized_multilang_dict_en_fallback():
    d = {"en": "hello", "fr": "bonjour"}
    # 'de' not found, base 'de' not found → falls back to 'en'
    assert resolve_localized(d, "de") == "hello"


def test_resolve_localized_multilang_dict_first_key_fallback():
    d = {"fr": "bonjour", "es": "hola"}
    # No 'en' key; unknown 'de' → falls back to first key 'fr'
    assert resolve_localized(d, "de") == "bonjour"


def test_resolve_localized_non_multilang_dict_returned_as_is():
    """A dict without any language keys is returned unchanged."""
    d = {"custom_field": "value", "other": 42}
    assert resolve_localized(d, "en") == d


# ---------------------------------------------------------------------------
# resolve_links
# ---------------------------------------------------------------------------


def test_resolve_links_none_returns_none():
    assert resolve_links(None, "en") is None


def test_resolve_links_empty_list():
    assert resolve_links([], "en") == []


def test_resolve_links_plain_link_model_with_localized_title():
    link = Link(
        href="https://example.com/",
        rel="self",
        title="This document",  # type: ignore[arg-type]
    )
    # The Link validator wraps "This document" → {"en": "This document"}
    result = resolve_links([link], "en")
    assert result is not None
    assert len(result) == 1
    assert result[0]["title"] == "This document"


def test_resolve_links_requested_language():
    link = Link(
        href="https://example.com/",
        rel="self",
        title={"en": "This document", "fr": "Ce document"},  # type: ignore[arg-type]
    )
    result = resolve_links([link], "fr")
    assert result is not None
    assert result[0]["title"] == "Ce document"


def test_resolve_links_unknown_lang_falls_back_to_en():
    link = Link(
        href="https://example.com/",
        rel="self",
        title={"en": "This document", "fr": "Ce document"},  # type: ignore[arg-type]
    )
    result = resolve_links([link], "de")
    assert result is not None
    assert result[0]["title"] == "This document"


def test_resolve_links_wildcard_keeps_full_dict():
    link = Link(
        href="https://example.com/",
        rel="self",
        title={"en": "This document", "fr": "Ce document"},  # type: ignore[arg-type]
    )
    result = resolve_links([link], "*")
    assert result is not None
    # title is stored as {"en": ..., "fr": ...} after the Link validator runs
    assert isinstance(result[0]["title"], dict)
    assert "en" in result[0]["title"]
    assert "fr" in result[0]["title"]


def test_resolve_links_no_title_field_excluded():
    link = Link(href="https://example.com/", rel="self")
    result = resolve_links([link], "en")
    assert result is not None
    assert "title" not in result[0]


def test_resolve_links_dict_passthrough():
    """A plain dict link without title is passed through as-is."""
    d = {"href": "https://example.com/", "rel": "self", "type": "application/json"}
    result = resolve_links([d], "en")  # type: ignore[list-item]
    assert result == [d]


# ---------------------------------------------------------------------------
# localize_response_dict
# ---------------------------------------------------------------------------


def test_localize_response_dict_resolves_top_level_text_field():
    data = {
        "title": {"en": "My API", "fr": "Mon API"},
        "description": {"en": "Description", "fr": "Description fr"},
    }
    result = localize_response_dict(data, "fr")
    assert result["title"] == "Mon API"
    assert result["description"] == "Description fr"


def test_localize_response_dict_resolves_links():
    data = {
        "links": [
            {"href": "https://x.com/", "rel": "self", "title": {"en": "Self", "fr": "Moi"}},
        ]
    }
    result = localize_response_dict(data, "fr")
    assert result["links"][0]["title"] == "Moi"


def test_localize_response_dict_mutates_in_place():
    data = {"title": {"en": "X"}}
    result = localize_response_dict(data, "en")
    assert result is data  # same object


def test_localize_response_dict_missing_field_skipped():
    data = {"other": "value"}
    result = localize_response_dict(data, "en")
    assert result == {"other": "value"}


def test_localize_response_dict_wildcard_passthrough():
    data = {
        "title": {"en": "My API", "fr": "Mon API"},
        "links": [{"href": "https://x.com/", "rel": "self", "title": {"en": "Self", "fr": "Moi"}}],
    }
    result = localize_response_dict(data, "*")
    # lang='*' → plain strings left as-is, multi-language dicts preserved
    assert result["title"] == {"en": "My API", "fr": "Mon API"}
    assert result["links"][0]["title"] == {"en": "Self", "fr": "Moi"}


# ---------------------------------------------------------------------------
# localize_model
# ---------------------------------------------------------------------------


class _SimpleModel(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    links: list = Field(default_factory=list)


def test_localize_model_returns_dict():
    m = _SimpleModel(title="Hello")
    result = localize_model(m, "en")
    assert isinstance(result, dict)


def test_localize_model_plain_str_unchanged():
    m = _SimpleModel(title="Hello")
    result = localize_model(m, "en")
    assert result["title"] == "Hello"


def test_localize_model_with_link_titles_resolved():
    from dynastore.extensions.tools.ogc_common_models import LandingPage

    landing = LandingPage(
        title="My API",
        description="Desc",
        links=[
            Link(
                href="https://example.com/",
                rel="self",
                type="application/json",
                title="This document",  # type: ignore[arg-type]
            ),
            Link(
                href="https://example.com/conformance",
                rel="conformance",
                type="application/json",
                title="Conformance classes",  # type: ignore[arg-type]
            ),
        ],
    )
    result = localize_model(landing, "en")
    assert isinstance(result, dict)
    assert result["title"] == "My API"
    for link in result["links"]:
        assert isinstance(link["title"], str), f"Expected str, got {link['title']!r}"
    titles = {link["title"] for link in result["links"]}
    assert "This document" in titles
    assert "Conformance classes" in titles


def test_localize_model_wildcard_keeps_full_dict_on_localized_link():
    from dynastore.extensions.tools.ogc_common_models import LandingPage

    landing = LandingPage(
        title="My API",
        links=[
            Link(
                href="https://example.com/",
                rel="self",
                title={"en": "Self link", "fr": "Lien self"},  # type: ignore[arg-type]
            ),
        ],
    )
    result = localize_model(landing, "*")
    link_title = result["links"][0]["title"]
    assert isinstance(link_title, dict)
    assert "en" in link_title
    assert "fr" in link_title
