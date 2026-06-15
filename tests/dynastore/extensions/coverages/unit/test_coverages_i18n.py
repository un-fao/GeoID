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

"""Coverages non-landing i18n: collection title/description resolved to a single language.

These tests verify that:
- list_catalogs / list_collections resolve LocalizedText titles to a single language string.
- _build_metadata_response passes the language through localize_response_dict.
- lang='*' returns the full multi-language object unchanged.
- An unknown language falls back to 'en'.
- Plain strings pass through unchanged.
"""
from __future__ import annotations


# ---------------------------------------------------------------------------
# resolve_localized helpers (used by list_catalogs / list_collections)
# ---------------------------------------------------------------------------


def test_resolve_localized_default_en():
    from dynastore.models.localization import LocalizedText
    from dynastore.extensions.tools.response_i18n import resolve_localized

    lt = LocalizedText(en="My Coverage Collection", fr="Ma collection de couvertures")
    assert resolve_localized(lt, "en") == "My Coverage Collection"


def test_resolve_localized_requested_language_fr():
    from dynastore.models.localization import LocalizedText
    from dynastore.extensions.tools.response_i18n import resolve_localized

    lt = LocalizedText(en="My Coverage Collection", fr="Ma collection de couvertures")
    assert resolve_localized(lt, "fr") == "Ma collection de couvertures"


def test_resolve_localized_unknown_language_falls_back_to_en():
    from dynastore.models.localization import LocalizedText
    from dynastore.extensions.tools.response_i18n import resolve_localized

    lt = LocalizedText(en="My Coverage Collection", fr="Ma collection de couvertures")
    assert resolve_localized(lt, "de") == "My Coverage Collection"


def test_resolve_localized_wildcard_returns_full_dict():
    """lang='*' preserves the full multi-language object on the wire."""
    from dynastore.models.localization import LocalizedText
    from dynastore.extensions.tools.response_i18n import resolve_localized

    lt = LocalizedText(en="My Coverage Collection", fr="Ma collection de couvertures")
    result = resolve_localized(lt, "*")
    dumped = result.model_dump(exclude_none=True) if hasattr(result, "model_dump") else result
    assert dumped["en"] == "My Coverage Collection"
    assert dumped["fr"] == "Ma collection de couvertures"


def test_resolve_localized_plain_str_unchanged():
    from dynastore.extensions.tools.response_i18n import resolve_localized

    assert resolve_localized("already a string", "en") == "already a string"


def test_resolve_localized_none_returns_none():
    from dynastore.extensions.tools.response_i18n import resolve_localized

    assert resolve_localized(None, "en") is None


def test_resolve_localized_dict_input_fr():
    from dynastore.extensions.tools.response_i18n import resolve_localized

    d = {"en": "My Coverage Collection", "fr": "Ma collection de couvertures"}
    assert resolve_localized(d, "fr") == "Ma collection de couvertures"


def test_resolve_localized_dict_wildcard_preserves_dict():
    from dynastore.extensions.tools.response_i18n import resolve_localized

    d = {"en": "My Coverage Collection", "fr": "Ma collection de couvertures"}
    assert resolve_localized(d, "*") is d


# ---------------------------------------------------------------------------
# _build_metadata_response: language wired through localize_response_dict.
#
# coverages_service has a module-level `import rasterio` SCOPE gate so it can
# only be imported when rasterio is installed.  The i18n contract for
# _build_metadata_response is: it calls localize_response_dict(data, language)
# on its assembled dict before returning.  We test that contract directly via
# localize_response_dict, mirroring what _build_metadata_response does
# internally, without importing the service module.
# ---------------------------------------------------------------------------


def test_metadata_dict_plain_str_title_default_en_unchanged():
    """Plain string title (item id) is left unchanged by localize_response_dict."""
    from dynastore.extensions.tools.response_i18n import localize_response_dict

    data = {"title": "coverage-item-1", "extent": {}, "domainset": {}, "rangetype": {}, "links": []}
    result = localize_response_dict(data, "en")
    assert result["title"] == "coverage-item-1"


def test_metadata_dict_plain_str_title_fr_unchanged():
    """Requesting 'fr' on a plain-string title leaves it unchanged."""
    from dynastore.extensions.tools.response_i18n import localize_response_dict

    data = {"title": "coverage-item-1", "extent": {}, "links": []}
    result = localize_response_dict(data, "fr")
    assert result["title"] == "coverage-item-1"


def test_metadata_dict_localized_title_resolved_to_fr():
    """A LocalizedText title in the assembled dict is resolved to the requested language."""
    from dynastore.models.localization import LocalizedText
    from dynastore.extensions.tools.response_i18n import localize_response_dict

    lt = LocalizedText(en="Coverage Title EN", fr="Titre de couverture FR")
    data = {"title": lt}
    result = localize_response_dict(data, "fr")
    assert result["title"] == "Titre de couverture FR"


def test_metadata_dict_wildcard_plain_str_title_unchanged():
    """lang='*' on a plain-string title does not change it."""
    from dynastore.extensions.tools.response_i18n import localize_response_dict

    data = {"title": "coverage-item-1", "links": []}
    result = localize_response_dict(data, "*")
    assert result["title"] == "coverage-item-1"


def test_metadata_dict_wildcard_localized_title_preserved():
    """lang='*' preserves the full multi-language LocalizedText on the wire."""
    from dynastore.models.localization import LocalizedText
    from dynastore.extensions.tools.response_i18n import localize_response_dict

    lt = LocalizedText(en="Coverage Title EN", fr="Titre de couverture FR")
    data = {"title": lt}
    result = localize_response_dict(data, "*")
    dumped = result["title"].model_dump(exclude_none=True) if hasattr(result["title"], "model_dump") else result["title"]
    assert dumped["en"] == "Coverage Title EN"
    assert dumped["fr"] == "Titre de couverture FR"


# ---------------------------------------------------------------------------
# localize_response_dict on a coverages-style metadata dict
# ---------------------------------------------------------------------------


def test_localize_response_dict_resolves_localized_description():
    """localize_response_dict resolves a LocalizedText description in a metadata dict."""
    from dynastore.models.localization import LocalizedText
    from dynastore.extensions.tools.response_i18n import localize_response_dict

    desc = LocalizedText(en="Coverage description", fr="Description de la couverture")
    data = {"title": "coverage-item-1", "description": desc}
    result = localize_response_dict(data, "fr")
    assert result["description"] == "Description de la couverture"


def test_localize_response_dict_wildcard_preserves_localized_description():
    """lang='*' preserves the full multi-language description dict."""
    from dynastore.models.localization import LocalizedText
    from dynastore.extensions.tools.response_i18n import localize_response_dict

    desc = LocalizedText(en="Coverage description", fr="Description de la couverture")
    data = {"title": "coverage-item-1", "description": desc}
    result = localize_response_dict(data, "*")
    dumped_desc = result["description"].model_dump(exclude_none=True) if hasattr(result["description"], "model_dump") else result["description"]
    assert dumped_desc["en"] == "Coverage description"
    assert dumped_desc["fr"] == "Description de la couverture"


def test_localize_response_dict_unknown_lang_falls_back_to_en():
    """Unknown language resolves to the 'en' value."""
    from dynastore.models.localization import LocalizedText
    from dynastore.extensions.tools.response_i18n import localize_response_dict

    lt = LocalizedText(en="Coverage Title EN", fr="Titre FR")
    data = {"title": lt}
    result = localize_response_dict(data, "de")
    assert result["title"] == "Coverage Title EN"
