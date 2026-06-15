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

"""Unit tests for i18n language resolution in the 3D GeoVolumes service.

Covers _plain_text and _build_3d_container for:
- default "en" resolution
- explicit language request (fr)
- unknown language fallback to "en"
- lang="*" full multi-language passthrough
"""

from __future__ import annotations

from unittest.mock import MagicMock

from dynastore.models.localization import LocalizedText


# ---------------------------------------------------------------------------
# _plain_text
# ---------------------------------------------------------------------------


def test_plain_text_default_returns_en():
    from dynastore.extensions.volumes.volumes_service import _plain_text

    title = LocalizedText(en="Buildings", fr="Bâtiments")
    assert _plain_text(title) == "Buildings"


def test_plain_text_explicit_en():
    from dynastore.extensions.volumes.volumes_service import _plain_text

    title = LocalizedText(en="Buildings", fr="Bâtiments")
    assert _plain_text(title, "en") == "Buildings"


def test_plain_text_explicit_fr():
    from dynastore.extensions.volumes.volumes_service import _plain_text

    title = LocalizedText(en="Buildings", fr="Bâtiments")
    assert _plain_text(title, "fr") == "Bâtiments"


def test_plain_text_unknown_language_falls_back_to_en():
    from dynastore.extensions.volumes.volumes_service import _plain_text

    title = LocalizedText(en="Buildings", fr="Bâtiments")
    # "de" is not present; must fall back to "en"
    result = _plain_text(title, "de")
    assert result == "Buildings"


def test_plain_text_star_returns_full_object():
    from dynastore.extensions.volumes.volumes_service import _plain_text

    title = LocalizedText(en="Buildings", fr="Bâtiments")
    result = _plain_text(title, "*")
    # Passthrough: the original LocalizedText object comes back unchanged
    assert result is title


def test_plain_text_plain_str_passthrough():
    from dynastore.extensions.volumes.volumes_service import _plain_text

    assert _plain_text("Just a string", "fr") == "Just a string"
    assert _plain_text("Just a string") == "Just a string"


def test_plain_text_none_returns_none():
    from dynastore.extensions.volumes.volumes_service import _plain_text

    assert _plain_text(None) is None
    assert _plain_text(None, "fr") is None


def test_plain_text_language_keyed_dict_fr():
    from dynastore.extensions.volumes.volumes_service import _plain_text

    title = {"en": "Buildings", "fr": "Bâtiments"}
    assert _plain_text(title, "fr") == "Bâtiments"


def test_plain_text_language_keyed_dict_unknown_fallback():
    from dynastore.extensions.volumes.volumes_service import _plain_text

    title = {"en": "Buildings", "fr": "Bâtiments"}
    assert _plain_text(title, "de") == "Buildings"


def test_plain_text_language_keyed_dict_star_passthrough():
    from dynastore.extensions.volumes.volumes_service import _plain_text

    title = {"en": "Buildings", "fr": "Bâtiments"}
    result = _plain_text(title, "*")
    assert result is title


# ---------------------------------------------------------------------------
# _build_3d_container — language propagation
# ---------------------------------------------------------------------------


def _make_localized_collection(en_title: str, fr_title: str) -> MagicMock:
    """Minimal mock collection with a LocalizedText title."""
    coll = MagicMock()
    coll.id = "buildings-lod2"
    coll.title = LocalizedText(en=en_title, fr=fr_title)
    coll.model_extra = {
        "extras": {
            "cityjson:version": "2.0",
            "geovolumes:zrange": {"zmin": 0.0, "zmax": 50.0},
        }
    }
    coll.extent = MagicMock()
    coll.extent.spatial = MagicMock()
    coll.extent.spatial.bbox = [[4.27, 52.06, 4.32, 52.09]]
    return coll


def test_build_3d_container_default_language_resolves_en():
    from dynastore.extensions.volumes.volumes_service import _build_3d_container

    coll = _make_localized_collection("Buildings", "Bâtiments")
    container = _build_3d_container(coll, "test-cat")
    assert container.title == "Buildings"


def test_build_3d_container_fr_language():
    from dynastore.extensions.volumes.volumes_service import _build_3d_container

    coll = _make_localized_collection("Buildings", "Bâtiments")
    container = _build_3d_container(coll, "test-cat", "fr")
    assert container.title == "Bâtiments"


def test_build_3d_container_unknown_language_falls_back_to_en():
    from dynastore.extensions.volumes.volumes_service import _build_3d_container

    coll = _make_localized_collection("Buildings", "Bâtiments")
    container = _build_3d_container(coll, "test-cat", "de")
    assert container.title == "Buildings"


def test_build_3d_container_star_language_collapses_to_en():
    """ThreeDContainer.title is Optional[str]; lang='*' cannot return a
    multi-language dict through that field.  The container falls back to the
    'en' value so Pydantic validation succeeds.  Callers that need the full
    multi-language shape should use the route-level response dict helpers.
    """
    from dynastore.extensions.volumes.volumes_service import _build_3d_container

    coll = _make_localized_collection("Buildings", "Bâtiments")
    container = _build_3d_container(coll, "test-cat", "*")
    assert container.title == "Buildings"


def test_build_3d_container_plain_str_title_unchanged():
    """Plain string titles must be returned as-is regardless of language."""
    from dynastore.extensions.volumes.volumes_service import _build_3d_container

    coll = MagicMock()
    coll.id = "plain-col"
    coll.title = "Plain Title"
    coll.model_extra = {
        "extras": {
            "geovolumes:enabled": True,
            "geovolumes:bbox": [-75.62, 40.03, -75.60, 40.05],
            "geovolumes:zrange": {"zmin": 0.0, "zmax": 100.0},
        }
    }
    coll.extent = None

    for lang in ("en", "fr", "de", "*"):
        container = _build_3d_container(coll, "test-cat", lang)
        assert container.title == "Plain Title", f"Failed for lang={lang!r}"
