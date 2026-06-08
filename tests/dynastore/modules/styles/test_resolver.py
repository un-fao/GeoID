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

from dynastore.modules.styles.resolver import StylesResolver, StyleResolution


def test_empty_available_returns_empty_resolution():
    res = StylesResolver().resolve(
        available={},
        coverages_config_default_id=None,
        item_assets_default_id=None,
    )
    assert res == StyleResolution(
        registered_style_ids=[],
        default_style_id=None,
        stylesheets_by_style_id={},
    )


def test_coverages_config_wins_over_item_assets():
    res = StylesResolver().resolve(
        available={"style-a": ["sheet-a"], "style-b": ["sheet-b"]},
        coverages_config_default_id="style-a",
        item_assets_default_id="style-b",
    )
    assert res.default_style_id == "style-a"
    assert set(res.registered_style_ids) == {"style-a", "style-b"}


def test_item_assets_default_when_no_coverages_config():
    res = StylesResolver().resolve(
        available={"style-a": ["sheet-a"], "style-b": ["sheet-b"]},
        coverages_config_default_id=None,
        item_assets_default_id="style-b",
    )
    assert res.default_style_id == "style-b"


def test_none_when_neither_set():
    res = StylesResolver().resolve(
        available={"style-a": ["sheet-a"]},
        coverages_config_default_id=None,
        item_assets_default_id=None,
    )
    assert res.default_style_id is None
    assert res.registered_style_ids == ["style-a"]


def test_stale_coverages_default_falls_through_to_item_assets():
    # Coverages config points at a style that is no longer registered.
    res = StylesResolver().resolve(
        available={"style-b": ["sheet-b"]},
        coverages_config_default_id="style-a-deleted",
        item_assets_default_id="style-b",
    )
    assert res.default_style_id == "style-b"


def test_stale_defaults_fall_through_to_none():
    res = StylesResolver().resolve(
        available={"style-b": ["sheet-b"]},
        coverages_config_default_id="deleted-1",
        item_assets_default_id="deleted-2",
    )
    assert res.default_style_id is None


def test_stylesheets_by_style_id_is_a_copy():
    # Resolver must not leak its input dict identity.
    original = {"style-a": ["sheet"]}
    res = StylesResolver().resolve(
        available=original,
        coverages_config_default_id=None,
        item_assets_default_id=None,
    )
    original["style-z"] = ["should-not-appear"]
    assert "style-z" not in res.stylesheets_by_style_id


def test_resolution_is_frozen():
    import dataclasses
    res = StylesResolver().resolve(
        available={},
        coverages_config_default_id=None,
        item_assets_default_id=None,
    )
    try:
        res.default_style_id = "x"  # type: ignore[misc]
    except dataclasses.FrozenInstanceError:
        return
    raise AssertionError("StyleResolution should be frozen")
