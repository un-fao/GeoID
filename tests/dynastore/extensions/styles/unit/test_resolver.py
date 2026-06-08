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

"""Unit tests for modules/styles/resolver.py — StylesResolver precedence cascade."""

import pytest

from dynastore.modules.styles.resolver import StyleResolution, StylesResolver


def _resolver() -> StylesResolver:
    return StylesResolver()


def _available(*style_ids):
    return {sid: [f"sheet-for-{sid}"] for sid in style_ids}


# ---------------------------------------------------------------------------
# Basic resolution
# ---------------------------------------------------------------------------


def test_empty_available_no_default():
    res = _resolver().resolve(
        available={},
        coverages_config_default_id=None,
        item_assets_default_id=None,
    )
    assert res.registered_style_ids == []
    assert res.default_style_id is None


def test_single_style_no_explicit_default():
    res = _resolver().resolve(
        available=_available("forest"),
        coverages_config_default_id=None,
        item_assets_default_id=None,
    )
    assert res.registered_style_ids == ["forest"]
    assert res.default_style_id is None


# ---------------------------------------------------------------------------
# Precedence: coverages_config wins
# ---------------------------------------------------------------------------


def test_coverages_default_wins_over_item_assets():
    res = _resolver().resolve(
        available=_available("a", "b"),
        coverages_config_default_id="a",
        item_assets_default_id="b",
    )
    assert res.default_style_id == "a"


def test_coverages_default_must_be_registered():
    res = _resolver().resolve(
        available=_available("b"),
        coverages_config_default_id="missing",
        item_assets_default_id="b",
    )
    # "missing" is not in registered → falls through to item_assets
    assert res.default_style_id == "b"


# ---------------------------------------------------------------------------
# Precedence: item_assets fallback
# ---------------------------------------------------------------------------


def test_item_assets_default_used_when_coverages_not_set():
    res = _resolver().resolve(
        available=_available("terrain"),
        coverages_config_default_id=None,
        item_assets_default_id="terrain",
    )
    assert res.default_style_id == "terrain"


def test_item_assets_stale_ref_ignored():
    res = _resolver().resolve(
        available=_available("terrain"),
        coverages_config_default_id=None,
        item_assets_default_id="deleted-style",
    )
    assert res.default_style_id is None


# ---------------------------------------------------------------------------
# Stylesheets dict is propagated
# ---------------------------------------------------------------------------


def test_stylesheets_by_style_id_returned():
    avail = {"s1": ["sheet-a", "sheet-b"], "s2": ["sheet-c"]}
    res = _resolver().resolve(
        available=avail,
        coverages_config_default_id="s1",
        item_assets_default_id=None,
    )
    assert res.stylesheets_by_style_id == avail
    assert res.default_style_id == "s1"
