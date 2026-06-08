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

from dynastore.extensions.coverages.coverages_service import (
    _resolve_default_style_for_coverage,
)


class _FakeConfig:
    default_style_id = None


class _ConfigWithDefault:
    default_style_id = "cfg-default"


_ITEM_WITH_ITEMASSETS_DEFAULT = {
    "assets": {"default_style": {"id": "item-default"}},
}

_ITEM_WITHOUT = {"assets": {}}


def test_config_overrides_item_level():
    assert _resolve_default_style_for_coverage(
        config=_ConfigWithDefault(), item=_ITEM_WITH_ITEMASSETS_DEFAULT,
    ) == "cfg-default"


def test_item_level_wins_when_no_config():
    assert _resolve_default_style_for_coverage(
        config=_FakeConfig(), item=_ITEM_WITH_ITEMASSETS_DEFAULT,
    ) == "item-default"


def test_none_when_neither_source_has_default():
    assert _resolve_default_style_for_coverage(
        config=_FakeConfig(), item=_ITEM_WITHOUT,
    ) is None
