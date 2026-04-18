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
