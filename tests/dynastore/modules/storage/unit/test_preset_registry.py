"""RoutingPreset protocol + registry (#847)."""
from __future__ import annotations

import pytest

from typing import ClassVar

from dynastore.modules.storage.presets import (
    PresetBundle,
    PresetTier,
    RoutingPreset,
    get_preset,
    list_presets,
    register_preset,
)


class _DummyPreset:
    tier: ClassVar[PresetTier] = PresetTier.CATALOG
    catalog_scopable: ClassVar[bool] = False

    def __init__(self, name: str = "demo-preset") -> None:
        self.name = name
        self.description = "demo"

    def build(self, catalog_id: str) -> PresetBundle:  # noqa: ARG002
        return PresetBundle()


def test_protocol_runtime_checkable_accepts_dummy():
    """A class with ``name``, ``description``, ``tier``, ``catalog_scopable``,
    ``build()`` satisfies the structural ``RoutingPreset`` protocol."""
    assert isinstance(_DummyPreset(), RoutingPreset)


def test_register_and_get_round_trip():
    name = "round-trip-preset"
    preset = _DummyPreset(name=name)
    register_preset(preset)
    # Shape-A routing presets (build() but no apply()) are auto-wrapped in a
    # RoutingPresetAdapter at registration so every registered preset exposes
    # the unified apply/revoke/dry_run interface — object identity is therefore
    # not preserved, but the name round-trips and the preset stays retrievable.
    got = get_preset(name)
    assert got.name == name
    assert name in list_presets()


def test_register_duplicate_rejected():
    preset = _DummyPreset(name="dup-preset")
    register_preset(preset)
    with pytest.raises(ValueError, match="already registered"):
        register_preset(_DummyPreset(name="dup-preset"))


def test_get_unknown_preset_raises_key_error():
    with pytest.raises(KeyError, match="no-such-preset"):
        get_preset("no-such-preset")


def test_list_presets_returns_sorted_names():
    register_preset(_DummyPreset(name="zzz-list-preset"))
    register_preset(_DummyPreset(name="aaa-list-preset"))
    names = list_presets()
    relevant = [n for n in names if n.endswith("-list-preset")]
    assert relevant == sorted(relevant)


class _PlatformPreset(_DummyPreset):
    tier: ClassVar[PresetTier] = PresetTier.PLATFORM


def test_list_presets_filters_by_tier():
    """``list_presets(tier=...)`` returns only presets of that tier; the
    registry stays a single flat namespace, the filter is read-time."""
    register_preset(_DummyPreset(name="filter-catalog-preset"))
    register_preset(_PlatformPreset(name="filter-platform-preset"))

    catalog_names = list_presets(PresetTier.CATALOG)
    platform_names = list_presets(PresetTier.PLATFORM)

    assert "filter-catalog-preset" in catalog_names
    assert "filter-catalog-preset" not in platform_names
    assert "filter-platform-preset" in platform_names
    assert "filter-platform-preset" not in catalog_names


def test_builtin_presets_declare_expected_tiers():
    """The shipped presets declare the tiers their URL families expect."""
    assert get_preset("public_catalog").tier == PresetTier.CATALOG
    assert get_preset("private_catalog").tier == PresetTier.CATALOG
    assert get_preset("defaults_postgres").tier == PresetTier.PLATFORM
    assert get_preset("private_collection").tier == PresetTier.COLLECTION
