"""Tests for the operator-tunable `index.mapping.total_fields.limit` knobs.

Default values must stay well above the ES 1000 default that prompted
issue #489; env-var overrides must take effect; non-integer overrides must
fall back to the default instead of raising at index-create time.
"""
from __future__ import annotations

import pytest

from dynastore.modules.elasticsearch.mappings import (
    get_assets_index_settings,
    get_items_index_settings,
)
from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
    get_private_items_index_settings,
)


@pytest.fixture(autouse=True)
def _clear_env(monkeypatch):
    for var in (
        "ES_ITEMS_TOTAL_FIELDS_LIMIT",
        "ES_ASSETS_TOTAL_FIELDS_LIMIT",
        "ES_PRIVATE_ITEMS_TOTAL_FIELDS_LIMIT",
    ):
        monkeypatch.delenv(var, raising=False)


def test_items_default_above_es_ceiling():
    assert get_items_index_settings()["index.mapping.total_fields.limit"] == 2000


def test_assets_default_above_es_ceiling():
    assert get_assets_index_settings()["index.mapping.total_fields.limit"] == 1500


def test_private_items_default_above_es_ceiling():
    assert (
        get_private_items_index_settings()["index.mapping.total_fields.limit"]
        == 1500
    )


@pytest.mark.parametrize(
    "var,getter",
    [
        ("ES_ITEMS_TOTAL_FIELDS_LIMIT", get_items_index_settings),
        ("ES_ASSETS_TOTAL_FIELDS_LIMIT", get_assets_index_settings),
        ("ES_PRIVATE_ITEMS_TOTAL_FIELDS_LIMIT", get_private_items_index_settings),
    ],
)
def test_env_override_takes_effect(monkeypatch, var, getter):
    monkeypatch.setenv(var, "5000")
    assert getter()["index.mapping.total_fields.limit"] == 5000


@pytest.mark.parametrize(
    "var,getter,default",
    [
        ("ES_ITEMS_TOTAL_FIELDS_LIMIT", get_items_index_settings, 2000),
        ("ES_ASSETS_TOTAL_FIELDS_LIMIT", get_assets_index_settings, 1500),
        ("ES_PRIVATE_ITEMS_TOTAL_FIELDS_LIMIT", get_private_items_index_settings, 1500),
    ],
)
def test_invalid_env_falls_back_to_default(monkeypatch, var, getter, default):
    monkeypatch.setenv(var, "not-an-int")
    assert getter()["index.mapping.total_fields.limit"] == default


@pytest.mark.parametrize(
    "var,getter,default",
    [
        ("ES_ITEMS_TOTAL_FIELDS_LIMIT", get_items_index_settings, 2000),
        ("ES_ASSETS_TOTAL_FIELDS_LIMIT", get_assets_index_settings, 1500),
        ("ES_PRIVATE_ITEMS_TOTAL_FIELDS_LIMIT", get_private_items_index_settings, 1500),
    ],
)
def test_blank_env_falls_back_to_default(monkeypatch, var, getter, default):
    monkeypatch.setenv(var, "   ")
    assert getter()["index.mapping.total_fields.limit"] == default
