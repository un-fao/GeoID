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

"""Unit tests for the extracted ``stamp_attrs_from_feature`` free function.

Covers the full behavioural surface of the original ``_stamp_attrs`` inline
body now delegated through this function, plus defensive edge cases.
"""
from __future__ import annotations

import pytest

from dynastore.modules.iam.stamping_config import stamp_attrs_from_feature


# ---------------------------------------------------------------------------
# Basic extraction
# ---------------------------------------------------------------------------

def test_simple_property_path_extracts_value():
    """``$.properties.foo`` extracts the value from ``feature['properties']['foo']``."""
    feature = {"properties": {"foo": "bar"}}
    result = stamp_attrs_from_feature(feature, {"key": "$.properties.foo"})
    assert result == {"key": "bar"}


def test_multiple_paths_all_extracted():
    """Multiple declared paths produce multiple keys in the result."""
    feature = {"properties": {"dept": "finance", "region": "EU"}}
    result = stamp_attrs_from_feature(
        feature,
        {"d": "$.properties.dept", "r": "$.properties.region"},
    )
    assert result == {"d": "finance", "r": "EU"}


def test_nested_value_under_properties():
    """A value that is itself a dict is extracted as-is."""
    feature = {"properties": {"meta": {"level": 3}}}
    result = stamp_attrs_from_feature(feature, {"m": "$.properties.meta"})
    assert result == {"m": {"level": 3}}


# ---------------------------------------------------------------------------
# Missing / absent paths
# ---------------------------------------------------------------------------

def test_missing_property_key_omits_entry():
    """A declared path whose property is absent in the Feature is omitted."""
    feature = {"properties": {"present": "x"}}
    result = stamp_attrs_from_feature(
        feature,
        {"present": "$.properties.present", "absent": "$.properties.absent"},
    )
    assert "present" in result
    assert "absent" not in result


def test_none_value_omits_entry():
    """A property whose value is ``None`` is omitted (matches original behaviour)."""
    feature = {"properties": {"foo": None}}
    result = stamp_attrs_from_feature(feature, {"foo": "$.properties.foo"})
    assert result == {}


def test_unsupported_path_prefix_is_skipped():
    """Paths not starting with ``$.properties.`` are silently skipped."""
    feature = {"properties": {"region": "US"}, "metadata": {"dept": "hr"}}
    result = stamp_attrs_from_feature(
        feature,
        {
            "unsupported": "$.metadata.dept",   # non-properties path
            "supported": "$.properties.region",
        },
    )
    assert result == {"supported": "US"}
    assert "unsupported" not in result


# ---------------------------------------------------------------------------
# Defensive / edge cases
# ---------------------------------------------------------------------------

def test_non_dict_feature_returns_empty():
    """A non-Mapping feature input returns an empty dict rather than raising."""
    result = stamp_attrs_from_feature("not a dict", {"k": "$.properties.x"})  # type: ignore[arg-type]
    assert result == {}


def test_none_feature_returns_empty():
    """``None`` as feature returns empty dict."""
    result = stamp_attrs_from_feature(None, {"k": "$.properties.x"})  # type: ignore[arg-type]
    assert result == {}


def test_empty_paths_returns_empty():
    """Empty paths mapping returns empty dict immediately."""
    feature = {"properties": {"foo": "bar"}}
    result = stamp_attrs_from_feature(feature, {})
    assert result == {}


def test_feature_without_properties_key_returns_empty():
    """Feature dict with no ``properties`` key returns empty dict."""
    result = stamp_attrs_from_feature({"id": "g1"}, {"k": "$.properties.x"})
    assert result == {}


def test_feature_with_none_properties_returns_empty():
    """Feature dict where ``properties`` is ``None`` returns empty dict."""
    result = stamp_attrs_from_feature({"properties": None}, {"k": "$.properties.x"})
    assert result == {}


def test_feature_with_non_dict_properties_returns_empty():
    """Feature dict where ``properties`` is a non-Mapping returns empty dict."""
    result = stamp_attrs_from_feature({"properties": "bad"}, {"k": "$.properties.x"})
    assert result == {}
