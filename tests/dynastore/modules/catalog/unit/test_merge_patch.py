"""Unit tests for the RFC 7396 JSON Merge Patch helper."""

from dynastore.modules.catalog._merge_patch import merge_patch


def test_merge_patch_adds_new_key():
    assert merge_patch({"a": 1}, {"b": 2}) == {"a": 1, "b": 2}


def test_merge_patch_overwrites_scalar():
    assert merge_patch({"a": 1}, {"a": 2}) == {"a": 2}


def test_merge_patch_null_removes_key():
    assert merge_patch({"a": 1, "b": 2}, {"a": None}) == {"b": 2}


def test_merge_patch_null_on_missing_key_is_noop():
    assert merge_patch({"a": 1}, {"missing": None}) == {"a": 1}


def test_merge_patch_recurses_nested_dict():
    target = {"meta": {"title": "old", "gdalinfo": {"size": [100, 100]}}}
    patch = {"meta": {"title": "new"}}
    assert merge_patch(target, patch) == {
        "meta": {"title": "new", "gdalinfo": {"size": [100, 100]}},
    }


def test_merge_patch_removes_nested_key():
    target = {"meta": {"keep": "ok", "drop": "bye"}}
    assert merge_patch(target, {"meta": {"drop": None}}) == {"meta": {"keep": "ok"}}


def test_merge_patch_list_replaces_not_merges():
    # Per RFC 7396, arrays are not element-merged.
    assert merge_patch({"tags": ["a", "b"]}, {"tags": ["c"]}) == {"tags": ["c"]}


def test_merge_patch_dict_replacing_scalar():
    assert merge_patch({"a": 1}, {"a": {"b": 2}}) == {"a": {"b": 2}}


def test_merge_patch_scalar_replacing_dict():
    assert merge_patch({"a": {"b": 1}}, {"a": "scalar"}) == {"a": "scalar"}


def test_merge_patch_empty_patch_is_identity():
    assert merge_patch({"a": 1, "b": 2}, {}) == {"a": 1, "b": 2}


def test_merge_patch_into_non_dict_target_uses_empty_base():
    # Per spec: if target is not a dict and patch is a dict, treat target as {}.
    assert merge_patch(None, {"a": 1}) == {"a": 1}
    assert merge_patch("scalar", {"a": 1}) == {"a": 1}


def test_merge_patch_non_dict_patch_replaces():
    assert merge_patch({"a": 1}, ["x"]) == ["x"]
    assert merge_patch({"a": 1}, "scalar") == "scalar"
    assert merge_patch({"a": 1}, None) is None


def test_merge_patch_does_not_mutate_target():
    target = {"a": {"b": 1}}
    merge_patch(target, {"a": {"c": 2}})
    assert target == {"a": {"b": 1}}


def test_merge_patch_gdalinfo_scenario():
    """The actual user-reported bug: PATCH metadata.title should not wipe gdalinfo."""
    stored = {
        "title": "old title",
        "description": "",
        "gdalinfo": {
            "size": [4096, 4096],
            "bands": [{"type": "Byte", "noDataValue": 0}],
        },
    }
    user_patch = {"title": "Albania Administrative Boundaries Level 1 Version 01"}
    result = merge_patch(stored, user_patch)
    assert result["title"] == "Albania Administrative Boundaries Level 1 Version 01"
    assert result["gdalinfo"]["size"] == [4096, 4096]
    assert result["gdalinfo"]["bands"] == [{"type": "Byte", "noDataValue": 0}]
    assert result["description"] == ""
