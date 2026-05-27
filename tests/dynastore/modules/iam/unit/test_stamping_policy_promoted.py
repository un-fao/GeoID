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

"""Unit tests for AttributeStampingPolicy promoted-column fields (F5).

Covers:
1. Valid policy round-trips without error.
2. promoted_columns key not in attribute_paths → ValidationError.
3. promoted_column key fails regex → ValidationError.
4. promoted_column_types disallowed pg_type → ValidationError.
5. promoted_column_types key fails regex → ValidationError.
6. promoted_columns defaults to empty list.
7. Empty promoted_columns with non-empty attribute_paths is valid.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from dynastore.modules.iam.stamping_config import (
    AttributeStampingPolicy,
    _ALLOWED_PG_TYPES,
)


# ---------------------------------------------------------------------------
# 1. Valid round-trip
# ---------------------------------------------------------------------------

def test_valid_policy_with_promoted_columns():
    policy = AttributeStampingPolicy(
        attribute_paths={"dept": "$.properties.department", "level": "$.properties.level"},
        promoted_columns=["dept"],
        promoted_column_types={"dept": "TEXT"},
    )
    assert "dept" in policy.promoted_columns
    assert policy.promoted_column_types["dept"] == "TEXT"


# ---------------------------------------------------------------------------
# 2. promoted key not in attribute_paths
# ---------------------------------------------------------------------------

def test_promoted_key_not_in_attribute_paths_raises():
    with pytest.raises(ValidationError, match="not declared in attribute_paths"):
        AttributeStampingPolicy(
            attribute_paths={"dept": "$.properties.department"},
            promoted_columns=["sensitivity"],  # not in attribute_paths
        )


# ---------------------------------------------------------------------------
# 3. promoted key fails regex
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bad_key", ["bad.key", "bad-key", "bad key", "123start", ""])
def test_promoted_key_fails_regex_raises(bad_key: str):
    with pytest.raises(ValidationError, match=r"must match"):
        AttributeStampingPolicy(
            attribute_paths={"dept": "$.properties.department"},
            promoted_columns=[bad_key],
        )


# ---------------------------------------------------------------------------
# 4. pg_type allowlist
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bad_type", ["INTEGER", "BOOLEAN", "JSONB", "VARCHAR"])
def test_promoted_column_type_not_in_allowlist_raises(bad_type: str):
    with pytest.raises(ValidationError, match="not allowed"):
        AttributeStampingPolicy(
            attribute_paths={"dept": "$.properties.department"},
            promoted_column_types={"dept": bad_type},
        )


@pytest.mark.parametrize("good_type", sorted(_ALLOWED_PG_TYPES))
def test_allowed_pg_types_are_accepted(good_type: str):
    policy = AttributeStampingPolicy(
        attribute_paths={"dept": "$.properties.department"},
        promoted_columns=["dept"],
        promoted_column_types={"dept": good_type},
    )
    assert policy.promoted_column_types["dept"] == good_type


# ---------------------------------------------------------------------------
# 5. promoted_column_types key fails regex
# ---------------------------------------------------------------------------

def test_promoted_column_types_key_fails_regex_raises():
    with pytest.raises(ValidationError, match=r"must match"):
        AttributeStampingPolicy(
            attribute_paths={"dept": "$.properties.department"},
            promoted_column_types={"bad.key": "TEXT"},
        )


# ---------------------------------------------------------------------------
# 6. promoted_columns defaults to empty list
# ---------------------------------------------------------------------------

def test_promoted_columns_defaults_to_empty():
    policy = AttributeStampingPolicy(
        attribute_paths={"dept": "$.properties.department"},
    )
    assert policy.promoted_columns == []
    assert policy.promoted_column_types == {}


# ---------------------------------------------------------------------------
# 7. Empty promoted_columns with non-empty attribute_paths is valid
# ---------------------------------------------------------------------------

def test_empty_promoted_columns_is_valid():
    policy = AttributeStampingPolicy(
        attribute_paths={"dept": "$.properties.department", "foo": "$.properties.foo"},
        promoted_columns=[],
    )
    assert policy.promoted_columns == []


# ---------------------------------------------------------------------------
# 8. Multiple promoted columns all valid
# ---------------------------------------------------------------------------

def test_multiple_promoted_columns_all_valid():
    policy = AttributeStampingPolicy(
        attribute_paths={
            "dept": "$.properties.department",
            "level": "$.properties.level",
        },
        promoted_columns=["dept", "level"],
        promoted_column_types={"dept": "TEXT", "level": "NUMERIC"},
    )
    assert set(policy.promoted_columns) == {"dept", "level"}
