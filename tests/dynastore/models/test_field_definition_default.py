"""Unit tests for ``FieldDefinition.default`` (P6 of the type-vocab work).

The SSOT field now carries an optional ``default`` whose Python type must back
the canonical ``data_type``. Validation happens at config-parse time (a model
validator), mirroring ``AttributeSchemaEntry.validate_default_type`` but over
the canonical vocabulary — and keeping ``bool`` out of the numeric types.
"""

from __future__ import annotations

import pytest

from dynastore.models.protocols.field_definition import FieldAccess, FieldDefinition


# ---------------------------------------------------------------------------
# Accepted defaults — value type matches the canonical data_type
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "data_type, value",
    [
        ("string", "n/a"),
        ("uuid", "00000000-0000-0000-0000-000000000000"),
        ("date", "2026-01-01"),
        ("time", "12:00:00"),
        ("timestamp", "2026-01-01T00:00:00Z"),
        ("binary", "AA=="),
        ("integer", 0),
        ("bigint", 9_000_000_000),
        ("double", 1.5),
        ("numeric", 2),          # int is a valid numeric default
        ("boolean", True),
        ("boolean", False),
        ("jsonb", {"any": "shape"}),
        ("jsonb", [1, 2, 3]),
    ],
)
def test_default_accepted_when_type_matches(data_type: str, value) -> None:
    fd = FieldDefinition(name="f", data_type=data_type, default=value)
    assert fd.default == value


def test_default_none_is_always_allowed() -> None:
    fd = FieldDefinition(name="f", data_type="geometry", default=None)
    assert fd.default is None


# ---------------------------------------------------------------------------
# Rejected defaults — value type does not match
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "data_type, value",
    [
        ("integer", "1"),        # string for an int column
        ("integer", 1.5),        # float for an int column
        ("integer", True),       # bool is NOT an int default
        ("bigint", True),
        ("double", "1.5"),
        ("double", True),        # bool is NOT a number default
        ("boolean", 1),          # int is NOT a boolean default
        ("boolean", "true"),
        ("string", 1),
        ("uuid", 1),
        ("timestamp", 0),        # epoch-int rejected; ISO string required
    ],
)
def test_default_rejected_on_type_mismatch(data_type: str, value) -> None:
    with pytest.raises(ValueError):
        FieldDefinition(name="f", data_type=data_type, default=value)


def test_default_rejected_on_geometry() -> None:
    with pytest.raises(ValueError, match="geometry"):
        FieldDefinition(name="geom", data_type="geometry", default="POINT(0 0)")


# ---------------------------------------------------------------------------
# #1291 — driver-agnostic access intent + sql_expression is driver-internal
# ---------------------------------------------------------------------------

def test_access_defaults_to_auto() -> None:
    """A field that doesn't declare an access intent defers to the driver (AUTO)."""
    fd = FieldDefinition(name="f", data_type="string")
    assert fd.access is FieldAccess.AUTO


@pytest.mark.parametrize("intent", [FieldAccess.AUTO, FieldAccess.FAST, FieldAccess.COMPACT])
def test_access_round_trips(intent: FieldAccess) -> None:
    fd = FieldDefinition(name="f", data_type="string", access=intent)
    assert fd.access is intent
    # str-enum: the value is the portable wire token.
    assert FieldDefinition(name="f", data_type="string", access=intent.value).access is intent


def test_sql_expression_excluded_from_dump() -> None:
    """``sql_expression`` is a driver-computed read-projection detail: drivers can
    still populate it in-process, but it never serialises into config / API output
    (and so can't be smuggled in as author-supplied raw SQL — cf. #1135)."""
    fd = FieldDefinition(name="f", data_type="string", sql_expression="sc.col")
    # Attribute access still works for the in-process read path…
    assert fd.sql_expression == "sc.col"
    # …but it is excluded from every serialised form, so it never lands in stored
    # config or leaks into API/queryables output.
    assert "sql_expression" not in fd.model_dump()
    assert "sql_expression" not in fd.model_dump(mode="json")
    # In the JSON schema it survives only as a read-only marker (drivers populate
    # it; authors can't meaningfully set it).
    props = FieldDefinition.model_json_schema()["properties"]
    assert props["sql_expression"].get("readOnly") is True
