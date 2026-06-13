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

"""Write-time value-constraint validation derived from ``ItemsSchema``.

The write path derives its value-constraint validator directly from
``ItemsSchema`` — the single source of truth — mirroring the read path in
``ItemService.get_collection_schema``. The wire schema is never carried on
``ItemsWritePolicy`` (the inert ``resolved_schema`` projection was removed in
#1164).

These tests pin the two pure helpers that the ``ItemService.upsert`` write
region uses (``_build_write_validator`` + ``_validate_feature_properties``) so
the value-constraint behaviour is covered without standing up the full PG
ingestion transaction.

Crucially, the derived write validator MUST NOT duplicate the two structural
checks that other write-path mechanisms already own:

* unknown property keys -> ``check_strict_unknown_fields`` raising
  ``UnknownFieldsError`` (so the derived schema carries no
  ``additionalProperties: false``);
* required fields -> ``NOT NULL`` sidecar columns derived from the items
  schema, or the ``check_required`` app-level fallback for backends without
  native enforcement (so the derived schema carries no ``required``).
"""

from __future__ import annotations

import pytest

from dynastore.models.protocols.field_definition import FieldDefinition
from dynastore.modules.catalog.item_service import (
    _build_write_validator,
    _validate_feature_properties,
)
from dynastore.modules.storage.driver_config import ItemsSchema
from dynastore.modules.storage.errors import UnknownFieldsError
from dynastore.modules.storage.field_constraints import (
    check_required,
    check_strict_unknown_fields,
)


# ---------------------------------------------------------------------------
# _build_write_validator
# ---------------------------------------------------------------------------


def test_no_validator_for_blob_collection() -> None:
    """A schema with no declared fields yields no validator (nothing to check)."""
    assert _build_write_validator(ItemsSchema(fields={})) is None


def test_no_validator_for_non_items_schema() -> None:
    """A non-ItemsSchema config (or None) yields no validator."""
    assert _build_write_validator(None) is None
    assert _build_write_validator(object()) is None


def test_validator_omits_required_and_additional_properties() -> None:
    """The derived write schema asserts values only, never structure."""
    schema = ItemsSchema(
        fields={
            "name": FieldDefinition(
                name="name", data_type="string", required=True, max_length=5
            ),
        }
    )
    validator = _build_write_validator(schema)
    assert validator is not None
    derived = validator.schema
    assert "required" not in derived
    assert "additionalProperties" not in derived
    # value constraint is present
    assert derived["properties"]["name"]["maxLength"] == 5


def test_validator_omits_additional_properties_even_when_strict() -> None:
    """``strict_unknown_fields`` must NOT leak into the write validator."""
    schema = ItemsSchema(
        fields={"name": FieldDefinition(name="name", data_type="string")},
        strict_unknown_fields=True,
    )
    validator = _build_write_validator(schema)
    assert validator is not None
    assert "additionalProperties" not in validator.schema


# ---------------------------------------------------------------------------
# _validate_feature_properties — value-constraint rejection (422-shaped)
# ---------------------------------------------------------------------------


def _validator(**fields: FieldDefinition):
    return _build_write_validator(ItemsSchema(fields=fields))


def test_none_validator_is_noop() -> None:
    _validate_feature_properties(None, {"properties": {"anything": "goes"}})


def test_accepts_valid_properties() -> None:
    validator = _validator(
        name=FieldDefinition(name="name", data_type="string", max_length=5),
        count=FieldDefinition(name="count", data_type="integer", minimum=0),
    )
    _validate_feature_properties(validator, {"properties": {"name": "abc", "count": 3}})


def test_rejects_maxlength_violation() -> None:
    validator = _validator(
        name=FieldDefinition(name="name", data_type="string", max_length=3),
    )
    with pytest.raises(ValueError, match="violate the items schema") as exc:
        _validate_feature_properties(validator, {"properties": {"name": "toolong"}})
    assert "name" in str(exc.value)


def test_rejects_wrong_type() -> None:
    validator = _validator(
        count=FieldDefinition(name="count", data_type="integer"),
    )
    with pytest.raises(ValueError, match="violate the items schema") as exc:
        _validate_feature_properties(validator, {"properties": {"count": "not-an-int"}})
    assert "count" in str(exc.value)


def test_rejects_enum_miss() -> None:
    validator = _validator(
        kind=FieldDefinition(name="kind", data_type="string", enum=["a", "b"]),
    )
    with pytest.raises(ValueError, match="violate the items schema") as exc:
        _validate_feature_properties(validator, {"properties": {"kind": "z"}})
    assert "kind" in str(exc.value)


def test_rejects_minimum_violation() -> None:
    validator = _validator(
        score=FieldDefinition(name="score", data_type="double", minimum=0.0),
    )
    with pytest.raises(ValueError, match="violate the items schema"):
        _validate_feature_properties(validator, {"properties": {"score": -1.0}})


def test_missing_properties_bag_is_tolerated() -> None:
    """A feature with no ``properties`` key validates against an empty bag.

    Value constraints only fire on present values; absence is a ``required``
    concern, which the write validator deliberately does not own.
    """
    validator = _validator(
        name=FieldDefinition(name="name", data_type="string", max_length=3),
    )
    _validate_feature_properties(validator, {"id": "f1"})


# ---------------------------------------------------------------------------
# No double enforcement: unknown-key and required checks stay with their owners
# ---------------------------------------------------------------------------


def test_unknown_key_passes_value_validator_but_is_caught_by_strict_check() -> None:
    """An unknown property key does NOT trip the value validator (no
    ``additionalProperties``); it is rejected exactly once by the dedicated
    strict-unknown-fields check."""
    schema = ItemsSchema(
        fields={"name": FieldDefinition(name="name", data_type="string")},
        strict_unknown_fields=True,
    )
    validator = _build_write_validator(schema)
    feature = {"properties": {"name": "ok", "rogue": "x"}}

    # Value validator: the unknown key is ignored (would otherwise be a second
    # 422 for the same input).
    _validate_feature_properties(validator, feature)

    # Strict-unknown check: this is where the unknown key is rejected — once.
    with pytest.raises(UnknownFieldsError) as exc:
        check_strict_unknown_fields(schema.fields.keys(), [feature])
    assert "rogue" in exc.value.unknown_fields


def test_missing_required_passes_value_validator_but_is_caught_by_required_check() -> None:
    """A missing required value does NOT trip the value validator (no
    ``required`` in the derived schema); the dedicated ``check_required``
    fallback owns that rejection."""
    schema = ItemsSchema(
        fields={"name": FieldDefinition(name="name", data_type="string", required=True)},
    )
    validator = _build_write_validator(schema)
    feature = {"properties": {}}

    # Value validator: silent — required is not its job.
    _validate_feature_properties(validator, feature)

    # The required check (app-level fallback) is the single owner of the raise.
    with pytest.raises(Exception) as exc:
        check_required(schema.fields, [feature])
    assert "name" in str(exc.value)


def test_required_external_id_missing_or_empty_is_rejected_by_required_check() -> None:
    """Behavior preserved after #1164 removed the inert ``external_id_required``
    guard: an external_id that is a declared required field, when missing OR an
    empty string, is still rejected via the normal ``check_required`` path
    (which treats ``""`` as missing). This is the case the old guard claimed to
    own — ``check_required`` plus NOT NULL columns already cover it.
    """
    schema = ItemsSchema(
        fields={
            "code": FieldDefinition(name="code", data_type="string", required=True),
        }
    )

    # Missing entirely.
    with pytest.raises(Exception) as missing_exc:
        check_required(schema.fields, [{"properties": {}}])
    assert "code" in str(missing_exc.value)

    # Present but empty string — must also be rejected.
    with pytest.raises(Exception) as empty_exc:
        check_required(schema.fields, [{"properties": {"code": ""}}])
    assert "code" in str(empty_exc.value)

    # A real value passes.
    check_required(schema.fields, [{"properties": {"code": "TG0309"}}])
