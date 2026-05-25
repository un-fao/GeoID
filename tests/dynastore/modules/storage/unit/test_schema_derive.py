from dynastore.models.protocols.field_definition import FieldDefinition
from dynastore.modules.storage.schema_derive import derive_wire_schema

DRAFT = "https://json-schema.org/draft/2020-12/schema"


def test_empty_fields_returns_none():
    assert derive_wire_schema({}) is None
    assert derive_wire_schema(None) is None  # type: ignore[arg-type]


def test_non_strict_omits_additional_properties():
    schema = derive_wire_schema({"a": FieldDefinition(name="a", data_type="string")})
    assert schema is not None
    assert schema["$schema"] == DRAFT
    assert schema["type"] == "object"
    assert "additionalProperties" not in schema


def test_strict_adds_additional_properties_false():
    schema = derive_wire_schema(
        {"a": FieldDefinition(name="a", data_type="string")}, strict=True
    )
    assert schema is not None
    assert schema["additionalProperties"] is False


def test_required_list_uses_dict_keys_and_is_sorted():
    fields = {
        "zeta": FieldDefinition(name="zeta_field", data_type="string", required=True),
        "alpha": FieldDefinition(name="alpha_field", data_type="string", required=True),
        "beta": FieldDefinition(name="beta_field", data_type="string", required=False),
    }
    schema = derive_wire_schema(fields)
    assert schema is not None
    assert schema["required"] == ["alpha", "zeta"]
    # property names also come from the dict key, not fd.name
    assert set(schema["properties"]) == {"zeta", "alpha", "beta"}


def test_data_type_mapping():
    fields = {
        "s": FieldDefinition(name="s", data_type="string"),
        "i": FieldDefinition(name="i", data_type="integer"),
        "f": FieldDefinition(name="f", data_type="double"),
        "b": FieldDefinition(name="b", data_type="boolean"),
        "t": FieldDefinition(name="t", data_type="timestamp"),
        "g": FieldDefinition(name="g", data_type="geometry"),
    }
    props = derive_wire_schema(fields)["properties"]  # type: ignore[index]
    assert props["s"] == {"type": "string"}
    assert props["i"] == {"type": "integer"}
    assert props["f"] == {"type": "number"}
    assert props["b"] == {"type": "boolean"}
    assert props["t"] == {"type": "string", "format": "date-time"}
    assert props["g"] == {"type": "object"}


def test_derive_is_defensive_against_noncanonical_data_type():
    # FieldDefinition rejects non-canonical data_type at construction (strict,
    # no legacy aliases) — so to exercise derive_wire_schema's defensive default
    # we bypass validation with model_construct. A value it can't map degrades
    # to {"type": "string"} rather than raising.
    fields = {
        "u": FieldDefinition.model_construct(name="u", data_type="something_weird"),
        "c": FieldDefinition.model_construct(name="c", data_type="TEXT"),
    }
    props = derive_wire_schema(fields)["properties"]  # type: ignore[index]
    assert props["u"] == {"type": "string"}
    assert props["c"] == {"type": "string"}


def test_validator_pass_through():
    fields = {
        "name": FieldDefinition(name="name", data_type="string", max_length=50, pattern="^[a-z]+$"),
        "score": FieldDefinition(name="score", data_type="double", minimum=0.0, maximum=100.0),
        "kind": FieldDefinition(name="kind", data_type="string", enum=["a", "b", "c"]),
    }
    props = derive_wire_schema(fields)["properties"]  # type: ignore[index]
    assert props["name"]["maxLength"] == 50
    assert props["name"]["pattern"] == "^[a-z]+$"
    assert props["score"]["minimum"] == 0.0
    assert props["score"]["maximum"] == 100.0
    assert props["kind"]["enum"] == ["a", "b", "c"]


def test_field_format_overrides_timestamp_default():
    fields = {
        "ts": FieldDefinition(name="ts", data_type="timestamp", format="date"),
        "raw": FieldDefinition(name="raw", data_type="string", format="email"),
    }
    props = derive_wire_schema(fields)["properties"]  # type: ignore[index]
    assert props["ts"]["format"] == "date"
    assert props["raw"]["format"] == "email"


# ── purpose="write" variant ─────────────────────────────────────────────
# The write variant feeds the write-path JSON-Schema validator. It must
# carry value constraints (type/enum/min/max/maxLength/pattern/format) but
# NOT structural constraints already owned by other write-path checks:
# - ``additionalProperties`` is owned by the strict-unknown-fields check
# - ``required`` is owned by NOT NULL DDL (PG) / the ``check_required``
#   app-level fallback (ES)


def test_write_purpose_omits_additional_properties_even_when_strict():
    fields = {"a": FieldDefinition(name="a", data_type="string")}
    schema = derive_wire_schema(fields, strict=True, purpose="write")
    assert schema is not None
    # strict would set additionalProperties=False on the read variant, but the
    # write variant must never do so (would double-422 with the unknown-fields
    # check).
    assert "additionalProperties" not in schema


def test_write_purpose_omits_required():
    fields = {
        "name": FieldDefinition(name="name", data_type="string", required=True),
        "score": FieldDefinition(name="score", data_type="double", required=True),
    }
    schema = derive_wire_schema(fields, purpose="write")
    assert schema is not None
    # required is enforced by NOT NULL DDL / app-level fallback, not here.
    assert "required" not in schema


def test_write_purpose_keeps_value_constraints():
    # required=True keeps the type strict ("string"), isolating this test to
    # value-constraint passthrough; nullability of non-required fields has its
    # own coverage below.
    fields = {
        "name": FieldDefinition(
            name="name", data_type="string", max_length=50, pattern="^[a-z]+$",
            required=True,
        ),
        "score": FieldDefinition(
            name="score", data_type="double", minimum=0.0, maximum=100.0,
            required=True,
        ),
        "kind": FieldDefinition(
            name="kind", data_type="string", enum=["a", "b", "c"], required=True,
        ),
    }
    schema = derive_wire_schema(fields, purpose="write")
    assert schema is not None
    props = schema["properties"]
    assert props["name"]["type"] == "string"
    assert props["name"]["maxLength"] == 50
    assert props["name"]["pattern"] == "^[a-z]+$"
    assert props["score"]["minimum"] == 0.0
    assert props["score"]["maximum"] == 100.0
    assert props["kind"]["enum"] == ["a", "b", "c"]


def test_write_purpose_returns_none_for_empty_fields():
    assert derive_wire_schema({}, purpose="write") is None


def test_read_purpose_is_default_and_unchanged():
    fields = {"a": FieldDefinition(name="a", data_type="string", required=True)}
    default = derive_wire_schema(fields, strict=True)
    explicit_read = derive_wire_schema(fields, strict=True, purpose="read")
    assert default == explicit_read
    # the read variant keeps both required and additionalProperties
    assert default["required"] == ["a"]  # type: ignore[index]
    assert default["additionalProperties"] is False  # type: ignore[index]


# ── non-required ⇒ nullable on the write path ──────────────────────────────
# A non-required field's sidecar column is created without NOT NULL, so a
# present-but-``null`` value is legal storage. The write validator must admit
# it (otherwise a value the database would store is rejected by a stricter
# wire check — the START_DATE/END_DATE ingestion failure). ``required`` is the
# key-presence axis; nullability is orthogonal and applies to the write schema
# only — the published read schema keeps the canonical typed shape.


def test_write_purpose_non_required_field_is_nullable():
    fields = {
        "start": FieldDefinition(name="start", data_type="string", required=False),
        "ts": FieldDefinition(name="ts", data_type="timestamp", required=False),
        "code": FieldDefinition(name="code", data_type="string", required=True),
    }
    props = derive_wire_schema(fields, purpose="write")["properties"]  # type: ignore[index]
    assert props["start"]["type"] == ["string", "null"]
    # value constraints survive nullability
    assert props["ts"]["type"] == ["string", "null"]
    assert props["ts"]["format"] == "date-time"
    # required field keeps its strict, non-null type
    assert props["code"]["type"] == "string"


def test_write_purpose_nullable_handles_object_type():
    fields = {"g": FieldDefinition(name="g", data_type="geometry", required=False)}
    props = derive_wire_schema(fields, purpose="write")["properties"]  # type: ignore[index]
    assert props["g"]["type"] == ["object", "null"]


def test_read_purpose_non_required_field_stays_non_null():
    fields = {"start": FieldDefinition(name="start", data_type="string", required=False)}
    props = derive_wire_schema(fields)["properties"]  # type: ignore[index]  # read default
    assert props["start"]["type"] == "string"


def test_write_validator_accepts_null_for_non_required_field():
    from jsonschema import Draft202012Validator

    fields = {
        "start": FieldDefinition(name="start", data_type="string", required=False),
    }
    schema = derive_wire_schema(fields, purpose="write")
    validator = Draft202012Validator(schema)  # type: ignore[arg-type]
    assert list(validator.iter_errors({"start": None})) == []  # null now OK
    assert list(validator.iter_errors({"start": "2026-01-01"})) == []  # string OK
    assert list(validator.iter_errors({})) == []  # absent OK
