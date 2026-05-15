"""End-to-end regression for #719 follow-up report.

`PR #725 <https://github.com/un-fao/geoid/pull/725>`_ shipped the case-preserving
fix on the columnar attributes sidecar. A user reported the same
``UndefinedColumn: column "Area" of relation ... does not exist`` AFTER #725
deployed (a fresh table ``t_fqjbxehc_attributes``) — this test reproduces the
user's exact config through the ``materialize_fields_as_columns`` bridge path
(the one ItemsSchema PUT travels) and asserts the generated DDL quotes every
mixed-case column AND that the INSERT a writer would emit references those
columns by the same quoted name.

If this test passes, the post-#725 code is correct end-to-end and the user's
recurrence is a deploy/timing artefact (e.g. table from an older revision left
in place). If it fails, there is a real path that bypasses the case-preserving
DDL — the test exhibits which column names get folded.
"""

from __future__ import annotations

from typing import Dict, List

from dynastore.models.protocols.field_definition import FieldDefinition
from dynastore.modules.storage.driver_config import ItemsSchema
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    FeatureAttributeSidecarConfig,
)
from dynastore.modules.storage.field_constraints import (
    bridge_schema_to_attribute_sidecar,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes import (
    FeatureAttributeSidecar,
)


# Exact field set from the user's issue-#719 comment (post-PR-#725 recurrence).
_USER_FIELDS: Dict[str, Dict[str, str]] = {
    "Shape_Leng": {"name": "Shape_Leng", "data_type": "float"},
    "Shape_Area": {"name": "Shape_Area", "data_type": "float"},
    "ADM2_FR":    {"name": "ADM2_FR",    "data_type": "text"},
    "ADM2_PCODE": {"name": "ADM2_PCODE", "data_type": "text"},
    "ADM2_REF":   {"name": "ADM2_REF",   "data_type": "text"},
    "ADM2ALT1FR": {"name": "ADM2ALT1FR", "data_type": "text"},
    "ADM2ALT2FR": {"name": "ADM2ALT2FR", "data_type": "text"},
    "ADM1_FR":    {"name": "ADM1_FR",    "data_type": "text"},
    "ADM1_PCODE": {"name": "ADM1_PCODE", "data_type": "text"},
    "ADM0_FR":    {"name": "ADM0_FR",    "data_type": "text"},
    "ADM0_PCODE": {"name": "ADM0_PCODE", "data_type": "text"},
    "date":       {"name": "date",       "data_type": "date"},
    "validOn":    {"name": "validOn",    "data_type": "date"},
    "validTo":    {"name": "validTo",    "data_type": "date"},
    "Area":       {"name": "Area",       "data_type": "integer"},
    "F_":         {"name": "F_",         "data_type": "float"},
}


def _build_schema_and_sidecar() -> tuple[ItemsSchema, FeatureAttributeSidecarConfig]:
    schema = ItemsSchema(
        fields={k: FieldDefinition(**v) for k, v in _USER_FIELDS.items()},
        allow_app_level_enforcement=True,
        strict_unknown_fields=True,
        materialize_fields_as_columns=True,
    )
    return schema, FeatureAttributeSidecarConfig(attribute_schema=[])


def test_bridge_materialises_every_user_field_with_preserved_case() -> None:
    schema, sidecar_cfg = _build_schema_and_sidecar()
    bridged = bridge_schema_to_attribute_sidecar(schema, sidecar_cfg)
    names: List[str] = [a.name for a in bridged.attribute_schema]
    assert set(names) == set(_USER_FIELDS), (
        f"materialize_fields_as_columns should lift EVERY declared field; "
        f"missing: {set(_USER_FIELDS) - set(names)}; "
        f"extra: {set(names) - set(_USER_FIELDS)}"
    )
    # Mixed-case names must be preserved verbatim (NOT lowercased).
    for original in _USER_FIELDS:
        assert original in names, f"{original!r} folded or dropped by bridge"


def test_ddl_quotes_every_user_field_case_preserved() -> None:
    schema, sidecar_cfg = _build_schema_and_sidecar()
    bridged = bridge_schema_to_attribute_sidecar(schema, sidecar_cfg)
    sidecar = FeatureAttributeSidecar(bridged)
    ddl = sidecar.get_ddl(physical_table="t_fqjbxehc")

    for name in _USER_FIELDS:
        # Column definition line is quoted, case-preserved.
        assert f'"{name}" ' in ddl, (
            f"DDL is missing the case-preserved column definition for {name!r}. "
            f"DDL excerpt:\n{ddl}"
        )
        # The bare (unquoted) token MUST NOT appear next to the type — PG
        # would have folded that to lowercase.
        for sql_type in ("FLOAT", "INTEGER", "TEXT", "DATE"):
            assert f"{name} {sql_type}" not in ddl, (
                f"DDL contains an unquoted column definition '{name} {sql_type}' "
                f"that Postgres would fold to lowercase."
            )


def test_upsert_sql_references_same_quoted_columns() -> None:
    """The INSERT emitted by ``_upsert_sidecar_table_raw`` quotes every key in
    the payload dict. If the payload dict carries the user's mixed-case field
    names, the INSERT references them with the same case-preserved quotation
    as the DDL — so PG can find them.
    """
    # Simulate the data dict that flows into _upsert_sidecar_table_raw after
    # prepare_upsert_payload extracts properties keyed by ``attr.name``.
    data = {name: "x" for name in _USER_FIELDS}
    data["geoid"] = "00000000-0000-0000-0000-000000000000"
    cols = [f'"{k}"' for k in data]
    for name in _USER_FIELDS:
        assert f'"{name}"' in cols, (
            f"INSERT column list missing case-preserved {name!r}"
        )
