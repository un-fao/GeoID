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

"""Regression coverage for falsy-but-present COLUMNAR attribute values.

Issue #1820: ingesting a feature whose ``FID`` field is a non-null
incremental integer starting at ``0`` failed with "required field FID is
null". The collection declared ``FID`` as a required ``integer`` field, so it
materialised as a ``NOT NULL`` sidecar column.

The COLUMNAR branch of ``prepare_upsert_payload`` extracted the value with an
``a or b`` short-circuit:

    val = self._extract_value(feature, "properties.FID") or feature.get("FID")

``_extract_value`` returned ``0`` (correct), but ``0`` is falsy, so the ``or``
discarded it and fell through to the top-level lookup — where ``FID`` does not
exist — yielding ``None``. The value was then dropped from the payload and the
``NOT NULL`` column raised on INSERT. ``FID == 1, 2, …`` are truthy and were
written fine, which is why only the zero-indexed first row failed and the
report mentioned ``bigint`` (a red herring: any falsy value triggers it).
"""
from __future__ import annotations

import pytest

from dynastore.modules.storage.drivers.pg_sidecars.attributes import (
    FeatureAttributeSidecar,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    AttributeSchemaEntry,
    FeatureAttributeSidecarConfig,
    PostgresType,
)


def _columnar_sidecar(*entries: AttributeSchemaEntry) -> FeatureAttributeSidecar:
    # A non-empty ``attribute_schema`` resolves storage_mode AUTOMATIC -> COLUMNAR.
    config = FeatureAttributeSidecarConfig(attribute_schema=list(entries))
    sidecar = FeatureAttributeSidecar(config)
    assert sidecar.resolved_storage_mode.value == "columnar"
    return sidecar


def _fid_entry(**overrides) -> AttributeSchemaEntry:
    params = {"name": "FID", "type": PostgresType.INTEGER}
    params.update(overrides)
    return AttributeSchemaEntry(**params)


@pytest.mark.parametrize(
    "value",
    [0, 0.0, False, ""],
    ids=["int-zero", "float-zero", "false", "empty-string"],
)
def test_falsy_attribute_value_is_kept_in_payload(value):
    """A falsy-but-present property value survives into the upsert payload."""
    sidecar = _columnar_sidecar(_fid_entry(nullable=False))
    payload = sidecar.prepare_upsert_payload(
        {"properties": {"FID": value}}, {"geoid": "geoid-1"}
    )
    assert payload is not None
    assert "FID" in payload, "falsy value was dropped from the payload"
    assert payload["FID"] == value


def test_nonzero_value_still_written():
    sidecar = _columnar_sidecar(_fid_entry(nullable=False))
    payload = sidecar.prepare_upsert_payload(
        {"properties": {"FID": 7}}, {"geoid": "geoid-1"}
    )
    assert payload is not None
    assert payload["FID"] == 7


def test_truly_missing_value_falls_back_to_default():
    sidecar = _columnar_sidecar(_fid_entry(default=42))
    payload = sidecar.prepare_upsert_payload(
        {"properties": {"other": "x"}}, {"geoid": "geoid-1"}
    )
    assert payload is not None
    assert payload["FID"] == 42


def test_missing_value_without_default_is_omitted():
    sidecar = _columnar_sidecar(_fid_entry())
    payload = sidecar.prepare_upsert_payload(
        {"properties": {"other": "x"}}, {"geoid": "geoid-1"}
    )
    assert payload is not None
    assert "FID" not in payload


def test_top_level_fallback_when_not_under_properties():
    """Value present only at the top level is still resolved (last resort)."""
    sidecar = _columnar_sidecar(_fid_entry(nullable=False))
    payload = sidecar.prepare_upsert_payload(
        {"FID": 0, "properties": {}}, {"geoid": "geoid-1"}
    )
    assert payload is not None
    assert payload["FID"] == 0
