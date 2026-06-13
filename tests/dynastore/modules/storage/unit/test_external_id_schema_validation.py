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

"""external_id ↔ ItemsSchema cross-validation is enforced symmetrically.

The ``external_id`` source path's leaf must reference a field declared in the
resolved ``ItemsSchema`` (or the system fields ``geoid`` / ``id``). The check
fires at **every** scope where a schema is resolvable — catalog and collection
alike — so a policy can't be accepted at the catalog tier yet rejected at the
collection tier (the asymmetry this pins against).
"""

import asyncio

import pytest

from dynastore.models.protocols.field_definition import FieldDefinition
from dynastore.modules.storage.computed_fields import DeriveSpec
from dynastore.modules.storage.driver_config import (
    ItemsSchema,
    ItemsWritePolicy,
    _validate_write_policy,
)


def _fake_configs(schema: ItemsSchema):
    async def get_config(cls, **_kw):
        return schema

    return type("FakeConfigs", (), {"get_config": staticmethod(get_config)})()


def _wp(ext_id: str) -> ItemsWritePolicy:
    return ItemsWritePolicy(derive=DeriveSpec(external_id=ext_id))


_SCHEMA_WITH = ItemsSchema(fields={"code": FieldDefinition(name="code", data_type="string")})
_SCHEMA_WITHOUT = ItemsSchema(fields={"name": FieldDefinition(name="name", data_type="string")})
_SCHEMA_EMPTY = ItemsSchema(fields={})


class TestExternalIdSchemaValidation:
    def test_skips_when_external_id_unset(self, monkeypatch) -> None:
        monkeypatch.setattr(
            "dynastore.tools.discovery.get_protocol",
            lambda _p: _fake_configs(_SCHEMA_WITHOUT),
        )
        asyncio.run(_validate_write_policy(ItemsWritePolicy(), "cat", "col", None))

    @pytest.mark.parametrize("system_field", ["id", "geoid"])
    def test_skips_system_field(self, monkeypatch, system_field) -> None:
        monkeypatch.setattr(
            "dynastore.tools.discovery.get_protocol",
            lambda _p: _fake_configs(_SCHEMA_WITHOUT),
        )
        asyncio.run(_validate_write_policy(_wp(system_field), "cat", "col", None))

    def test_skips_when_no_schema_fields(self, monkeypatch) -> None:
        monkeypatch.setattr(
            "dynastore.tools.discovery.get_protocol",
            lambda _p: _fake_configs(_SCHEMA_EMPTY),
        )
        # No schema declared yet → nothing to validate against, at either scope.
        asyncio.run(_validate_write_policy(_wp("properties.code"), "cat", None, None))
        asyncio.run(_validate_write_policy(_wp("properties.code"), "cat", "col", None))

    def test_rejects_undeclared_at_collection_scope(self, monkeypatch) -> None:
        monkeypatch.setattr(
            "dynastore.tools.discovery.get_protocol",
            lambda _p: _fake_configs(_SCHEMA_WITHOUT),
        )
        with pytest.raises(ValueError, match="code"):
            asyncio.run(_validate_write_policy(_wp("properties.code"), "cat", "col", None))

    def test_rejects_undeclared_at_catalog_scope(self, monkeypatch) -> None:
        # The symmetric half: catalog scope (collection_id=None) used to skip
        # the check entirely; it must now reject just like the collection scope.
        monkeypatch.setattr(
            "dynastore.tools.discovery.get_protocol",
            lambda _p: _fake_configs(_SCHEMA_WITHOUT),
        )
        with pytest.raises(ValueError, match="code"):
            asyncio.run(_validate_write_policy(_wp("properties.code"), "cat", None, None))

    def test_accepts_declared_at_catalog_scope(self, monkeypatch) -> None:
        monkeypatch.setattr(
            "dynastore.tools.discovery.get_protocol",
            lambda _p: _fake_configs(_SCHEMA_WITH),
        )
        asyncio.run(_validate_write_policy(_wp("properties.code"), "cat", None, None))

    def test_accepts_declared_at_collection_scope(self, monkeypatch) -> None:
        monkeypatch.setattr(
            "dynastore.tools.discovery.get_protocol",
            lambda _p: _fake_configs(_SCHEMA_WITH),
        )
        asyncio.run(_validate_write_policy(_wp("properties.code"), "cat", "col", None))
