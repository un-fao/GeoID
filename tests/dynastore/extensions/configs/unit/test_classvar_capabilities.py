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

"""Pin the post-#678 contract: ``capabilities`` is a ClassVar on every
driver config, not a Pydantic field.

Plan #6 of umbrella #665 said capabilities are a structural property of
the driver class — they cannot vary at runtime, so they must NOT appear
in the composed-config wire payload. The legitimate operator-facing
surface is the registry endpoint (``describedby`` → ``GET
/configs/registry/{class_key}``), reached via the ``DriverInfo`` DTO at
``service.py:list_drivers()``.

These tests fix that contract:

  1. ``capabilities`` is absent from ``model_fields`` on every concrete
     driver config (so Pydantic skips it in ``model_dump()`` and the
     generated JSON Schema).
  2. ClassVar lookup via an instance still returns the per-class set
     (so the ~25 runtime consumers reading ``obj.capabilities`` are
     unaffected).
  3. ``model_dump()`` of a freshly-constructed instance carries no
     ``capabilities`` key.
  4. The generated JSON Schema does not declare a ``capabilities``
     property.
"""

from __future__ import annotations

import pytest

from dynastore.modules.elasticsearch.catalog_es_driver import (
    CatalogElasticsearchDriverConfig,
)
from dynastore.modules.storage.driver_config import (
    AssetElasticsearchDriverConfig,
    AssetPostgresqlDriverConfig,
    DriverCapability,
    DriverPluginConfig,
    ItemsDuckdbDriverConfig,
    ItemsElasticsearchDriverConfig,
    ItemsElasticsearchPrivateDriverConfig,
    ItemsIcebergDriverConfig,
    ItemsPostgresqlDriverConfig,
)


_EXPECTED_BY_CLASS = {
    ItemsPostgresqlDriverConfig: frozenset(
        {DriverCapability.SYNC, DriverCapability.TRANSACTIONAL}
    ),
    ItemsElasticsearchDriverConfig: frozenset({DriverCapability.ASYNC}),
    ItemsElasticsearchPrivateDriverConfig: frozenset({DriverCapability.ASYNC}),
    ItemsDuckdbDriverConfig: frozenset(
        {DriverCapability.ASYNC, DriverCapability.BATCH}
    ),
    ItemsIcebergDriverConfig: frozenset(
        {DriverCapability.ASYNC, DriverCapability.BATCH}
    ),
    AssetPostgresqlDriverConfig: frozenset(
        {DriverCapability.SYNC, DriverCapability.TRANSACTIONAL}
    ),
    AssetElasticsearchDriverConfig: frozenset({DriverCapability.ASYNC}),
    CatalogElasticsearchDriverConfig: frozenset({DriverCapability.ASYNC}),
}


@pytest.mark.parametrize("cls", list(_EXPECTED_BY_CLASS))
def test_capabilities_is_not_a_pydantic_field(cls):
    assert "capabilities" not in cls.model_fields, (
        f"{cls.__name__}.capabilities leaked back into model_fields — "
        "regression of #678. It must stay a ClassVar so it does not "
        "appear in the composed-config response."
    )


@pytest.mark.parametrize("cls,expected", list(_EXPECTED_BY_CLASS.items()))
def test_classvar_resolves_to_expected_set(cls, expected):
    assert cls.capabilities == expected
    # Instance access must also resolve via the class attribute (ClassVar
    # lookup) so runtime consumers reading `obj.capabilities` keep
    # working unchanged.
    instance = cls()
    assert instance.capabilities == expected


@pytest.mark.parametrize("cls", list(_EXPECTED_BY_CLASS))
def test_model_dump_omits_capabilities(cls):
    instance = cls()
    dumped = instance.model_dump()
    assert "capabilities" not in dumped, (
        f"{cls.__name__}.model_dump() carries 'capabilities' — the "
        "composed-config wire would leak a structural driver trait."
    )


@pytest.mark.parametrize("cls", list(_EXPECTED_BY_CLASS))
def test_json_schema_omits_capabilities_property(cls):
    schema = cls.model_json_schema()
    properties = schema.get("properties", {})
    assert "capabilities" not in properties, (
        f"{cls.__name__}.model_json_schema() declares a 'capabilities' "
        "property — form builders would render it as editable."
    )


def test_base_driver_plugin_config_has_empty_capabilities_classvar():
    """The base class's ClassVar default is an empty frozenset; concrete
    subclasses override it."""
    assert DriverPluginConfig.capabilities == frozenset()
    assert "capabilities" not in DriverPluginConfig.model_fields
