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

"""Guiding principle #1: every non-exceptional ``PluginConfig`` subclass must
instantiate with no args so the waterfall always has a code-level floor.

Documented exception: ``ItemsIcebergDriverConfig`` has mandatory fields
(``warehouse``/namespace/table_name) — when the waterfall cannot supply those,
the expected outcome is ``ConfigResolutionError`` at request time.
"""

import pytest

from dynastore.modules.storage.driver_config import (
    ItemsSchema,
    ItemsWritePolicy,
    WriteConflictPolicy,
)
from dynastore.modules.storage.routing_config import (
    ItemsRoutingConfig,
    Operation,
)


class TestCollectionRoutingConfigDefaults:
    def test_zero_arg_instantiation(self):
        cfg = ItemsRoutingConfig()
        assert cfg is not None

    def test_has_write_operation_default(self):
        cfg = ItemsRoutingConfig()
        ops = cfg.operations or {}
        # Default posture: at least one WRITE and one READ driver entry
        assert Operation.WRITE in ops
        assert len(ops[Operation.WRITE]) >= 1

    def test_has_read_operation_default(self):
        cfg = ItemsRoutingConfig()
        ops = cfg.operations or {}
        assert Operation.READ in ops
        assert len(ops[Operation.READ]) >= 1

    def test_write_default_is_pg(self):
        cfg = ItemsRoutingConfig()
        ids = [e.driver_ref for e in cfg.operations[Operation.WRITE]]
        assert "items_postgresql_driver" in ids


class TestItemsWritePolicyDefaults:
    def test_zero_arg_instantiation(self):
        cfg = ItemsWritePolicy()
        assert cfg is not None

    def test_default_on_conflict_is_update(self):
        cfg = ItemsWritePolicy()
        assert cfg.on_conflict == WriteConflictPolicy.UPDATE


class TestItemsSchemaDefaults:
    def test_zero_arg_instantiation(self):
        cfg = ItemsSchema()
        assert cfg is not None

    def test_default_schema_is_permissive(self):
        """A fresh-deploy collection with no explicit schema must not block
        ingestion: no required fields, no constraints."""
        cfg = ItemsSchema()
        fields = cfg.fields or []
        required = [f for f in fields if getattr(f, "required", False)]
        assert required == []


class TestIcebergZeroArgInstantiation:
    """Iceberg resolves its ``warehouse_uri`` from ``ICEBERG_WAREHOUSE_URI`` env
    (with an ``Optional[str]`` fallback), so construction is zero-arg viable.

    A ``ConfigResolutionError`` is raised later by the driver at activation
    time if the warehouse cannot be resolved — that's an ops/bootstrap bug,
    not a pydantic construction error.
    """

    def test_zero_arg_instantiation(self):
        try:
            from dynastore.modules.storage.driver_config import (
                ItemsIcebergDriverConfig,
            )
        except ImportError:
            pytest.skip("Iceberg driver config not available in this build")

        cfg = ItemsIcebergDriverConfig()
        assert cfg is not None
