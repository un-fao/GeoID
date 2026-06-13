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

"""STAC items sidecar is OPT-IN, gated on the ``stac_items_pg`` context.

STAC is no longer injected by default.  ``StacItemsSidecar.get_default_config``
returns ``None`` unless the injection context carries ``stac_items_pg is
True`` — which the async PG items driver plumbs only when the scope's
``StacStorageConfig`` has items-tier enabled AND includes PG storage
(``items_stac_enabled(level) and pg_stac(storage)``).  Absent a
``StacPreset`` (default ``stac_level=NONE``) no ``stac_metadata`` sidecar is
materialized, so VECTOR/RASTER collections pay no empty-table JOIN cost.

The RECORDS guard is independent of the opt-in flag: RECORDS collections
never carry a per-item STAC descriptive layer.
"""

# Importing the extension package registers the ``stac_metadata`` sidecar
# (config + impl) via its module-level ``SidecarRegistry.register`` /
# ``SidecarConfigRegistry.register`` side effects.
import dynastore.extensions.stac  # noqa: F401
from dynastore.extensions.stac.stac_items_sidecar import StacItemsSidecar
from dynastore.extensions.stac.stac_metadata_config import (
    StacItemsSidecarConfig,
)
from dynastore.modules.storage.drivers.pg_sidecars.registry import (
    SidecarRegistry,
)


class TestGetDefaultConfigGate:
    """Unit-level opt-in gate on the STAC sidecar's ``get_default_config``."""

    def test_returns_none_when_flag_absent(self):
        # No ``stac_items_pg`` key → opt-in default: NOT injected.
        cfg = StacItemsSidecar.get_default_config({"collection_type": "VECTOR"})
        assert cfg is None

    def test_returns_config_when_stac_items_pg_true(self):
        cfg = StacItemsSidecar.get_default_config(
            {"collection_type": "VECTOR", "stac_items_pg": True}
        )
        assert isinstance(cfg, StacItemsSidecarConfig)

    def test_returns_none_when_stac_items_pg_false(self):
        cfg = StacItemsSidecar.get_default_config(
            {"collection_type": "VECTOR", "stac_items_pg": False}
        )
        assert cfg is None

    def test_records_collection_skipped_even_when_opted_in(self):
        # RECORDS guard wins over the opt-in flag.
        assert (
            StacItemsSidecar.get_default_config(
                {"collection_type": "RECORDS", "stac_items_pg": True}
            )
            is None
        )


class TestInjectedSidecarConfigsRespectFlag:
    """End-to-end through ``SidecarRegistry.get_injected_sidecar_configs``."""

    def _types(self, configs):
        return {getattr(c, "sidecar_type", None) for c in configs}

    def test_opted_in_collection_gets_stac_sidecar(self):
        injected = SidecarRegistry.get_injected_sidecar_configs(
            {"collection_type": "VECTOR", "stac_items_pg": True}
        )
        assert "stac_metadata" in self._types(injected)

    def test_default_collection_excludes_stac_sidecar(self):
        # Flag absent → opt-in default: STAC sidecar NOT injected.
        injected = SidecarRegistry.get_injected_sidecar_configs(
            {"collection_type": "VECTOR"}
        )
        types = self._types(injected)
        assert "stac_metadata" not in types
        # Core, non-STAC sidecars are unaffected by the opt-in gate.
        assert "geometries" in types
        assert "attributes" in types

    def test_opted_out_collection_excludes_stac_sidecar(self):
        injected = SidecarRegistry.get_injected_sidecar_configs(
            {"collection_type": "VECTOR", "stac_items_pg": False}
        )
        types = self._types(injected)
        assert "stac_metadata" not in types
        assert "geometries" in types
        assert "attributes" in types
