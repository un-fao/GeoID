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

"""Issue #957/#974 — ``ItemsWritePolicy.validity_field`` is the null-object
SSOT for the sidecar's validity column.

Following the null-object pattern (#1048), validity is expressed as a single
``validity_field: Optional[str]`` on both ``ItemsWritePolicy`` and
``FeatureAttributeSidecarConfig``. The field name IS the toggle: a path enables
the validity column, ``None`` disables it. ``enable_validity`` is a derived
read-only property on both sides, so the bool can never independently diverge
from the configured path.

The PG driver overlays ``ItemsWritePolicy.validity_field`` onto
``FeatureAttributeSidecarConfig.validity_field`` at ``ensure_storage`` time and
persists the result, so every read path that rehydrates the collection's driver
config sees the policy-aligned value. These tests pin the SSOT contract.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.storage.driver_config import (
    ItemsPostgresqlDriverConfig,
    ItemsWritePolicy,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    AttributeStorageMode,
    FeatureAttributeSidecarConfig,
)
from dynastore.modules.storage.drivers.pg_sidecars.registry import SidecarRegistry


class TestPolicyNullObject:
    """``ItemsWritePolicy.validity_field`` is the null-object SSOT;
    ``enable_validity`` is derived and not independently settable."""

    def test_default_validity_field_is_none(self):
        policy = ItemsWritePolicy()
        assert policy.validity_field is None
        assert policy.enable_validity is False

    def test_set_validity_field_enables_validity(self):
        policy = ItemsWritePolicy(validity_field="reference_year")
        assert policy.validity_field == "reference_year"
        assert policy.enable_validity is True

    def test_enable_validity_is_not_a_settable_field(self):
        """The bool is a derived property — authoring it is rejected so it can
        never diverge from ``validity_field`` (PluginConfig is ``extra=forbid``)."""
        import pytest as _pytest

        with _pytest.raises(Exception):
            ItemsWritePolicy(enable_validity=True)


class TestSidecarNullObject:
    """The sidecar mirrors the policy via the same null-object field. Its
    default matches the policy default so a collection that sets neither side
    has validity OFF everywhere (was previously divergent: sidecar=True /
    policy=False before the SSOT collapse)."""

    def test_sidecar_default_validity_field_matches_policy(self):
        sc = FeatureAttributeSidecarConfig()
        policy = ItemsWritePolicy()
        assert sc.validity_field is None
        assert sc.enable_validity is False
        assert policy.validity_field is None
        assert sc.validity_field == policy.validity_field

    def test_enable_validity_property_derives_from_field(self):
        sc_off = FeatureAttributeSidecarConfig(validity_field=None)
        sc_on = FeatureAttributeSidecarConfig(validity_field="valid_from")
        assert sc_off.enable_validity is False
        assert sc_off.has_validity is False
        assert sc_on.enable_validity is True
        assert sc_on.has_validity is True

    def test_bool_cannot_drive_behaviour(self):
        """A stray ``enable_validity=True`` (no path) does NOT enable validity —
        only ``validity_field`` does. Proves the bool can't diverge from the
        path: the field name is the single toggle."""
        sc = FeatureAttributeSidecarConfig(enable_validity=True)
        assert sc.validity_field is None
        assert sc.enable_validity is False
        assert sc.has_validity is False

    def test_sidecar_factory_propagates_config_value(self):
        """Sidecar ``has_validity()`` reads ``self.config.validity_field`` via
        the derived property — the value the driver overlaid from policy."""
        sc = FeatureAttributeSidecarConfig(validity_field="valid_from")
        impl = SidecarRegistry.get_sidecar(sc)
        assert impl is not None
        assert impl.has_validity() is True

        sc_off = FeatureAttributeSidecarConfig(validity_field=None)
        impl_off = SidecarRegistry.get_sidecar(sc_off)
        assert impl_off is not None
        assert impl_off.has_validity() is False

    def test_factory_absorbs_unknown_kwargs(self):
        """Ctor accepts forward-compatible kwargs (e.g. ``policy=...``)
        without erroring — keeps room for full SSOT threading later."""
        sc = FeatureAttributeSidecarConfig()
        from dynastore.modules.storage.drivers.pg_sidecars.attributes import (
            FeatureAttributeSidecar,
        )

        instance = FeatureAttributeSidecar(sc, policy=MagicMock(validity_field="x"))
        # policy is not consumed by the ctor — config remains the SSOT source.
        assert instance.config.validity_field is None


class TestResolveWritePolicy:
    """The PG driver's ``_resolve_write_policy`` helper looks up
    :class:`ItemsWritePolicy` through the configs waterfall and
    falls back to defaults when the configs service or config is missing.
    Mirrors :meth:`IcebergItemsDriver._resolve_write_policy`."""

    @pytest.mark.asyncio
    async def test_resolves_policy_from_configs(self):
        from dynastore.modules.storage.drivers.postgresql import ItemsPostgresqlDriver

        mock_configs = MagicMock()
        mock_configs.get_config = AsyncMock(
            return_value=ItemsWritePolicy(validity_field="valid_from")
        )
        with patch(
            "dynastore.tools.discovery.get_protocol", return_value=mock_configs
        ):
            policy = await ItemsPostgresqlDriver._resolve_write_policy("c", "col")
        assert policy.validity_field == "valid_from"
        assert policy.enable_validity is True

    @pytest.mark.asyncio
    async def test_returns_defaults_when_configs_missing(self):
        from dynastore.modules.storage.drivers.postgresql import ItemsPostgresqlDriver

        with patch(
            "dynastore.tools.discovery.get_protocol", return_value=None
        ):
            policy = await ItemsPostgresqlDriver._resolve_write_policy("c", "col")
        assert policy.validity_field is None
        assert policy.enable_validity is False

    @pytest.mark.asyncio
    async def test_swallows_configs_errors(self):
        from dynastore.modules.storage.drivers.postgresql import ItemsPostgresqlDriver

        mock_configs = MagicMock()
        mock_configs.get_config = AsyncMock(side_effect=RuntimeError("boom"))
        with patch(
            "dynastore.tools.discovery.get_protocol", return_value=mock_configs
        ):
            policy = await ItemsPostgresqlDriver._resolve_write_policy("c", "col")
        # Falls back to defaults — divergence resolution must never raise.
        assert policy.validity_field is None
        assert policy.enable_validity is False


class TestPolicyOverlay:
    """The overlay logic at ``ensure_storage`` updates each
    :class:`FeatureAttributeSidecarConfig` so its ``validity_field``
    matches the resolved policy. The same overlay is applied to the
    persisted col_config so reads see the policy-aligned value."""

    @staticmethod
    def _apply_overlay(
        col_config: ItemsPostgresqlDriverConfig, policy: ItemsWritePolicy
    ) -> ItemsPostgresqlDriverConfig:
        """Inlines the overlay block from ``ItemsPostgresqlDriver.ensure_storage``
        so the contract can be pinned without spinning a DB."""
        overlay_sidecars = []
        any_overlay = False
        for sc in col_config.sidecars:
            if isinstance(sc, FeatureAttributeSidecarConfig) and (
                sc.validity_field != policy.validity_field
            ):
                overlay_sidecars.append(
                    sc.model_copy(update={"validity_field": policy.validity_field})
                )
                any_overlay = True
            else:
                overlay_sidecars.append(sc)
        if any_overlay:
            return col_config.model_copy(update={"sidecars": overlay_sidecars})
        return col_config

    def test_overlay_enables_sidecar_when_policy_sets_path(self):
        """Divergent case: sidecar=None, policy=path → sidecar gets the path."""
        sc = FeatureAttributeSidecarConfig(
            storage_mode=AttributeStorageMode.JSONB, validity_field=None
        )
        col = ItemsPostgresqlDriverConfig(sidecars=[sc])
        policy = ItemsWritePolicy(validity_field="valid_from")

        overlaid = self._apply_overlay(col, policy)
        attr_sc = next(
            s
            for s in overlaid.sidecars
            if isinstance(s, FeatureAttributeSidecarConfig)
        )
        assert attr_sc.validity_field == "valid_from"
        assert attr_sc.enable_validity is True

    def test_overlay_disables_sidecar_when_policy_clears_path(self):
        """Divergent case: sidecar=path, policy=None → sidecar cleared."""
        sc = FeatureAttributeSidecarConfig(
            storage_mode=AttributeStorageMode.JSONB, validity_field="valid_from"
        )
        col = ItemsPostgresqlDriverConfig(sidecars=[sc])
        policy = ItemsWritePolicy(validity_field=None)

        overlaid = self._apply_overlay(col, policy)
        attr_sc = next(
            s
            for s in overlaid.sidecars
            if isinstance(s, FeatureAttributeSidecarConfig)
        )
        assert attr_sc.validity_field is None
        assert attr_sc.enable_validity is False

    def test_overlay_no_op_when_aligned(self):
        """Sidecar and policy already agree → no model_copy churn."""
        sc = FeatureAttributeSidecarConfig(
            storage_mode=AttributeStorageMode.JSONB, validity_field="valid_from"
        )
        col = ItemsPostgresqlDriverConfig(sidecars=[sc])
        policy = ItemsWritePolicy(validity_field="valid_from")

        overlaid = self._apply_overlay(col, policy)
        assert overlaid is col  # Same object — no overlay applied.

    def test_overlay_propagates_non_default_path(self):
        """The overlay carries the actual path, not just a boolean — the
        sidecar mirror keeps the policy's source field selection."""
        sc = FeatureAttributeSidecarConfig(
            storage_mode=AttributeStorageMode.JSONB, validity_field=None
        )
        col = ItemsPostgresqlDriverConfig(sidecars=[sc])
        policy = ItemsWritePolicy(validity_field="properties.start_date")

        overlaid = self._apply_overlay(col, policy)
        attr_sc = next(
            s
            for s in overlaid.sidecars
            if isinstance(s, FeatureAttributeSidecarConfig)
        )
        assert attr_sc.validity_field == "properties.start_date"

    def test_overlay_sets_external_id_field_from_policy(self):
        """Policy has EXTERNAL_ID compute rule → external_id_field="external_id"."""
        from dynastore.modules.storage.driver_config import ComputedField, ComputedKind

        sc = FeatureAttributeSidecarConfig(
            storage_mode=AttributeStorageMode.JSONB,
            external_id_field=None,  # disabled
        )
        col = ItemsPostgresqlDriverConfig(sidecars=[sc])
        policy = ItemsWritePolicy(
            compute=[ComputedField(kind=ComputedKind.EXTERNAL_ID, name="properties.code")]
        )
        policy_external_id_field = (
            "external_id"
            if policy.find_compute(ComputedKind.EXTERNAL_ID) is not None
            else None
        )
        overlay_sidecars = []
        any_overlay = False
        for s in col.sidecars:
            if isinstance(s, FeatureAttributeSidecarConfig):
                updates = {}
                if s.external_id_field != policy_external_id_field:
                    updates["external_id_field"] = policy_external_id_field
                if updates:
                    overlay_sidecars.append(s.model_copy(update=updates))
                    any_overlay = True
                else:
                    overlay_sidecars.append(s)
            else:
                overlay_sidecars.append(s)
        if any_overlay:
            col = col.model_copy(update={"sidecars": overlay_sidecars})
        attr_sc = next(s for s in col.sidecars if isinstance(s, FeatureAttributeSidecarConfig))
        assert attr_sc.external_id_field == "external_id"
        assert attr_sc.enable_external_id is True  # property shim

    def test_overlay_clears_external_id_field_when_policy_has_no_rule(self):
        """Policy with empty identity list → no EXTERNAL_ID rule → external_id_field=None."""
        from dynastore.modules.storage.driver_config import ComputedKind

        sc = FeatureAttributeSidecarConfig(
            storage_mode=AttributeStorageMode.JSONB,
            external_id_field="external_id",  # enabled
        )
        col = ItemsPostgresqlDriverConfig(sidecars=[sc])
        # identity=[] removes the default IdentityRule(match_on=[EXTERNAL_ID])
        policy = ItemsWritePolicy(identity=[])
        policy_external_id_field = (
            "external_id"
            if policy.find_compute(ComputedKind.EXTERNAL_ID) is not None
            else None
        )
        overlay_sidecars = []
        any_overlay = False
        for s in col.sidecars:
            if isinstance(s, FeatureAttributeSidecarConfig):
                updates = {}
                if s.external_id_field != policy_external_id_field:
                    updates["external_id_field"] = policy_external_id_field
                if updates:
                    overlay_sidecars.append(s.model_copy(update=updates))
                    any_overlay = True
                else:
                    overlay_sidecars.append(s)
            else:
                overlay_sidecars.append(s)
        if any_overlay:
            col = col.model_copy(update={"sidecars": overlay_sidecars})
        attr_sc = next(s for s in col.sidecars if isinstance(s, FeatureAttributeSidecarConfig))
        assert attr_sc.external_id_field is None
        assert attr_sc.enable_external_id is False  # property shim

    def test_overlay_sets_asset_id_field_from_policy(self):
        """policy.track_asset_id=True → asset_id_field="asset_id"."""
        sc = FeatureAttributeSidecarConfig(
            storage_mode=AttributeStorageMode.JSONB,
            asset_id_field=None,  # disabled
        )
        col = ItemsPostgresqlDriverConfig(sidecars=[sc])
        policy = ItemsWritePolicy(track_asset_id=True)
        policy_asset_id_field = "asset_id" if policy.track_asset_id else None
        overlay_sidecars = []
        any_overlay = False
        for s in col.sidecars:
            if isinstance(s, FeatureAttributeSidecarConfig):
                updates = {}
                if s.asset_id_field != policy_asset_id_field:
                    updates["asset_id_field"] = policy_asset_id_field
                if updates:
                    overlay_sidecars.append(s.model_copy(update=updates))
                    any_overlay = True
                else:
                    overlay_sidecars.append(s)
            else:
                overlay_sidecars.append(s)
        if any_overlay:
            col = col.model_copy(update={"sidecars": overlay_sidecars})
        attr_sc = next(s for s in col.sidecars if isinstance(s, FeatureAttributeSidecarConfig))
        assert attr_sc.asset_id_field == "asset_id"
        assert attr_sc.enable_asset_id is True  # property shim

    def test_null_object_defaults_both_fields_enabled(self):
        """Default config has both fields enabled (backward-compatible defaults)."""
        sc = FeatureAttributeSidecarConfig()
        assert sc.external_id_field == "external_id"
        assert sc.asset_id_field == "asset_id"
        assert sc.enable_external_id is True
        assert sc.enable_asset_id is True

    def test_null_object_none_disables_columns(self):
        """Setting field to None disables the column and the boolean property."""
        sc = FeatureAttributeSidecarConfig(
            external_id_field=None,
            asset_id_field=None,
        )
        assert sc.enable_external_id is False
        assert sc.enable_asset_id is False
        assert sc.feature_id_field_name is None

    def test_overlay_leaves_non_attribute_sidecars_alone(self):
        """Geometries / item_metadata sidecars don't get touched."""
        from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
            GeometriesSidecarConfig,
            TargetDimension,
        )

        attr_sc = FeatureAttributeSidecarConfig(validity_field=None)
        geom_sc = GeometriesSidecarConfig(
            target_srid=4326,
            target_dimension=TargetDimension.FORCE_2D,
        )
        col = ItemsPostgresqlDriverConfig(sidecars=[geom_sc, attr_sc])
        policy = ItemsWritePolicy(validity_field="valid_from")

        overlaid = self._apply_overlay(col, policy)
        geom_after = next(
            s for s in overlaid.sidecars if isinstance(s, GeometriesSidecarConfig)
        )
        attr_after = next(
            s
            for s in overlaid.sidecars
            if isinstance(s, FeatureAttributeSidecarConfig)
        )
        assert geom_after is geom_sc  # Untouched
        assert attr_after.validity_field == "valid_from"  # Overlaid


class TestByTimePartitionValidation:
    """``BY_TIME`` partitioning requires the validity column. The validation
    reads the null-object field via the derived property."""

    def test_by_time_requires_validity_field(self):
        from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
            AttributePartitionStrategyPreset,
        )

        sc = FeatureAttributeSidecarConfig(
            validity_field=None,
            partition_strategy=AttributePartitionStrategyPreset.BY_TIME,
        )
        with pytest.raises(ValueError, match="BY_TIME partitioning requires"):
            _ = sc.partition_key_contributions

    def test_by_time_ok_when_validity_field_set(self):
        from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
            AttributePartitionStrategyPreset,
        )

        sc = FeatureAttributeSidecarConfig(
            validity_field="valid_from",
            partition_strategy=AttributePartitionStrategyPreset.BY_TIME,
        )
        assert sc.partition_key_contributions == {"validity": "TSTZRANGE"}
