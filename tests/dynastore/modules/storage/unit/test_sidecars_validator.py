"""Fix 2: CollectionPostgresqlDriverConfig.validate_sidecars_polymorphic must
fail loud on malformed entries instead of silently passing dicts through. This
surfaces data-corruption bugs at hydration time — the ingestion Cloud Run
regression was masked for months by the silent else-branch.
"""

import pytest

from dynastore.modules.catalog.sidecars.base import SidecarConfig
from dynastore.modules.catalog.sidecars.geometries_config import (
    GeometriesSidecarConfig,
)
from dynastore.modules.storage.driver_config import (
    CollectionPostgresqlDriverConfig,
)


class TestSidecarsValidatorHappyPaths:
    def test_typed_instances_pass_through(self):
        cfg = CollectionPostgresqlDriverConfig(
            sidecars=[GeometriesSidecarConfig()],
        )
        assert len(cfg.sidecars) == 1
        assert isinstance(cfg.sidecars[0], GeometriesSidecarConfig)

    def test_dict_with_discriminator_coerces_to_subclass(self):
        cfg = CollectionPostgresqlDriverConfig(
            sidecars=[{"sidecar_type": "geometries"}],
        )
        assert len(cfg.sidecars) == 1
        assert isinstance(cfg.sidecars[0], GeometriesSidecarConfig)
        assert cfg.sidecars[0].sidecar_type == "geometries"

    def test_mixed_list_resolves_correctly(self):
        cfg = CollectionPostgresqlDriverConfig(
            sidecars=[
                GeometriesSidecarConfig(),
                {"sidecar_type": "attributes"},
            ],
        )
        assert len(cfg.sidecars) == 2
        assert cfg.sidecars[0].sidecar_type == "geometries"
        assert cfg.sidecars[1].sidecar_type == "attributes"


class TestSidecarsValidatorFailsLoud:
    def test_dict_without_sidecar_type_raises(self):
        with pytest.raises(ValueError, match="sidecar_type"):
            CollectionPostgresqlDriverConfig(sidecars=[{"enabled": True}])

    def test_dict_with_empty_sidecar_type_raises(self):
        with pytest.raises(ValueError, match="sidecar_type"):
            CollectionPostgresqlDriverConfig(
                sidecars=[{"sidecar_type": "", "enabled": True}]
            )

    def test_non_dict_non_sidecarconfig_raises(self):
        with pytest.raises(ValueError, match="expected SidecarConfig or dict"):
            CollectionPostgresqlDriverConfig(sidecars=["not-a-config"])

    def test_error_message_includes_index_for_multi_item_list(self):
        with pytest.raises(ValueError, match=r"sidecars\[1\]"):
            CollectionPostgresqlDriverConfig(
                sidecars=[
                    GeometriesSidecarConfig(),
                    {"enabled": True},  # bad item at idx 1
                ]
            )


class TestSidecarsValidatorRoundTrip:
    def test_model_dump_then_validate_preserves_subclass(self):
        """Round-trip via model_dump (mode=python) + model_validate is the path
        exercised by ConfigService.get_config tier-merge at config_service.py:299.
        """
        original = CollectionPostgresqlDriverConfig(
            sidecars=[GeometriesSidecarConfig()],
        )
        dumped = original.model_dump(mode="python")
        restored = CollectionPostgresqlDriverConfig.model_validate(dumped)
        assert isinstance(restored.sidecars[0], GeometriesSidecarConfig)
        assert restored.sidecars[0].sidecar_type == "geometries"

    def test_model_dump_json_then_validate_preserves_subclass(self):
        original = CollectionPostgresqlDriverConfig(
            sidecars=[GeometriesSidecarConfig()],
        )
        payload = original.model_dump_json()
        restored = CollectionPostgresqlDriverConfig.model_validate_json(payload)
        assert isinstance(restored.sidecars[0], SidecarConfig)
        assert restored.sidecars[0].sidecar_type == "geometries"
