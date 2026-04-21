"""Fix 1: SidecarRegistry.get_sidecar must accept both typed SidecarConfig
instances and mappings carrying a sidecar_type discriminator — the ingestion
Cloud Run path regression (dict leaks past the container validator) must no
longer crash the consumer.
"""

import pytest

from dynastore.modules.storage.drivers.pg_sidecars.base import SidecarConfig
from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
    GeometriesSidecarConfig,
)
from dynastore.modules.storage.drivers.pg_sidecars.registry import SidecarRegistry


class TestGetSidecarAcceptsTypedConfig:
    def test_typed_config_resolves(self):
        config = GeometriesSidecarConfig()
        impl = SidecarRegistry.get_sidecar(config)
        assert impl is not None
        assert impl.sidecar_id == "geometries"


class TestGetSidecarCoercesDict:
    def test_dict_with_discriminator_resolves_to_geometries(self):
        impl = SidecarRegistry.get_sidecar({"sidecar_type": "geometries"})
        assert impl is not None
        assert impl.sidecar_id == "geometries"

    def test_dict_with_discriminator_resolves_to_attributes(self):
        impl = SidecarRegistry.get_sidecar({"sidecar_type": "attributes"})
        assert impl is not None
        assert impl.sidecar_id == "attributes"

    def test_dict_without_discriminator_raises_type_error(self):
        with pytest.raises(TypeError, match="sidecar_type"):
            SidecarRegistry.get_sidecar({"enabled": True})

    def test_non_mapping_non_sidecarconfig_raises_type_error(self):
        with pytest.raises(TypeError, match="expected SidecarConfig or Mapping"):
            SidecarRegistry.get_sidecar(object())  # type: ignore[arg-type]


class TestGetSidecarLenientBehaviour:
    def test_unknown_sidecar_type_lenient_returns_none(self):
        class _Bogus(SidecarConfig):
            sidecar_type: str = "bogus-nonexistent"

        impl = SidecarRegistry.get_sidecar(_Bogus(), lenient=True)
        assert impl is None

    def test_unknown_sidecar_type_strict_raises(self):
        class _Bogus(SidecarConfig):
            sidecar_type: str = "bogus-nonexistent"

        with pytest.raises(ValueError, match="No sidecar implementation"):
            SidecarRegistry.get_sidecar(_Bogus(), lenient=False)
