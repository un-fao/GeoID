"""Parametrised coverage test: every togglable extension must inherit ExposableConfigMixin."""

# Eagerly import every togglable extension's config module so that their
# PluginConfig subclasses auto-register via PersistentModel.__init_subclass__
# before list_registered_configs() is called in the tests below.
import dynastore.extensions.features.features_config  # noqa: F401
import dynastore.extensions.wfs.wfs_config  # noqa: F401
import dynastore.modules.tiles.tiles_config  # noqa: F401
import dynastore.modules.stac.stac_config  # noqa: F401
import dynastore.modules.gcp.gcp_config  # noqa: F401
import dynastore.extensions.maps.config  # noqa: F401
import dynastore.extensions.coverages.config  # noqa: F401
import dynastore.extensions.records.config  # noqa: F401
import dynastore.extensions.processes.config  # noqa: F401
import dynastore.extensions.dimensions.config  # noqa: F401
import dynastore.extensions.dwh.config  # noqa: F401
import dynastore.extensions.search.config  # noqa: F401
import dynastore.modules.stats.config  # noqa: F401
import dynastore.extensions.logs.config  # noqa: F401
import dynastore.extensions.notebooks.config  # noqa: F401
import dynastore.extensions.crs.config  # noqa: F401
import dynastore.extensions.gdal.config  # noqa: F401
import dynastore.extensions.assets.config  # noqa: F401
import dynastore.extensions.styles.config  # noqa: F401

import pytest
from dynastore.extensions.tools.exposure_mixin import (
    ExposableConfigMixin,
    KNOWN_EXTENSION_IDS,
    ALWAYS_ON_EXTENSIONS,
)
from dynastore.modules.db_config.platform_config_service import list_registered_configs


TOGGLABLE = KNOWN_EXTENSION_IDS - ALWAYS_ON_EXTENSIONS


@pytest.mark.parametrize("ext_id", sorted(TOGGLABLE))
def test_togglable_has_exposable_config(ext_id):
    """Every togglable extension MUST register at least one PluginConfig inheriting the mixin."""
    classes = list_registered_configs().values()
    matching = [
        c for c in classes
        if c.__module__.startswith(f"dynastore.extensions.{ext_id}")
        or c.__module__.startswith(f"dynastore.modules.{ext_id}")
    ]
    assert matching, f"Extension '{ext_id}' registers no PluginConfig; add one (atomic design, no always-on hedge)."
    assert any(issubclass(c, ExposableConfigMixin) for c in matching), (
        f"Extension '{ext_id}' has no PluginConfig inheriting ExposableConfigMixin."
    )


def test_no_extension_redeclares_enabled_field():
    """Inherited field only — do not shadow the mixin."""
    for cls in list_registered_configs().values():
        if not issubclass(cls, ExposableConfigMixin):
            continue
        own_fields = set(cls.__annotations__ or {})
        assert "enabled" not in own_fields, (
            f"{cls.__name__} shadows ExposableConfigMixin.enabled — remove the local field."
        )
