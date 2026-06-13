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

"""Pin `_to_snake` correctness across the tricky PascalCase boundaries.

The two-step regex (BOUNDARY_1 + BOUNDARY_2) handles consecutive caps —
the simple `(?<!^)(?=[A-Z])` form would mangle `DGGSConfig` into
`d_g_g_s_config`, breaking the wire identity for several plugin classes.
"""

import pytest

from dynastore.tools.typed_store.base import _to_snake


@pytest.mark.parametrize(
    "name,expected",
    [
        # Simple two-word case
        ("GeometryStorage", "geometry_storage"),
        # Consecutive caps coalesce into one word, then underscore
        ("DGGSConfig", "dggs_config"),
        ("WFSPluginConfig", "wfs_plugin_config"),
        ("EDRConfig", "edr_config"),
        # Long multi-word
        ("ItemsElasticsearchPrivateDriver", "items_elasticsearch_private_driver"),
        ("CollectionStacPostgresqlDriver", "collection_stac_postgresql_driver"),
        # Asset / catalog / collection prefixes
        ("ItemsPostgresqlDriver", "items_postgresql_driver"),
        ("AssetPostgresqlDriver", "asset_postgresql_driver"),
        ("CatalogElasticsearchDriver", "catalog_elasticsearch_driver"),
        ("ItemsWritePolicy", "items_write_policy"),
        # Already lowercase: unchanged
        ("foo", "foo"),
        # Numbers stay attached to the preceding lowercase letter
        ("S2Config", "s2_config"),
        ("H3Resolution", "h3_resolution"),
    ],
)
def test_to_snake(name: str, expected: str) -> None:
    assert _to_snake(name) == expected


def test_class_key_returns_snake_case():
    """`PersistentModel.class_key()` flows through `_to_snake` end to end."""
    from dynastore.modules.storage.driver_config import (
        ItemsWritePolicy,
        ItemsPostgresqlDriverConfig,
    )
    # Import the bound driver so TypedDriver.__init_subclass__ has populated
    # _DRIVER_REGISTRY[ItemsPostgresqlDriverConfig] before we read class_key().
    from dynastore.modules.storage.drivers.postgresql import ItemsPostgresqlDriver  # noqa: F401

    assert ItemsWritePolicy.class_key() == "items_write_policy"
    # The DriverConfig→Driver class_key rewrite still happens inside the
    # TypedDriver bind, so the wire key here is the bound DRIVER name (in
    # snake_case): the *Config Python suffix is dropped.
    assert ItemsPostgresqlDriverConfig.class_key() == "items_postgresql_driver"
