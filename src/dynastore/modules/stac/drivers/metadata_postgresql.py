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

"""STAC-slice PG metadata drivers — relocated to the STAC module.

The two collection/catalog-tier STAC drivers persist the STAC subset of
the metadata envelope into the per-tenant ``{schema}.collection_metadata_stac``
and global ``catalog.catalog_metadata_stac`` tables respectively.

The shared CRUD bodies live on ``_CollectionMetadataDomainBase`` /
``_CatalogMetadataDomainBase`` in the core PG metadata module — imported
here, not duplicated. PR 1c renames the base classes (``_PgCollectionMetadataBase``
/ ``_PgCatalogMetadataBase``) and drops the ``MetadataDomain`` ClassVar;
the import path updates atomically with that rename.
"""

from __future__ import annotations

from typing import ClassVar, FrozenSet, Tuple

from dynastore.models.protocols.driver_roles import MetadataDomain
from dynastore.models.protocols.metadata_driver import MetadataCapability
from dynastore.modules.storage.driver_config import DriverPluginConfig
from dynastore.modules.storage.drivers.metadata_domain_postgresql import (
    _CatalogMetadataDomainBase,
    _CollectionMetadataDomainBase,
)


_COLLECTION_STAC_COLUMNS: Tuple[str, ...] = (
    "stac_version", "stac_extensions", "extent", "providers",
    "summaries", "links", "assets", "item_assets",
)
_CATALOG_STAC_COLUMNS: Tuple[str, ...] = (
    "stac_version", "stac_extensions", "conforms_to", "links", "assets",
)


class CollectionStacPostgresqlDriverConfig(DriverPluginConfig):
    """Identity marker for CollectionStacPostgresqlDriver."""


class CatalogStacPostgresqlDriverConfig(DriverPluginConfig):
    """Identity marker for CatalogStacPostgresqlDriver."""


class CollectionStacPostgresqlDriver(_CollectionMetadataDomainBase):
    """Primary driver for STAC collection metadata (``extent``, ``providers``, …).

    Backs ``{schema}.collection_metadata_stac``. Declares ``SPATIAL_FILTER``
    because the ``extent`` column carries the STAC bbox the spatial-filter
    endpoints match against. Active via the collection-metadata router
    alongside ``CollectionCorePostgresqlDriver``.

    Structurally satisfies ``StacCollectionMetadataCapability`` via the
    ``stac_metadata_columns()`` marker method below — this is the runtime
    discriminator that ``stac_service._has_stac()`` uses.
    """

    _table: ClassVar[str] = "collection_metadata_stac"
    _columns: ClassVar[Tuple[str, ...]] = _COLLECTION_STAC_COLUMNS
    domain: ClassVar[MetadataDomain] = MetadataDomain.STAC

    capabilities: FrozenSet[str] = frozenset({
        MetadataCapability.READ,
        MetadataCapability.WRITE,
        MetadataCapability.SOFT_DELETE,
        MetadataCapability.SPATIAL_FILTER,
        MetadataCapability.PHYSICAL_ADDRESSING,
    })

    def stac_metadata_columns(self) -> Tuple[str, ...]:
        return self._columns


class CatalogStacPostgresqlDriver(_CatalogMetadataDomainBase):
    """Primary driver for STAC catalog metadata.

    Backs ``catalog.catalog_metadata_stac``. Scope: ``stac_version``,
    ``stac_extensions``, ``conforms_to``, ``links``, ``assets``.

    Structurally satisfies ``StacCatalogMetadataCapability`` via the
    ``stac_metadata_columns()`` marker method.
    """

    _table: ClassVar[str] = "catalog_metadata_stac"
    _columns: ClassVar[Tuple[str, ...]] = _CATALOG_STAC_COLUMNS
    domain: ClassVar[MetadataDomain] = MetadataDomain.STAC

    capabilities: FrozenSet[str] = frozenset({
        MetadataCapability.READ,
        MetadataCapability.WRITE,
        MetadataCapability.SOFT_DELETE,
    })

    def stac_metadata_columns(self) -> Tuple[str, ...]:
        return self._columns
