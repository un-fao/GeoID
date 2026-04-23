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

"""STAC capability sub-Protocols — owned by the STAC extension.

Two ``@runtime_checkable`` Protocols extend the core
``CollectionMetadataStore`` / ``CatalogMetadataStore`` surfaces to mark a
driver as STAC-slice-capable. STAC code dispatches via
``isinstance(d, StacCollectionMetadataCapability)`` instead of inspecting
a string ``domain`` ClassVar — class identity, not data identity.

Drivers structurally satisfy these by implementing the base Protocols and
living in ``modules/stac/`` (the practical "this driver persists STAC
fields" gate). A future tighter contract (e.g. a ``materialise_stac_extent()``
method) can be added here without touching core protocols.
"""

from __future__ import annotations

from typing import Protocol, Tuple, runtime_checkable

from dynastore.models.protocols.metadata_driver import (
    CatalogMetadataStore,
    CollectionMetadataStore,
)


@runtime_checkable
class StacCollectionMetadataCapability(CollectionMetadataStore, Protocol):
    """A ``CollectionMetadataStore`` that persists the STAC slice of metadata.

    Concrete STAC PG sidecar drivers (``modules/stac/drivers/metadata_postgresql.py``)
    satisfy this structurally by implementing the base ``CollectionMetadataStore``
    methods *and* exposing the ``stac_metadata_columns()`` marker method.
    Non-STAC drivers (``CollectionCorePostgresqlDriver``, ``CollectionElasticsearchDriver``,
    etc.) lack the marker method and therefore fail the ``isinstance`` check —
    that's the discriminator the STAC service relies on.
    """

    def stac_metadata_columns(self) -> Tuple[str, ...]:
        """Return the tuple of STAC-slice columns this driver persists.

        Marker method that distinguishes STAC drivers from CORE drivers
        at runtime. Implementations typically return ``self._columns``.
        """
        ...


@runtime_checkable
class StacCatalogMetadataCapability(CatalogMetadataStore, Protocol):
    """A ``CatalogMetadataStore`` that persists the STAC slice of metadata.

    Catalog-tier sibling of ``StacCollectionMetadataCapability``. Same
    marker-method discriminator pattern.
    """

    def stac_metadata_columns(self) -> Tuple[str, ...]:
        """Return the tuple of STAC-slice columns this driver persists."""
        ...
