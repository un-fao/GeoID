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

"""Composition driver for PG-backed catalog metadata.

Catalog-tier sibling of
:mod:`dynastore.modules.storage.drivers.collection_metadata_postgresql`.
Wraps the existing per-domain catalog-tier PG metadata drivers
(:class:`CatalogCorePostgresqlDriver` plus the optional
:class:`CatalogStacPostgresqlDriver` from the stac module) and fans
:class:`CatalogMetadataStore` CRUD across them at write/read/delete time.

PR 1e step 3c lands this wrapper with its entry-point active from day
one, mirroring the 3b cutover for the collection tier.  The two raw
catalog drivers' entry-points are removed in the same change so
``get_protocols(CatalogMetadataStore)`` returns one PG-tier
implementation (the wrapper) instead of two raw drivers.

The wrapper exposes ``stac_metadata_columns()`` from day one so it
structurally satisfies :class:`extensions.stac.protocols.StacCatalogMetadataCapability`
iff a STAC inner is loaded — closing the regression that 3b followed
up on (`747477d`).  ``stac_service._has_stac`` already uses the
non-empty-columns semantic, so an empty STAC sidecar correctly
surfaces as "STAC unavailable".
"""

from __future__ import annotations

import logging
from functools import cached_property
from typing import (
    Annotated,
    Any,
    ClassVar,
    Dict,
    FrozenSet,
    List,
    Literal,
    Optional,
    Tuple,
    Type,
    Union,
)

from pydantic import BaseModel, Discriminator, Field, model_validator

from dynastore.models.protocols.metadata_driver import (
    CatalogMetadataStore,
    MetadataCapability,
)
from dynastore.models.protocols.typed_driver import (
    TypedDriver,
    _PluginDriverConfig,
)
from dynastore.modules.db_config.platform_config_service import Immutable

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Sidecar discriminated union — mirrors the collection-tier wrapper's shape
# ---------------------------------------------------------------------------


class _PgCatalogMetadataSidecarConfigBase(BaseModel):
    """Common base for catalog-metadata PG sidecar configs.

    Subclasses pin ``sidecar_type`` as a ``Literal[...]`` so the
    discriminated union round-trips cleanly through
    ``model_dump(exclude_unset=True) → model_validate``.
    """

    sidecar_type: str

    @model_validator(mode="after")
    def _pin_discriminator_as_set(self) -> "_PgCatalogMetadataSidecarConfigBase":
        """Mark ``sidecar_type`` as explicitly set so ``exclude_unset=True`` keeps it.

        Mirrors the collection-tier wrapper's validator (which itself
        mirrors the items-tier ``SidecarConfig._retain_sidecar_type_on_dump``
        fix from commit ``96fbf8c``).
        """
        self.__pydantic_fields_set__.add("sidecar_type")
        return self


class CatalogMetadataCoreSidecarConfig(_PgCatalogMetadataSidecarConfigBase):
    """Routes the CORE catalog metadata slice (``title``, ``description``,
    ``keywords``, ``license``, ``extra_metadata``) to
    :class:`CatalogCorePostgresqlDriver`.
    """

    sidecar_type: Literal["catalog_metadata_core"] = "catalog_metadata_core"


class CatalogMetadataStacSidecarConfig(_PgCatalogMetadataSidecarConfigBase):
    """Routes the STAC catalog metadata slice (``stac_version``,
    ``stac_extensions``, ``conforms_to``, ``links``, ``assets``) to
    :class:`CatalogStacPostgresqlDriver` from the stac module.

    Resolved via try-import in :class:`CatalogMetadataPgSidecarRegistry` —
    a deployment without the stac extra installed will see this entry's
    resolution log a single warning and skip the slice (no crash).
    """

    sidecar_type: Literal["catalog_metadata_stac"] = "catalog_metadata_stac"


_PgCatalogMetadataSidecarConfig = Annotated[
    Union[
        CatalogMetadataCoreSidecarConfig,
        CatalogMetadataStacSidecarConfig,
    ],
    Discriminator("sidecar_type"),
]


# ---------------------------------------------------------------------------
# Sidecar registry — maps discriminator string → inner driver class
# ---------------------------------------------------------------------------


class CatalogMetadataPgSidecarRegistry:
    """Registry mapping ``sidecar_type`` to inner ``CatalogMetadataStore``
    classes.  STAC entry uses try-import so the wrapper works in
    deployments without the stac extra installed.
    """

    _registry: Dict[str, Type[CatalogMetadataStore]] = {}
    _defaults_loaded: bool = False

    @classmethod
    def _ensure_defaults(cls) -> None:
        if cls._defaults_loaded:
            return
        cls._defaults_loaded = True

        # CORE — same module tree, no try-import needed.
        from dynastore.modules.storage.drivers.metadata_postgresql import (
            CatalogCorePostgresqlDriver,
        )
        cls._registry.setdefault(
            "catalog_metadata_core", CatalogCorePostgresqlDriver,
        )

        # STAC — different module; deployments without the stac extra
        # silently omit the entry.
        try:
            from dynastore.modules.stac.drivers.metadata_postgresql import (
                CatalogStacPostgresqlDriver,
            )
            cls._registry.setdefault(
                "catalog_metadata_stac", CatalogStacPostgresqlDriver,
            )
        except ImportError as exc:
            logger.debug(
                "CatalogMetadataPgSidecarRegistry: stac sidecar unavailable (%s)",
                exc,
            )

    @classmethod
    def get_driver_cls(
        cls, sidecar_type: str,
    ) -> Optional[Type[CatalogMetadataStore]]:
        cls._ensure_defaults()
        return cls._registry.get(sidecar_type)

    @classmethod
    def register(
        cls, sidecar_type: str, driver_cls: Type[CatalogMetadataStore],
    ) -> None:
        """Register a new catalog metadata sidecar type — used by extensions
        that contribute a new domain slice.
        """
        cls._registry[sidecar_type] = driver_cls

    @classmethod
    def default_sidecars(cls) -> List[_PgCatalogMetadataSidecarConfigBase]:
        """Built-in default — CORE always, STAC if the extra is installed."""
        cls._ensure_defaults()
        out: List[_PgCatalogMetadataSidecarConfigBase] = [
            CatalogMetadataCoreSidecarConfig(),
        ]
        if "catalog_metadata_stac" in cls._registry:
            out.append(CatalogMetadataStacSidecarConfig())
        return out

    @classmethod
    def clear(cls) -> None:
        """Test-isolation hook."""
        cls._registry.clear()
        cls._defaults_loaded = False


# ---------------------------------------------------------------------------
# Wrapper config + driver
# ---------------------------------------------------------------------------


class CatalogPostgresqlDriverConfig(_PluginDriverConfig):
    """Configuration for the PG-backed composition catalog metadata driver.

    ``sidecars`` is the typed list of catalog-metadata domain slices the
    wrapper will fan CRUD across.  Empty list → wrapper falls back to
    :meth:`CatalogMetadataPgSidecarRegistry.default_sidecars`
    (``[catalog_metadata_core, catalog_metadata_stac if installed]``).

    Marked ``Immutable`` because changing the active sidecar set after
    rows exist would orphan domain slices in ``catalog.catalog_metadata_*``.
    """

    sidecars: Immutable[List[_PgCatalogMetadataSidecarConfig]] = Field(
        default_factory=list,
        description=(
            "Catalog metadata sidecar configs — discriminated union on "
            "`sidecar_type`.  Empty → registry default "
            "(`catalog_metadata_core` always, `catalog_metadata_stac` "
            "if the stac extra is installed).  Immutable once set — "
            "changing it would orphan rows in the per-domain "
            "``catalog.catalog_metadata_*`` tables.  "
            "NOTE — runtime override is NOT YET WIRED (sister gap to "
            "``CollectionPostgresqlDriverConfig.sidecars``); the wrapper "
            "uses ``CatalogMetadataPgSidecarRegistry.default_sidecars()`` "
            "unconditionally.  Apply handler emits a WARNING when a "
            "non-empty list is submitted to surface the silent-drop."
        ),
    )


async def _on_apply_catalog_pg_driver_config(
    config: Any,
    catalog_id: Optional[str],
    collection_id: Optional[str],
    db_resource: Optional[Any],
) -> None:
    """Warn operators when their non-empty ``sidecars`` override will
    be silently dropped at runtime.  Sister handler to the collection
    tier's ``_on_apply_collection_pg_driver_config``.
    """
    if not isinstance(config, CatalogPostgresqlDriverConfig):
        return
    if not config.sidecars:
        return
    scope = (
        f"catalog '{catalog_id}'" if catalog_id else "platform"
    )
    logger.warning(
        "CatalogPostgresqlDriverConfig.sidecars override at %s scope is "
        "currently NOT honored at runtime — the wrapper uses "
        "CatalogMetadataPgSidecarRegistry.default_sidecars() unconditionally.  "
        "Submitted entries: %s.  Tracked as PR 1e step 4 follow-up.",
        scope, [getattr(s, "sidecar_type", "?") for s in config.sidecars],
    )


CatalogPostgresqlDriverConfig.register_apply_handler(
    _on_apply_catalog_pg_driver_config,
)


class CatalogPostgresqlDriver(TypedDriver[CatalogPostgresqlDriverConfig]):
    """Composition driver: fans CatalogMetadataStore CRUD across the
    configured PG catalog-metadata sidecars (``catalog_metadata_core``,
    optionally ``catalog_metadata_stac``, plus any extension-contributed
    sidecar registered via
    :meth:`CatalogMetadataPgSidecarRegistry.register`).

    The wrapper itself owns no SQL — every method delegates to the
    inner drivers, each of which already filters the payload to its
    own column set via ``_PgCatalogMetadataBase._upsert_catalog_row``.
    """

    capabilities: ClassVar[FrozenSet[str]] = frozenset({
        MetadataCapability.READ,
        MetadataCapability.WRITE,
        MetadataCapability.SOFT_DELETE,
        MetadataCapability.QUERY_FALLBACK_SOURCE,
    })

    def _resolve_inner_drivers(
        self,
        sidecars: Optional[List[_PgCatalogMetadataSidecarConfigBase]] = None,
    ) -> List[CatalogMetadataStore]:
        """Resolve the configured ``sidecars`` list to instantiated inner
        ``CatalogMetadataStore`` driver instances.  Production paths
        call this with ``sidecars=None`` and so go through
        :attr:`_default_inner_drivers`.
        """
        if not sidecars:
            sidecars = CatalogMetadataPgSidecarRegistry.default_sidecars()
        out: List[CatalogMetadataStore] = []
        for cfg in sidecars:
            sc_type = getattr(cfg, "sidecar_type", None)
            if sc_type is None:
                logger.warning(
                    "CatalogPostgresqlDriver: sidecar entry missing "
                    "`sidecar_type` discriminator — skipping",
                )
                continue
            cls = CatalogMetadataPgSidecarRegistry.get_driver_cls(sc_type)
            if cls is None:
                logger.warning(
                    "CatalogPostgresqlDriver: sidecar %r not registered "
                    "(module not installed?) — skipping",
                    sc_type,
                )
                continue
            out.append(cls())
        return out

    @cached_property
    def _default_inner_drivers(self) -> List[CatalogMetadataStore]:
        """Cached registry-default resolution.  One wrapper instance is
        the entry-point-discovered singleton, so this list is effectively
        process-global — instantiated lazily on the first metadata call.
        """
        return self._resolve_inner_drivers(None)

    async def is_available(self) -> bool:
        for inner in self._default_inner_drivers:
            if await inner.is_available():
                return True
        return False

    async def get_catalog_metadata(
        self,
        catalog_id: str,
        *,
        context: Optional[Dict[str, Any]] = None,
        db_resource: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        """Fan-in: read from each inner, shallow-merge slices.  Returns
        ``None`` only when every inner returned ``None``.  Per-inner
        failures degrade to ``None`` for that slice, mirroring the
        existing ``catalog_metadata_router`` _safe_get semantics.
        """
        merged: Dict[str, Any] = {}
        for inner in self._default_inner_drivers:
            try:
                slice_ = await inner.get_catalog_metadata(
                    catalog_id, context=context, db_resource=db_resource,
                )
            except Exception as exc:
                logger.warning(
                    "CatalogPostgresqlDriver: inner %s.get_catalog_metadata "
                    "failed: %s", type(inner).__name__, exc,
                )
                continue
            if slice_:
                merged.update(slice_)
        return merged or None

    async def upsert_catalog_metadata(
        self,
        catalog_id: str,
        metadata: Dict[str, Any],
        *,
        db_resource: Optional[Any] = None,
    ) -> None:
        """Fan-out: hand the full payload to each inner driver.  Each
        inner filters to its own ``_columns`` and no-ops if the filtered
        slice is empty (existing ``_PgCatalogMetadataBase`` invariant).
        """
        for inner in self._default_inner_drivers:
            await inner.upsert_catalog_metadata(
                catalog_id, metadata, db_resource=db_resource,
            )

    async def delete_catalog_metadata(
        self,
        catalog_id: str,
        *,
        soft: bool = False,
        db_resource: Optional[Any] = None,
    ) -> None:
        for inner in self._default_inner_drivers:
            await inner.delete_catalog_metadata(
                catalog_id, soft=soft, db_resource=db_resource,
            )

    async def get_driver_config(
        self,
        catalog_id: str,
        *,
        db_resource: Optional[Any] = None,
    ) -> Any:
        """No per-catalog driver config today — same default-empty as inners."""
        return None

    def stac_metadata_columns(self) -> Tuple[str, ...]:
        """Forward the STAC capability marker through to the first inner
        driver that exposes it.

        Defining this method makes the wrapper structurally satisfy
        :class:`extensions.stac.protocols.StacCatalogMetadataCapability`
        iff a STAC inner is loaded.  Returns ``()`` when no inner
        advertises the marker; ``stac_service._has_stac`` treats the
        empty tuple as "STAC unavailable" so the catalog-tier hard-reject
        at ``_assert_stac_capable_metadata_stack`` still fires correctly
        in stac-less deployments.
        """
        for inner in self._default_inner_drivers:
            method = getattr(inner, "stac_metadata_columns", None)
            if method is not None:
                return tuple(method())
        return ()
