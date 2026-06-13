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

"""Platform-wired factories for SidecarBoundsSource and SidecarGeometryFetcher.

Not auto-registered — a deployment opts in by calling
``register_sidecar_bounds_source()`` during startup. Keeping the
registration explicit avoids imposing PostGIS + geometries-sidecar
assumptions on every /volumes/* caller.

``register_sidecar_bounds_source()`` registers BOTH the bounds source
(for tileset.json index building) AND the geometry fetcher (for tile
content generation). Both share one connection factory and one
``layout_resolver``.

The layout resolver reads the *actual* physical layout from the storage
driver + sidecar config rather than hardcoding conventions:

- hub table   → ``ItemsPostgresqlDriver.resolve_physical_table`` (the
  machine-assigned physical table, which is NOT always the collection_id);
- geom column → the live ``GeometriesSidecarConfig.geom_column``;
- geometries table → ``sidecar_table_name(hub, <geometries sidecar id>)``
  (the one-and-only naming SSOT shared with the sidecar DDL).

Everything is degrade-safe: a resolution miss falls back to the
``VolumesConfig`` defaults so the tiler reads the standard layout instead
of raising (which would silently empty the tileset).
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Optional

from dynastore.extensions.volumes.config import VolumesConfig
from dynastore.models.protocols import CatalogsProtocol, DatabaseProtocol
from dynastore.modules.storage.drivers.pg_sidecars import (
    FeatureAttributeSidecarConfig,
    GeometriesSidecarConfig,
    driver_sidecars,
    sidecar_table_name,
)
from dynastore.modules.volumes.geometry_fetcher import SidecarGeometryFetcher
from dynastore.modules.volumes.sidecar_bounds import (
    CollectionPhysicalLayout,
    SidecarBoundsSource,
)
from dynastore.tools.discovery import get_protocol, register_plugin

logger = logging.getLogger(__name__)


def _resolve_attr_z_exprs(
    attrs_sidecar: FeatureAttributeSidecarConfig,
) -> Optional[tuple[str, Optional[str], str, str]]:
    """Render SQL exprs for the attributes sidecar z-range, over join alias ``a``.

    Returns ``(zmin_expr, height_expr_or_None, zmax_expr, mode)`` or ``None``
    when the sidecar cannot supply zmin/zmax. The expressions match the
    physical storage layout (#2089):

    - **Columnar** (explicit COLUMNAR, or AUTOMATIC with a schema that declares
      ``zmin``/``zmax``) → typed columns ``a."zmin"`` / ``a."zmax"`` /
      ``a."height"``.
    - **JSONB** → ``(a."<jsonb_col>"->>'zmin')::float`` etc.
    """
    from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
        AttributeStorageMode,
    )

    mode = getattr(attrs_sidecar, "storage_mode", AttributeStorageMode.AUTOMATIC)
    schema = getattr(attrs_sidecar, "attribute_schema", None) or []
    schema_names = {getattr(e, "name", None) for e in schema}
    columnar = mode == AttributeStorageMode.COLUMNAR or (
        mode == AttributeStorageMode.AUTOMATIC and bool(schema)
    )

    if columnar:
        # Only trust columnar columns that are actually declared; otherwise the
        # query would reference a non-existent column and raise.
        if "zmin" not in schema_names or "zmax" not in schema_names:
            return None
        zmin = 'a."zmin"'
        zmax = 'a."zmax"'
        height = 'a."height"' if "height" in schema_names else None
        return zmin, height, zmax, "columnar"

    # JSONB blob.
    col = getattr(attrs_sidecar, "jsonb_column_name", "attributes")
    zmin = f"(a.\"{col}\"->>'zmin')::float"
    zmax = f"(a.\"{col}\"->>'zmax')::float"
    height = f"(a.\"{col}\"->>'height')::float"
    return zmin, height, zmax, "jsonb"


def register_sidecar_bounds_source(
    *, volumes_config: Optional[VolumesConfig] = None,
) -> None:
    """Register SidecarBoundsSource + SidecarGeometryFetcher.

    Resolves the platform's DatabaseProtocol + CatalogsProtocol at call
    time — both must already be registered. Physical table / column names
    are resolved per-collection from the storage driver + sidecar config
    (see module docstring), so the wiring is pluggable and configurable
    rather than convention-hardcoded.
    """
    cfg = volumes_config or VolumesConfig()

    @asynccontextmanager
    async def _connection_factory():
        db = get_protocol(DatabaseProtocol)
        if db is None:
            raise RuntimeError(
                "DatabaseProtocol not registered; "
                "cannot open bounds-source connection",
            )
        engine = db.async_engine
        if engine is None:
            raise RuntimeError(
                "DatabaseProtocol has no async engine; "
                "SidecarBoundsSource requires an async tenant DB",
            )
        async with engine.connect() as conn:
            yield conn

    async def _resolve_schema(catalog_id: str) -> str:
        catalogs = get_protocol(CatalogsProtocol)
        if catalogs is None:
            raise RuntimeError("CatalogsProtocol not registered")
        schema = await catalogs.resolve_physical_schema(catalog_id)
        if schema is None:
            raise RuntimeError(
                f"CatalogsProtocol could not resolve schema for "
                f"catalog_id={catalog_id!r}",
            )
        return schema

    async def _resolve_hub_and_geom(
        catalog_id: str, collection_id: str,
    ) -> tuple[str, str, Optional[str], Optional[str], Optional[str], Optional[str]]:
        """Resolve (physical hub table, geom column) from the PG store.

        The geometries sidecar is a PostgreSQL-store concept, so the layout
        must come from the PG driver — NOT whichever driver is read-primary.
        STAC collections are Elasticsearch-read-primary (``op=READ`` routes
        to ES, which has no physical table and no PG sidecars), while PG is
        write-primary. Probe ``WRITE`` first, then ``READ``, and lock onto
        the first driver that actually declares a ``GeometriesSidecarConfig``
        — that is the PG store that owns the geometry. Read both the physical
        table and the geom column from that same driver.

        Degrades to (collection_id, fallback column) on any miss so the tiler
        reads the standard layout instead of raising (a raise would silently
        empty the tileset).
        """
        # Lazy import mirrors the stac/maps extensions — keeps the storage
        # router out of this module's import graph at load time.
        from dynastore.modules.storage.router import get_driver
        from dynastore.modules.storage.routing_config import Operation

        hub = collection_id
        geom_column = cfg.geometry_column_fallback
        attrs_table: Optional[str] = None
        zmin_expr: Optional[str] = None
        zmax_expr: Optional[str] = None
        height_expr: Optional[str] = None

        for op in (Operation.WRITE, Operation.READ):
            try:
                driver = await get_driver(op, catalog_id, collection_id)
            except Exception as exc:  # noqa: BLE001 — degrade-safe by design
                logger.debug(
                    "volumes layout: no %s driver for %s/%s (%s)",
                    op, catalog_id, collection_id, exc,
                )
                continue
            if not hasattr(driver, "get_driver_config"):
                continue
            try:
                driver_cfg = await driver.get_driver_config(
                    catalog_id, collection_id,
                )
            except Exception as exc:  # noqa: BLE001
                logger.debug(
                    "volumes layout: get_driver_config failed for %s/%s "
                    "via %s (%s)",
                    catalog_id, collection_id, op, exc,
                )
                continue

            geom_sidecar = next(
                (
                    sc
                    for sc in driver_sidecars(driver_cfg)
                    if isinstance(sc, GeometriesSidecarConfig)
                ),
                None,
            )
            if geom_sidecar is None:
                # Not the PG geometry store (e.g. the ES read driver) —
                # try the next operation's driver.
                continue

            if geom_sidecar.geom_column:
                geom_column = geom_sidecar.geom_column
            if hasattr(driver, "resolve_physical_table"):
                try:
                    resolved = await driver.resolve_physical_table(
                        catalog_id, collection_id,
                    )
                    if resolved:
                        hub = resolved
                except Exception as exc:  # noqa: BLE001
                    logger.debug(
                        "volumes layout: resolve_physical_table failed for "
                        "%s/%s (%s); using collection_id",
                        catalog_id, collection_id, exc,
                    )

            # Resolve the attributes sidecar for true per-feature z-range
            # (#2089). Same driver_cfg, so we already have it. Degrade-safe: a
            # miss leaves attrs_table None → z-range falls back to geometry.
            attrs_sidecar = next(
                (
                    sc
                    for sc in driver_sidecars(driver_cfg)
                    if isinstance(sc, FeatureAttributeSidecarConfig)
                ),
                None,
            )
            if attrs_sidecar is not None:
                try:
                    rendered = _resolve_attr_z_exprs(attrs_sidecar)
                except Exception as exc:  # noqa: BLE001 — degrade-safe
                    logger.debug(
                        "volumes layout: attr z-expr resolve failed for %s/%s "
                        "(%s); z-range from geometry",
                        catalog_id, collection_id, exc,
                    )
                    rendered = None
                if rendered is not None:
                    zmin_expr, height_expr, zmax_expr, _mode = rendered
                    attrs_table = sidecar_table_name(
                        hub, attrs_sidecar.sidecar_id,
                    )
            break

        return hub, geom_column, attrs_table, zmin_expr, zmax_expr, height_expr

    async def _resolve_layout(
        catalog_id: str, collection_id: str,
    ) -> CollectionPhysicalLayout:
        schema = await _resolve_schema(catalog_id)
        (
            hub,
            geom_column,
            attrs_table,
            zmin_expr,
            zmax_expr,
            height_expr,
        ) = await _resolve_hub_and_geom(catalog_id, collection_id)
        geoms = sidecar_table_name(hub, cfg.geometries_sidecar_id)
        return CollectionPhysicalLayout(
            schema=schema,
            hub_table=hub,
            geometries_table=geoms,
            geom_column=geom_column,
            feature_id_column=cfg.feature_id_column,
            attributes_table=attrs_table,
            zmin_expr=zmin_expr,
            zmax_expr=zmax_expr,
            height_expr=height_expr,
        )

    # height_column is intentionally NOT wired (it would reference a hub column
    # that does not exist). The true per-feature z-range (zmin/zmax) and height
    # of a CityJSON feature are filterable attributes the PG driver materializes
    # in the SEPARATE feature_attributes sidecar; the layout resolver above
    # discovers that table and renders the column expressions (#2089), and the
    # bounds/geometry queries LEFT JOIN it with the flat geometry z as fallback.
    source = SidecarBoundsSource(
        connection_factory=_connection_factory,
        layout_resolver=_resolve_layout,
        height_column=None,
    )
    register_plugin(source)
    logger.info("SidecarBoundsSource registered against BoundsSourceProtocol")

    fetcher = SidecarGeometryFetcher(
        connection_factory=_connection_factory,
        layout_resolver=_resolve_layout,
        height_column=None,
    )
    register_plugin(fetcher)
    logger.info("SidecarGeometryFetcher registered against GeometryFetcherProtocol")
