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

"""Composable STAC presets — routing and the StacStorageConfig flip, split.

STAC opt-in is two orthogonal concerns, exposed here as two
single-responsibility children plus a backward-compatible composite:

- ``stac_routing`` — *decide the routing*.  Writes per-tier
  ``CatalogRoutingConfig`` / ``CollectionRoutingConfig`` /
  ``ItemsRoutingConfig`` for the tiers enabled by ``stac_level`` and the
  backend named by ``stac_storage``.  Writes no ``StacStorageConfig``.
- ``stac_storage`` — *enable STAC*.  Writes the single ``StacStorageConfig``
  SSOT entry — the signal the PG/ES drivers read at runtime to materialize
  the ``catalog_stac`` / ``collection_stac`` wrapper slices and the per-item
  ``stac_metadata`` sidecar.  Writes no routing.
- ``stac`` — the composite ``compose=("stac_routing", "stac_storage")``.
  One-shot, unchanged public behavior; applies routing first (so the drivers
  the SSOT references exist before the signal flips), then the SSOT; revoke
  reverses.

The split lets an operator stage the work: apply ``stac_routing`` to decide
where data goes, then ``stac_storage`` to enable STAC, then ingest — or apply
``stac`` to do both at once.  Absent any of these, no STAC slices, sidecars,
or ES STAC routes are materialized.

Shared parameters (``StacPresetParams``):

- ``stac_level`` (default ``COLLECTION``) — cumulative depth:
  ``none`` revokes STAC; ``catalog`` enables catalog STAC only;
  ``collection`` also adds collection STAC; ``items`` also adds the
  per-item ``stac_metadata`` sidecar.
- ``stac_storage`` (default ``ES_PG``) — backend(s):
  ``ES`` routes STAC to Elasticsearch only; ``PG`` materializes the PG
  wrapper slices and items sidecar; ``ES_PG`` does both (PG preferred
  for WRITE/READ, ES for SEARCH + async secondary).
"""
from __future__ import annotations

from typing import ClassVar, Dict, List, Optional, Tuple, Type

from pydantic import BaseModel

from dynastore.modules.stac.stac_storage_config import (
    StacLevel,
    StacStorageBackend,
    StacStorageConfig,
    catalog_stac_enabled,
    collection_stac_enabled,
    es_stac,
    items_stac_enabled,
    pg_stac,
)
from dynastore.modules.storage.routing_config import (
    CatalogRoutingConfig,
    CollectionRoutingConfig,
    FailurePolicy,
    ItemsRoutingConfig,
    Operation,
    OperationDriverEntry,
    WriteMode,
)

from .bundle_preset import BundlePreset
from .preset import CompositePreset
from .protocol import PresetBundle, PresetBundleEntry, PresetTier


class StacPresetParams(BaseModel):
    """Parameters for the ``stac`` preset."""

    stac_level: StacLevel = StacLevel.COLLECTION
    stac_storage: StacStorageBackend = StacStorageBackend.ES_PG


# ---------------------------------------------------------------------------
# Routing helpers
# ---------------------------------------------------------------------------


def _catalog_routing_es() -> CatalogRoutingConfig:
    """ES-only catalog routing — all ops to catalog_elasticsearch_driver."""
    return CatalogRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="catalog_elasticsearch_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="catalog_elasticsearch_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.SEARCH: [
                OperationDriverEntry(
                    driver_ref="catalog_elasticsearch_driver",
                    source="operator",
                ),
            ],
        },
    )


def _catalog_routing_es_pg() -> CatalogRoutingConfig:
    """ES_PG catalog routing — PG preferred WRITE/READ, ES for SEARCH + async."""
    return CatalogRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="catalog_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
                OperationDriverEntry(
                    driver_ref="catalog_elasticsearch_driver",
                    write_mode=WriteMode.ASYNC,
                    on_failure=FailurePolicy.OUTBOX,
                    secondary_index=True,
                    source="auto",
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="catalog_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.SEARCH: [
                OperationDriverEntry(
                    driver_ref="catalog_elasticsearch_driver",
                    source="auto",
                ),
            ],
        },
    )


def _catalog_routing_pg() -> CatalogRoutingConfig:
    """PG-only catalog routing — no ES."""
    return CatalogRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="catalog_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="catalog_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.SEARCH: [
                OperationDriverEntry(
                    driver_ref="catalog_postgresql_driver",
                    source="operator",
                ),
            ],
        },
    )


def _collection_routing_es() -> CollectionRoutingConfig:
    """ES-only collection routing."""
    return CollectionRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="collection_elasticsearch_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="collection_elasticsearch_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.SEARCH: [
                OperationDriverEntry(
                    driver_ref="collection_elasticsearch_driver",
                    source="operator",
                ),
            ],
        },
    )


def _collection_routing_es_pg() -> CollectionRoutingConfig:
    """ES_PG collection routing — PG preferred WRITE/READ, ES SEARCH + async."""
    return CollectionRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="collection_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
                OperationDriverEntry(
                    driver_ref="collection_elasticsearch_driver",
                    write_mode=WriteMode.ASYNC,
                    on_failure=FailurePolicy.OUTBOX,
                    secondary_index=True,
                    source="auto",
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="collection_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.SEARCH: [
                OperationDriverEntry(
                    driver_ref="collection_elasticsearch_driver",
                    source="auto",
                ),
            ],
        },
    )


def _collection_routing_pg() -> CollectionRoutingConfig:
    """PG-only collection routing."""
    return CollectionRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="collection_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="collection_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.SEARCH: [
                OperationDriverEntry(
                    driver_ref="collection_postgresql_driver",
                    source="operator",
                ),
            ],
        },
    )


def _items_routing_es() -> ItemsRoutingConfig:
    """ES-only items routing."""
    return ItemsRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.SEARCH: [
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_driver",
                    source="operator",
                ),
            ],
        },
    )


def _items_routing_es_pg() -> ItemsRoutingConfig:
    """ES_PG items routing — PG preferred WRITE/READ, ES SEARCH + async."""
    return ItemsRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="items_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_driver",
                    write_mode=WriteMode.ASYNC,
                    on_failure=FailurePolicy.OUTBOX,
                    secondary_index=True,
                    source="auto",
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="items_postgresql_driver",
                ),
            ],
            Operation.SEARCH: [
                OperationDriverEntry(
                    driver_ref="items_elasticsearch_driver",
                    source="auto",
                ),
            ],
        },
    )


def _items_routing_pg() -> ItemsRoutingConfig:
    """PG-only items routing."""
    return ItemsRoutingConfig(
        operations={
            Operation.WRITE: [
                OperationDriverEntry(
                    driver_ref="items_postgresql_driver",
                    on_failure=FailurePolicy.FATAL,
                ),
            ],
            Operation.READ: [
                OperationDriverEntry(
                    driver_ref="items_postgresql_driver",
                ),
            ],
            Operation.SEARCH: [
                OperationDriverEntry(
                    driver_ref="items_postgresql_driver",
                    source="operator",
                ),
            ],
        },
    )


# ---------------------------------------------------------------------------
# Bundle builders — one per concern, so the two presets below stay disjoint
# ---------------------------------------------------------------------------


def _coerce_params(params: BaseModel) -> StacPresetParams:
    """Coerce an arbitrary params model to ``StacPresetParams`` defensively.

    The composite lifecycle validates incoming params against the composite's
    ``params_model`` (``StacPresetParams``) and forwards the SAME instance to
    each child's ``apply``; this guard also covers a child applied directly
    with a foreign params model.
    """
    if isinstance(params, StacPresetParams):
        return params
    return StacPresetParams.model_validate(
        params.model_dump() if hasattr(params, "model_dump") else {}
    )


def _build_stac_storage_bundle(
    params: StacPresetParams,
    *,
    catalog_id: str = "",  # noqa: ARG001 — scope carried by lifecycle, not the SSOT body
    collection_id: Optional[str] = None,  # noqa: ARG001
) -> PresetBundle:
    """Build the SSOT-only bundle: a single ``StacStorageConfig`` entry.

    This is the "enable STAC" half — the SSOT signal the PG/ES drivers read at
    runtime to materialize STAC slices, sidecars, and ES routes.  Present even
    for ``stac_level=none`` (carries ``stac_level=NONE``); revoke removes it and
    the absence restores the platform default (no STAC).
    """
    return PresetBundle(
        entries=(
            PresetBundleEntry(
                slot="stac_storage_config",
                config_cls=StacStorageConfig,
                instance=StacStorageConfig(
                    stac_level=params.stac_level,
                    stac_storage=params.stac_storage,
                ),
                rollback_priority=5,
            ),
        )
    )


def _build_stac_routing_bundle(
    params: StacPresetParams,
    *,
    catalog_id: str = "",  # noqa: ARG001 — scope carried by lifecycle
    collection_id: Optional[str] = None,  # noqa: ARG001
) -> PresetBundle:
    """Build the routing-only bundle: per-tier routing for the chosen backend.

    This is the "decide the routing" half — it wires which drivers handle
    WRITE/READ/SEARCH at each tier enabled by ``stac_level``, for the backend
    given by ``stac_storage``.  It writes NO ``StacStorageConfig``: routing is
    orthogonal to the STAC signal, so a catalog can carry this routing with or
    without STAC materialization enabled.  ``stac_level=none`` => empty bundle.
    """
    level = params.stac_level
    backend = params.stac_storage

    if level == StacLevel.NONE:
        return PresetBundle(entries=())

    entries: List[PresetBundleEntry] = []

    # Catalog-tier routing (enabled when catalog+ level)
    if catalog_stac_enabled(level):
        if es_stac(backend) and pg_stac(backend):
            cat_routing = _catalog_routing_es_pg()
        elif es_stac(backend):
            cat_routing = _catalog_routing_es()
        else:
            cat_routing = _catalog_routing_pg()

        entries.append(
            PresetBundleEntry(
                slot="catalog_routing",
                config_cls=CatalogRoutingConfig,
                instance=cat_routing,
                rollback_priority=30,
            )
        )
        # The catalog_stac PG slice is NOT authored here: when pg_stac is
        # active, CatalogPostgresqlDriver._resolve_sidecars_for_catalog adds it
        # at runtime by reading this scope's StacStorageConfig — the SSOT the
        # ``stac_storage`` preset writes.

    # Collection-tier routing (enabled when collection+ level)
    if collection_stac_enabled(level):
        if es_stac(backend) and pg_stac(backend):
            coll_routing = _collection_routing_es_pg()
        elif es_stac(backend):
            coll_routing = _collection_routing_es()
        else:
            coll_routing = _collection_routing_pg()

        entries.append(
            PresetBundleEntry(
                slot="collection_template",
                config_cls=CollectionRoutingConfig,
                instance=coll_routing,
                rollback_priority=20,
            )
        )
        # The collection_stac PG slice is NOT authored here: when pg_stac is
        # active, CollectionPostgresqlDriver._resolve_sidecars_for_catalog adds
        # it at runtime by reading this scope's StacStorageConfig (the SSOT).

    # Items-tier routing (enabled when items level)
    if items_stac_enabled(level):
        if es_stac(backend) and pg_stac(backend):
            items_routing = _items_routing_es_pg()
        elif es_stac(backend):
            items_routing = _items_routing_es()
        else:
            items_routing = _items_routing_pg()

        entries.append(
            PresetBundleEntry(
                slot="items_template",
                config_cls=ItemsRoutingConfig,
                instance=items_routing,
                rollback_priority=10,
            )
        )
        # Items stac_metadata sidecar is handled by the StacStorageConfig signal
        # (SidecarRegistry injection via StacItemsSidecar.get_default_config),
        # not by an authored sidecars list — no driver-config entry needed here.

    return PresetBundle(entries=tuple(entries))


# ---------------------------------------------------------------------------
# Presets — two single-responsibility children + the composite that chains them
# ---------------------------------------------------------------------------


class StacRoutingPreset(BundlePreset):
    """Per-tier storage routing for the chosen backend — the routing half.

    Writes ``CatalogRoutingConfig`` / ``CollectionRoutingConfig`` /
    ``ItemsRoutingConfig`` for the tiers enabled by ``stac_level`` and the
    backend named by ``stac_storage``.  Writes NO ``StacStorageConfig`` — STAC
    materialization is a separate, orthogonal flip (see ``StacStoragePreset``).

    Apply this first to "decide the routing", then apply ``stac_storage`` to
    enable STAC, then ingest.
    """

    name = "stac_routing"
    tier: ClassVar[PresetTier] = PresetTier.CATALOG
    catalog_scopable: ClassVar[bool] = True
    keywords: ClassVar[Tuple[str, ...]] = ("routing", "stac")
    params_model = StacPresetParams
    description = (
        "Per-tier storage routing for STAC ingestion.  Writes catalog / "
        "collection / items routing entries for the tiers enabled by "
        "``stac_level`` and the backend named by ``stac_storage`` (ES / PG / "
        "ES_PG).  Writes no StacStorageConfig — pair with the ``stac_storage`` "
        "preset to actually enable STAC materialization.  Default: "
        "level=collection, storage=ES_PG."
    )

    def build(self, catalog_id: str = "", **_scope: str) -> PresetBundle:  # noqa: ARG002
        return _build_stac_routing_bundle(StacPresetParams(), catalog_id=catalog_id)

    def _build_bundle(
        self, params: BaseModel, scope_kwargs: Dict[str, str]
    ) -> PresetBundle:
        return _build_stac_routing_bundle(
            _coerce_params(params),
            catalog_id=scope_kwargs.get("catalog_id", ""),
            collection_id=scope_kwargs.get("collection_id"),
        )


class StacStoragePreset(BundlePreset):
    """The ``StacStorageConfig`` SSOT flip — the "enable STAC" half.

    Writes the single ``StacStorageConfig`` entry that signals STAC
    materialization at the scope.  The PG/ES drivers read this SSOT at runtime
    to add the ``catalog_stac`` / ``collection_stac`` wrapper slices and the
    per-item ``stac_metadata`` sidecar.  Writes NO routing — pair with the
    ``stac_routing`` preset (apply routing first) so the drivers it references
    exist before the signal flips.

    ``stac_level=none`` writes ``StacStorageConfig(stac_level=NONE)``; revoke
    removes it and the absence restores the default (no STAC).
    """

    name = "stac_storage"
    tier: ClassVar[PresetTier] = PresetTier.CATALOG
    catalog_scopable: ClassVar[bool] = True
    keywords: ClassVar[Tuple[str, ...]] = ("stac", "config")
    params_model = StacPresetParams
    description = (
        "Writes the StacStorageConfig SSOT that enables STAC materialization "
        "at the scope (the signal PG/ES drivers read to add STAC slices, "
        "sidecars, and ES routes).  Parameters: ``stac_level`` (none/catalog/"
        "collection/items) and ``stac_storage`` (ES/PG/ES_PG).  Writes no "
        "routing — pair with ``stac_routing`` (applied first).  Set "
        "stac_level=none to revoke STAC from the scope."
    )

    def build(self, catalog_id: str = "", **_scope: str) -> PresetBundle:  # noqa: ARG002
        return _build_stac_storage_bundle(StacPresetParams(), catalog_id=catalog_id)

    def _build_bundle(
        self, params: BaseModel, scope_kwargs: Dict[str, str]
    ) -> PresetBundle:
        return _build_stac_storage_bundle(
            _coerce_params(params),
            catalog_id=scope_kwargs.get("catalog_id", ""),
            collection_id=scope_kwargs.get("collection_id"),
        )


class StacPreset(CompositePreset):
    """One-shot STAC opt-in — composes routing + the StacStorageConfig flip.

    Backward-compatible umbrella over the two single-responsibility children:
    ``stac_routing`` (decide the routing) then ``stac_storage`` (enable STAC).
    Apply order matters — routing is wired first so the drivers the SSOT
    references exist before the signal flips; revoke reverses (SSOT off, then
    routing).  Both children receive the same ``StacPresetParams``.

    Equivalent to applying ``stac_routing`` and ``stac_storage`` by hand; use
    this when you want the whole STAC stack in a single call, or apply the two
    children separately to stage routing and STAC independently.
    """

    name: ClassVar[str] = "stac"
    description: ClassVar[str] = (
        "One-shot STAC opt-in: composes ``stac_routing`` (per-tier routing for "
        "the chosen backend) then ``stac_storage`` (the StacStorageConfig SSOT "
        "that enables materialization).  Default: level=collection, "
        "storage=ES_PG.  Set stac_level=none to revoke STAC.  Apply the two "
        "children separately to stage routing and STAC independently."
    )
    keywords: ClassVar[Tuple[str, ...]] = ("stac", "routing", "composite")
    tier: ClassVar[PresetTier] = PresetTier.CATALOG
    catalog_scopable: ClassVar[bool] = True
    params_model: ClassVar[Type[BaseModel]] = StacPresetParams
    compose: ClassVar[Tuple[str, ...]] = ("stac_routing", "stac_storage")
