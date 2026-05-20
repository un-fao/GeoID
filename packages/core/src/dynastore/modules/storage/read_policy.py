#    Copyright 2025 FAO
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

"""
ItemsReadPolicy — the read-side companion to :class:`ItemsWritePolicy`.

Phase 3 of the items-policy consolidation (see
``docs/architecture/items-policy-consolidation-957-950.md``). The class
exists so collections can declare a read-time wire-shape contract and
an ordered chain of :class:`EntityTransformProtocol` keys. Drivers do
not consume this yet — that integration arrives with the read-path
rewrite. Adding the class today lets the config waterfall persist
read-policy seeds and unblocks admin-UI work in parallel.

Collection-scoped only (see [[feedback_items_policies_collection_scoped_only]]).
"""

import logging
from typing import ClassVar, List, Optional, Tuple

from pydantic import Field

from dynastore.models.mutability import Mutable
from dynastore.modules.db_config.plugin_config import PluginConfig
from dynastore.modules.storage.computed_fields import FeatureType

_logger = logging.getLogger(__name__)


class ItemsReadPolicy(PluginConfig):
    """Collection-scoped read-time wire-shape contract.

    Two concerns:

    - ``feature_type``: :class:`FeatureType` declaring which JSON Schema
      the response promises and which computed fields to surface beyond
      the schema's declared properties (``expose``).
    - ``output_transformers``: ordered list of class-key strings,
      resolved via the existing ``EntityTransformProtocol`` registry
      (same machinery as ``PrivateEntityTransformer``). Each transformer
      may add/remove/rewrite fields on the outgoing feature.

    The default ``feature_type.schema_ref = "items_write_policy.schema"``
    means: by default, the response shape equals the write shape. A
    collection that wants a different read projection overrides
    ``feature_type.expose`` or composes ``output_transformers``.

    There is no ``ItemsReadPolicy`` at the catalog or platform tier —
    item-shape decisions are inherently per-collection (see the
    auto-memory feedback rule on collection-scoped-only).
    """

    # Grouped with ItemsWritePolicy under ``items.policy`` — the composer keys
    # each leaf by ``class_key``, so this nests as ``items.policy.items_read_policy``
    # alongside ``items.policy.items_write_policy``. Storage/lookup is by class_key,
    # independent of this address.
    _address: ClassVar[Tuple[str, ...]] = (
        "platform",
        "catalog",
        "collection",
        "items",
        "policy",
    )
    _visibility: ClassVar[Optional[str]] = "collection"

    feature_type: Mutable[FeatureType] = Field(
        default_factory=FeatureType,
        description=(
            "Wire-shape contract. ``schema_ref`` defaults to the write-time "
            "schema; ``expose`` surfaces additional computed fields; "
            "``failure_mode`` controls transformer error handling."
        ),
    )

    output_transformers: Mutable[List[str]] = Field(
        default_factory=list,
        description=(
            "Ordered class keys (``EntityTransformProtocol`` registry) "
            "applied left-to-right on the outgoing feature. Empty list = "
            "no transformation beyond the driver's native read shape."
        ),
    )


async def _validate_read_policy(
    config: PluginConfig,
    catalog_id: Optional[str],
    collection_id: Optional[str],
    db_resource: Optional[object],
) -> None:
    """Reject ``feature_type.expose`` names the collection can't produce.

    Each ``expose`` entry must resolve to a ``ComputedField.resolved_name``
    from ``ItemsWritePolicy.compute`` or a declared ``ItemsSchema.fields``
    key at the same scope. Fail-fast at config-save so a typo never silently
    drops an expected output property. Skipped when the sibling configs are
    not yet present.
    """
    if not isinstance(config, ItemsReadPolicy):
        return
    expose = list(config.feature_type.expose or [])
    if not expose or not (catalog_id and collection_id):
        return

    try:
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.modules.storage.driver_config import (
            ItemsSchema,
            ItemsWritePolicy,
        )
        from dynastore.tools.discovery import get_protocol

        configs = get_protocol(ConfigsProtocol)
        if not configs:
            return

        wp = await configs.get_config(
            ItemsWritePolicy, catalog_id=catalog_id, collection_id=collection_id
        )
        computed = {cf.resolved_name for cf in getattr(wp, "compute", []) or []}
        schema = await configs.get_config(
            ItemsSchema, catalog_id=catalog_id, collection_id=collection_id
        )
        declared = set(getattr(schema, "fields", {}) or {})
        allowed = computed | declared
        unknown = [e for e in expose if e not in allowed]
        if unknown:
            raise ValueError(
                "ItemsReadPolicy.feature_type.expose references field(s) not produced "
                f"by ItemsWritePolicy.compute nor declared in ItemsSchema.fields for "
                f"{catalog_id}/{collection_id}: {sorted(unknown)}. "
                f"Computed: {sorted(computed)}; declared: {sorted(declared)}."
            )
    except ValueError:
        raise
    except Exception as exc:
        _logger.debug(
            "read_policy expose validation skipped for %s/%s: %s",
            catalog_id,
            collection_id,
            exc,
        )


ItemsReadPolicy.register_validate_handler(_validate_read_policy)


__all__ = ["ItemsReadPolicy"]
