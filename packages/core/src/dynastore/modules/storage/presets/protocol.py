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

"""Routing preset protocol + bundle dataclass (#847).

A preset is a thin factory that emits a validated ``PresetBundle`` of
routing configs plus opt-in audience configs. Each bundle entry is
applied through the standard ``ConfigsProtocol.update_config(...)``
lifecycle (validate → apply); presets do NOT bypass validation. The
catalog-tier cascade validator (#960 scope 4) and the existing
items/collection cascade handlers are the load-bearing invariants; this
protocol just gives operators named, cascade-consistent bundles to
apply with a single admin call.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional, Protocol, runtime_checkable

from dynastore.modules.db_config.plugin_config import PluginConfig
from dynastore.modules.storage.routing_config import (
    CatalogRoutingConfig,
    CollectionRoutingConfig,
    ItemsRoutingConfig,
)


@dataclass
class PresetBundle:
    """Output of ``RoutingPreset.build(catalog_id)``.

    All four fields are optional so a preset can target a subset of the
    routing tiers; the admin apply endpoint skips ``None`` slots and
    walks ``audience_configs`` in dict-insertion order.
    """

    catalog_routing: Optional[CatalogRoutingConfig]
    collection_template: Optional[CollectionRoutingConfig]
    items_template: Optional[ItemsRoutingConfig]
    audience_configs: Dict[str, PluginConfig] = field(default_factory=dict)


@runtime_checkable
class RoutingPreset(Protocol):
    """Structural contract every preset must satisfy.

    ``name`` is the registry key + URL path segment; ``description`` is
    surfaced by ``GET /admin/presets``. ``build(catalog_id)`` is called
    per apply — preset implementations stay stateless so the registry
    can be a plain dict.
    """

    name: str
    description: str

    def build(self, catalog_id: str) -> PresetBundle: ...
