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

"""DTOs for the centralised composed-config API.

The response is a two-level (``scope -> topic`` — three-level under
``storage/drivers``) tree where the leaves are raw PluginConfig
payloads keyed by class name.  No wrapper envelopes, no duplicated
driver configs inline under routing entries, no ``class_key`` field
(the map key IS the class name).

Tier-of-origin diagnostics (``meta[ClassName] = {source}``) are opt-in
via the ``?meta=true`` query parameter; off by default to keep the
default response slim.
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, RootModel


class DriverRef(BaseModel):
    """Slim reference to a driver configured under a routing operation.

    ``config_ref`` is the class name the UI follows to look up the
    driver's full config under the sibling ``storage.drivers.*`` topic
    in the same response — avoiding per-routing-entry payload
    duplication.
    """

    driver_id: str = Field(
        ..., description="Driver class name, e.g. 'ItemsPostgresqlDriver'."
    )
    config_ref: Optional[str] = Field(
        None,
        description=(
            "Class name of the driver's config (sibling lookup under "
            "configs.storage.drivers.*).  Null when the driver has no "
            "registered config."
        ),
    )
    on_failure: str = Field(
        "fatal", description="Failure policy: fatal | warn | ignore."
    )
    write_mode: str = Field("sync", description="Write mode: sync | async.")


class ConfigLayer(BaseModel):
    """One tier in the waterfall trace for a single config entry.

    Tells the operator WHICH tier contributed WHAT — useful when the
    effective value at a collection comes from the catalog tier and the
    operator needs to know whether the platform default would take over
    if the catalog override were deleted.
    """

    level: str = Field(
        ...,
        description=(
            "Tier name in waterfall order: 'default' (code-level class "
            "default), 'platform', 'catalog', 'collection'."
        ),
    )
    present: bool = Field(
        ...,
        description=(
            "True when an explicit row exists at this tier (i.e. the tier "
            "contributed a delta that was merged into the effective value). "
            "False when the tier was empty for this class — the value was "
            "inherited from a higher (lower-precedence) tier."
        ),
    )


class ConfigMeta(BaseModel):
    """Tier-of-origin diagnostics for a single resolved config entry."""

    source: str = Field(
        ...,
        description=(
            "Tier that supplied the effective value: "
            "'collection' | 'catalog' | 'platform' | 'default'.  Single-tier "
            "summary; for the full waterfall trace see ``layers`` below."
        ),
    )
    layers: Optional[List[ConfigLayer]] = Field(
        default=None,
        description=(
            "Full waterfall trace for this config: one entry per tier "
            "(default, platform, catalog, collection) with ``present`` "
            "indicating whether that tier explicitly contributed a delta. "
            "Populated when ``?meta=true`` is requested."
        ),
    )
    json_schema: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "JSON Schema 2020-12 document for this config class, emitted by "
            "Pydantic's ``model_json_schema()``. Includes per-field title, "
            "description, type, default, examples, constraints — everything a "
            "dashboard form-builder needs. Populated when ``?docs=schema`` is "
            "requested; null otherwise (default ``?docs=none``). Orthogonal to "
            "``?meta=true``: the two flags can be combined to get both source "
            "diagnostics and schemas in one response."
        ),
    )


class ConfigPage(BaseModel):
    """A paginated page of child config objects at one category of a scope level."""

    category: str = Field(
        ...,
        description="Category name: 'collections' | 'assets' | 'catalogs'.",
    )
    total: int = Field(0, ge=0, description="Total items in this category.")
    page: int = Field(1, ge=1, description="Current page number (1-based).")
    page_size: int = Field(
        15, ge=1, le=100, description="Number of items per page."
    )
    links: List[Dict[str, str]] = Field(
        default_factory=list,
        description="Navigation links: rel='next' and rel='prev' when available.",
    )
    items: Optional[List[Any]] = Field(
        None,
        description=(
            "Items at this page, or null if this level was not expanded (depth "
            "not reached)."
        ),
    )


class CollectionConfigResponse(BaseModel):
    """Composed view of all effective configs at a single collection scope."""

    collection_id: str
    catalog_id: str
    configs: Dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Effective configs at this collection scope, nested as "
            "scope -> topic -> [sub ->] ClassName -> payload."
        ),
    )
    meta: Optional[Dict[str, ConfigMeta]] = Field(
        None,
        description=(
            "Per-class tier-of-origin diagnostics; populated only when "
            "?meta=true is requested."
        ),
    )
    routing_resolution: Optional[Dict[str, Dict[str, Dict[str, str]]]] = Field(
        None,
        description=(
            "Per-entity, per-op driver resolution: "
            "``{entity: {op: {driver_id, reason}}}``. Tells the operator "
            "WHICH driver fires for each operation at this collection — "
            "factoring in private-mode resolution "
            "(``ElasticsearchCatalogConfig.private`` + per-collection "
            "override).  Populated only when ``?meta=true``.  Currently "
            "covers ``items.{WRITE,READ,SEARCH}``; other entities will "
            "be added as their resolvers stabilise."
        ),
    )
    categories: Optional[Dict[str, ConfigPage]] = Field(
        None,
        description="Paginated child categories: 'assets'. Null if depth=0.",
    )


class CatalogConfigResponse(BaseModel):
    """Composed view of all effective configs at a single catalog scope."""

    catalog_id: str
    configs: Dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Effective configs at this catalog scope, nested as "
            "scope -> topic -> [sub ->] ClassName -> payload."
        ),
    )
    meta: Optional[Dict[str, ConfigMeta]] = Field(
        None,
        description=(
            "Per-class tier-of-origin diagnostics; populated only when "
            "?meta=true is requested."
        ),
    )
    categories: Optional[Dict[str, ConfigPage]] = Field(
        None,
        description=(
            "Paginated child categories: 'collections', 'assets'. Null if depth=0."
        ),
    )


class PlatformConfigResponse(BaseModel):
    """Composed view of all effective configs at the platform scope."""

    scope: str = Field("platform", frozen=True)
    configs: Dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Effective platform-level configs, nested as "
            "scope -> topic -> [sub ->] ClassName -> payload."
        ),
    )
    meta: Optional[Dict[str, ConfigMeta]] = Field(
        None,
        description=(
            "Per-class tier-of-origin diagnostics; populated only when "
            "?meta=true is requested."
        ),
    )
    categories: Optional[Dict[str, ConfigPage]] = Field(
        None,
        description="Paginated child categories: 'catalogs'. Null if depth=0.",
    )


class PatchConfigBody(RootModel[Dict[str, Optional[Dict[str, Any]]]]):
    """Partial composed configuration.

    Keys: class name.  Values:

    * non-null dict — partial merge into the stored config at this scope.
    * null          — delete the stored record at this scope (revert to
                      inherit / class default).
    """
    pass
