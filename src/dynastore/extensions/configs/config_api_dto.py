"""DTOs for the centralised Config API endpoints (/configs/.../config)."""

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, RootModel


class ResolvedDriverEntry(BaseModel):
    """A driver resolved from a routing config entry, with its effective config."""

    driver_id: str = Field(
        ..., description="Driver class name, e.g. 'ItemsPostgresqlDriver'."
    )
    on_failure: str = Field(..., description="Failure policy: fatal | warn | ignore.")
    write_mode: str = Field(..., description="Write mode: sync | async.")
    config_class_key: Optional[str] = Field(
        None,
        description="PluginConfig class_key for this driver, if registered.",
    )
    config: Optional[Dict[str, Any]] = Field(
        None,
        description="Effective (waterfall-resolved) driver config payload.",
    )


class ConfigEntry(BaseModel):
    """A single config class with its waterfall-resolved value and source tier."""

    class_key: str = Field(..., description="PluginConfig class_key.")
    value: Dict[str, Any] = Field(..., description="Effective configuration payload.")
    source: str = Field(
        ...,
        description=(
            "Tier that provided this value: 'collection' | 'catalog' | 'platform' | "
            "'default'."
        ),
    )
    resolved_drivers: Optional[Dict[str, List[ResolvedDriverEntry]]] = Field(
        None,
        description=(
            "For routing configs only. Maps operation name (WRITE, READ, SEARCH, "
            "TRANSFORM) to the list of resolved drivers with their effective configs."
        ),
    )


class ConfigPage(BaseModel):
    """A paginated page of child config objects at one category of a scope level."""

    category: str = Field(
        ...,
        description="Category name: 'collections' | 'assets' | 'records' | 'catalogs'.",
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
    configs: Dict[str, ConfigEntry] = Field(
        default_factory=dict,
        description="Effective configs at this collection scope, keyed by class_key.",
    )
    categories: Optional[Dict[str, ConfigPage]] = Field(
        None,
        description=(
            "Paginated child categories: 'records', 'assets'. Null if depth=0."
        ),
    )


class CatalogConfigResponse(BaseModel):
    """Composed view of all effective configs at a single catalog scope."""

    catalog_id: str
    configs: Dict[str, ConfigEntry] = Field(
        default_factory=dict,
        description="Effective configs at this catalog scope, keyed by class_key.",
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
    configs: Dict[str, ConfigEntry] = Field(
        default_factory=dict,
        description="Effective platform-level configs, keyed by class_key.",
    )
    categories: Optional[Dict[str, ConfigPage]] = Field(
        None,
        description=(
            "Paginated child categories: 'catalogs'. Null if depth=0."
        ),
    )


class PatchConfigBody(RootModel[Dict[str, Optional[Dict[str, Any]]]]):
    """Partial composed configuration.

    Keys: plugin_id (class key). Values:
      - non-null dict: partial merge into the stored config at this scope.
      - null: delete the stored record at this scope (revert to inherit / class default).
    """
    pass
