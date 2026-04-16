"""DTOs for the paginated deep configuration view endpoints."""

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class ResolvedDriverEntry(BaseModel):
    """A driver resolved from a routing config entry, with its effective config."""

    driver_id: str = Field(
        ..., description="Driver class name, e.g. 'CollectionPostgresqlDriver'."
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


class ConfigViewEntry(BaseModel):
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


class CollectionConfigView(BaseModel):
    """Deep config view for a single collection scope."""

    collection_id: str
    catalog_id: str
    configs: Dict[str, ConfigViewEntry] = Field(
        default_factory=dict,
        description="Effective configs at this collection scope, keyed by class_key.",
    )
    categories: Optional[Dict[str, ConfigPage]] = Field(
        None,
        description=(
            "Paginated child categories: 'records', 'assets'. Null if depth=0."
        ),
    )


class CatalogConfigView(BaseModel):
    """Deep config view for a single catalog scope."""

    catalog_id: str
    configs: Dict[str, ConfigViewEntry] = Field(
        default_factory=dict,
        description="Effective configs at this catalog scope, keyed by class_key.",
    )
    categories: Optional[Dict[str, ConfigPage]] = Field(
        None,
        description=(
            "Paginated child categories: 'collections', 'assets'. Null if depth=0."
        ),
    )


class PlatformConfigView(BaseModel):
    """Deep config view for the platform scope."""

    scope: str = Field("platform", frozen=True)
    configs: Dict[str, ConfigViewEntry] = Field(
        default_factory=dict,
        description="Effective platform-level configs, keyed by class_key.",
    )
    categories: Optional[Dict[str, ConfigPage]] = Field(
        None,
        description=(
            "Paginated child categories: 'catalogs'. Null if depth=0."
        ),
    )


class PatchConfigBody(BaseModel):
    """Request body for PATCH /configs/.../view — write one config at a scope level."""

    class_key: str = Field(..., description="PluginConfig class_key to set.")
    value: Dict[str, Any] = Field(..., description="New configuration payload.")
