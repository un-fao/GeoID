"""Unified input model for the public ``elasticsearch_indexer`` OGC Process.

A single shape that can drive either a catalog-wide reindex (when only
``catalog_id`` is set) or a single-collection reindex (when ``collection_id``
is also set). The dispatcher class :class:`.indexer_task.ElasticsearchIndexerTask`
adapts this to the legacy ``BulkCatalogReindexInputs`` / ``BulkCollectionReindexInputs``
expected by the underlying implementations.
"""
from typing import Literal, Optional

from pydantic import BaseModel, Field


class ElasticsearchIndexerRequest(BaseModel):
    catalog_id: str = Field(
        ..., description="Catalog to reindex into the per-tenant items index."
    )
    collection_id: Optional[str] = Field(
        None,
        description=(
            "If set, only reindex this collection. Otherwise reindex every "
            "collection of the catalog that routes through the regular ES driver."
        ),
    )
    mode: Optional[Literal["catalog", "private"]] = Field(
        None,
        description=(
            "Override the indexer mode. If omitted, falls back to the catalog's "
            "configured indexer mode."
        ),
    )
    driver: Optional[str] = Field(
        None,
        description=(
            "Restrict the reindex to a single secondary driver (e.g. only the "
            "regular ES driver). Omit to reindex through every active driver."
        ),
    )
