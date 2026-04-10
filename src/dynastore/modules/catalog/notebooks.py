"""Platform notebook registrations for the catalog module.

Registers collection configuration notebooks:
  - collection_write_policy        — write policy configuration
  - collection_feature_type        — feature type definition
  - collection_metadata_enrichment — metadata enrichment / METADATA storage routing

Import this module during CatalogModule lifespan (before NotebooksModule seeds).
"""
from pathlib import Path

from dynastore.modules.notebooks.example_registry import register_platform_notebook

_HERE = Path(__file__).parent / "notebooks"
_REG = "dynastore.modules.catalog"

register_platform_notebook(
    notebook_id="collection_write_policy",
    registered_by=_REG,
    notebook_path=_HERE / "write_policy.ipynb",
    title={"en": "Collection Write Policy"},
    description={"en": "Configure write-once semantics, external ID tracking, and temporal validity for a collection."},
    tags=["collection", "write-policy", "configuration"],
)

register_platform_notebook(
    notebook_id="collection_feature_type",
    registered_by=_REG,
    notebook_path=_HERE / "feature_type.ipynb",
    title={"en": "Collection Feature Type Definition"},
    description={"en": "Define the feature type schema (field names, types, geometry) for a collection."},
    tags=["collection", "feature-type", "schema", "configuration"],
)

register_platform_notebook(
    notebook_id="collection_metadata_enrichment",
    registered_by=_REG,
    notebook_path=_HERE / "metadata_enrichment.ipynb",
    title={"en": "Collection Metadata Enrichment"},
    description={"en": "Configure METADATA storage routing — which storage drivers supply collection metadata at read time."},
    tags=["collection", "metadata", "enrichment", "configuration"],
)
