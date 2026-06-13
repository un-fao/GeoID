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

"""Platform notebook registrations for the catalog module.

Registers collection configuration notebooks:
  - items_write_policy            — write policy configuration
  - collection_feature_type            — feature type definition
  - collection_metadata_enrichment     — metadata enrichment / METADATA storage routing
  - collection_shapefile_strict_schema — end-to-end strict-schema demo (PRs #178/179/180/181)

Import this module during CatalogModule lifespan (before NotebooksModule seeds).
"""
from pathlib import Path

from dynastore.modules.notebooks.example_registry import register_platform_notebook

_HERE = Path(__file__).parent / "notebooks"
_REG = "dynastore.modules.catalog"

register_platform_notebook(
    notebook_id="items_write_policy",
    registered_by=_REG,
    notebook_path=_HERE / "items_write_policy.ipynb",
    title={"en": "Items Write Policy"},
    description={"en": "Configure write-once semantics, external ID tracking, and temporal validity for items."},
    tags=["items", "write-policy", "configuration"],
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

register_platform_notebook(
    notebook_id="collection_shapefile_strict_schema",
    registered_by=_REG,
    notebook_path=_HERE / "shapefile_strict_schema.ipynb",
    title={"en": "Strict-Schema Collection from a Shapefile"},
    description={"en": "End-to-end demo: upload shapefile → derive schema via OGR → enforce strict schema → optimize storage by columns → refuse duplicate identity."},
    tags=["collection", "schema", "shapefile", "strict-mode", "ogr", "ingestion", "demo"],
)

# Catalog provisioning + collection lifecycle — migrated from showcase/catalog/
register_platform_notebook(
    notebook_id="catalog_provision_tenant_catalog",
    registered_by=_REG,
    notebook_path=_HERE / "cat01_provision_tenant_catalog.ipynb",
    title={"en": "Provision a Tenant Catalog"},
    tags=["catalog", "tenant", "provisioning"],
)
register_platform_notebook(
    notebook_id="catalog_create_collection_with_layerconfig",
    registered_by=_REG,
    notebook_path=_HERE / "cat02_create_collection_with_layerconfig.ipynb",
    title={"en": "Create a Collection with layer_config"},
    tags=["catalog", "collection", "layer-config"],
)
register_platform_notebook(
    notebook_id="catalog_schema_evolution_add_column",
    registered_by=_REG,
    notebook_path=_HERE / "cat03_schema_evolution_add_column.ipynb",
    title={"en": "Schema Evolution — Add a Column"},
    tags=["catalog", "schema", "evolution", "migration"],
)

# Write policy — migrated from showcase/write_policy/
register_platform_notebook(
    notebook_id="write_policy_external_id_deduplication",
    registered_by=_REG,
    notebook_path=_HERE / "wp01_external_id_deduplication.ipynb",
    title={"en": "Write Policy — external_id Deduplication"},
    tags=["catalog", "write-policy", "external-id", "deduplication"],
)
register_platform_notebook(
    notebook_id="write_policy_content_hash_idempotent_update",
    registered_by=_REG,
    notebook_path=_HERE / "wp02_content_hash_idempotent_update.ipynb",
    title={"en": "Write Policy — content_hash Idempotent Update"},
    tags=["catalog", "write-policy", "content-hash", "idempotent"],
)

# Transactional ingest — migrated from showcase/transactions/
register_platform_notebook(
    notebook_id="catalog_stac_item_ingest",
    registered_by=_REG,
    notebook_path=_HERE / "tx01_stac_item_ingest.ipynb",
    title={"en": "STAC Item Ingest"},
    tags=["catalog", "stac", "ingest", "transaction"],
)
register_platform_notebook(
    notebook_id="catalog_bulk_feature_ingest",
    registered_by=_REG,
    notebook_path=_HERE / "tx02_bulk_feature_ingest.ipynb",
    title={"en": "Bulk Feature Ingest"},
    tags=["catalog", "bulk", "ingest", "transaction"],
)
register_platform_notebook(
    notebook_id="catalog_soft_delete_by_external_id",
    registered_by=_REG,
    notebook_path=_HERE / "tx03_soft_delete_by_external_id.ipynb",
    title={"en": "Soft Delete by external_id"},
    tags=["catalog", "soft-delete", "external-id"],
)
