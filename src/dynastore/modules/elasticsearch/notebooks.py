"""Platform notebook registrations for the elasticsearch module.

Registers Elasticsearch-specific pattern notebooks:
  - storage_combo_obfuscated_es_plus_pg — PG primary + obfuscated ES search
  - storage_combo_es_obfuscated_only    — ES obfuscated index only
  - catalog_metadata_search             — ES/PG collection metadata search

Import this module during ElasticsearchModule lifespan (before NotebooksModule seeds).
"""
from pathlib import Path

from dynastore.modules.notebooks.example_registry import register_platform_notebook

_HERE = Path(__file__).parent / "notebooks"
_REG = "dynastore.modules.elasticsearch"

register_platform_notebook(
    notebook_id="storage_combo_obfuscated_es_plus_pg",
    registered_by=_REG,
    notebook_path=_HERE / "obfuscated_es_plus_pg.ipynb",
    title={"en": "PG Primary + Obfuscated ES Search"},
    description={"en": "Route WRITE to both PostgreSQL and obfuscated ES; READ from PG; SEARCH via obfuscated ES shared index."},
    tags=["storage", "postgresql", "elasticsearch", "obfuscated", "routing"],
)

register_platform_notebook(
    notebook_id="storage_combo_es_obfuscated_only",
    registered_by=_REG,
    notebook_path=_HERE / "es_obfuscated_only.ipynb",
    title={"en": "ES Obfuscated Index Only"},
    description={"en": "Write-only to a shared obfuscated Elasticsearch index per catalog. No listing or full-text search."},
    tags=["storage", "elasticsearch", "obfuscated"],
)

register_platform_notebook(
    notebook_id="catalog_metadata_search",
    registered_by=_REG,
    notebook_path=_HERE / "catalog_metadata_search.ipynb",
    title={"en": "Catalog Metadata Search"},
    description={
        "en": (
            "Configure and query the collection metadata search backend — "
            "Elasticsearch or PostgreSQL via the metadata.override routing. "
            "Covers auto-discovery, explicit config, multilanguage search, spatial bbox, "
            "CQL2 filter, aggregations, and PG fallback."
        )
    },
    tags=["catalog", "metadata", "elasticsearch", "search", "configuration"],
)
