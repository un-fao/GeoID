"""Platform notebook registrations for the elasticsearch module.

Registers Elasticsearch-specific pattern notebooks:
  - storage_combo_private_es_plus_pg — PG primary + private ES index
  - storage_combo_private_es_only    — private ES index only
  - catalog_metadata_search          — ES/PG collection metadata search

Import this module during ElasticsearchModule lifespan (before NotebooksModule seeds).
"""
from pathlib import Path

from dynastore.modules.notebooks.example_registry import register_platform_notebook

_HERE = Path(__file__).parent / "notebooks"
_REG = "dynastore.modules.elasticsearch"

register_platform_notebook(
    notebook_id="storage_combo_private_es_plus_pg",
    registered_by=_REG,
    notebook_path=_HERE / "private_es_plus_pg.ipynb",
    title={"en": "PG Primary + Private ES Index"},
    description={"en": "Route WRITE to both PostgreSQL and a private ES index; READ from PG; SEARCH via the private ES index (geoid tokens only)."},
    tags=["storage", "postgresql", "elasticsearch", "private", "routing"],
)

register_platform_notebook(
    notebook_id="storage_combo_private_es_only",
    registered_by=_REG,
    notebook_path=_HERE / "private_es_only.ipynb",
    title={"en": "Private ES Index Only"},
    description={"en": "All operations routed to a private Elasticsearch index — only geoid tokens are stored, no item attributes persisted."},
    tags=["storage", "elasticsearch", "private"],
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
