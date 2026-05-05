from .module import ElasticsearchModule
from .es_catalog_config import ElasticsearchCatalogConfig

# NOTE: ``ElasticsearchCollectionConfig`` and ``is_collection_private``
# were retired in Cycle C of the config-API restructure
# (2026-05-05).  The class carried only the ``private`` flag, which
# duplicated the catalog-tier ``ElasticsearchCatalogConfig.private``
# (still present, scheduled for replacement in Cycle E by a first-class
# ``is_private: bool`` on ``CollectionPluginConfig``).  The cached
# resolver had only one runtime consumer — the synthetic
# ``routing_resolution`` block, also dropped in Cycle C.

__all__ = [
    "ElasticsearchModule",
    "ElasticsearchCatalogConfig",
]
