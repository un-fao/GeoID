from typing import ClassVar

from dynastore.tools.discovery import register_plugin

from .tasks import (
    BulkCatalogReindexInputs,
    BulkCatalogReindexTask,
    BulkCollectionReindexInputs,
    BulkCollectionReindexTask,
)


class _IndexerTasksConsumerRole:
    """Marker plugin: deployment hosts ES indexer tasks → consumes catalog events.

    Implements ``CatalogEventConsumer`` (structural — see
    ``models/protocols/event_consumer.py``).  Registered at package import
    so that loading this package — which only happens when the
    ``task_elasticsearch_indexer_deps`` extra is installed (catalog, worker)
    — flips ``CatalogModule``'s consumer gate on.

    Services that load the ES module without indexer tasks (maps's
    ``module_catalog_elasticsearch`` for query/resolver work) do not import
    this package, the marker stays unregistered, and the 16-shard durable
    consumer stays dormant on those services.
    """

    is_catalog_event_consumer: ClassVar[bool] = True


register_plugin(_IndexerTasksConsumerRole())


__all__ = [
    "BulkCatalogReindexInputs",
    "BulkCatalogReindexTask",
    "BulkCollectionReindexInputs",
    "BulkCollectionReindexTask",
]
