"""Re-export shim — search data models have moved to ``dynastore.models.search_models``.

All public names are re-exported here so existing importers
(router, service, tests, protocols) continue to work unchanged.
"""
from dynastore.models.search_models import (  # noqa: F401
    ItemCollection,
    SearchBody,
    SearchLink,
)

__all__ = [
    "ItemCollection",
    "SearchBody",
    "SearchLink",
]
