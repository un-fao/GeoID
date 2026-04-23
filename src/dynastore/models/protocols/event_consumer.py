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

"""Catalog event-consumer marker protocol."""

from __future__ import annotations

from typing import ClassVar, Protocol, runtime_checkable


@runtime_checkable
class CatalogEventConsumer(Protocol):
    """Marker — module participates as a consumer of the catalog event outbox.

    A module opts in by setting ``is_catalog_event_consumer: ClassVar[bool] = True``.
    ``CatalogModule`` checks for at least one such module before starting the
    16-shard durable outbox consumer; services without one (maps, auth, tools,
    and any future query-only deployment) skip the consumer spawn entirely
    and avoid the asyncpg connect storm that costs nothing on those services.

    Why narrower than ``IndexerProtocol``
    -------------------------------------
    A service may legitimately register ``IndexerProtocol`` for query-side
    work (e.g. consulting an ES index from a request-path resolver) without
    needing to consume the catalog event outbox.  Gating consumer start on
    ``IndexerProtocol`` re-arms the consumer wherever ES is loaded, which
    defeats the deployment-role separation between
    catalog/worker (consume + index) and maps/auth (query-side only).

    Set the marker exclusively on modules whose presence indicates the
    deployment is responsible for consuming catalog mutations and dispatching
    them onward — today that is ``ElasticsearchModule`` only.  Future
    consumer-role modules (e.g. a webhook fan-out, a BigQuery streaming
    indexer) should set the same marker.
    """

    is_catalog_event_consumer: ClassVar[bool]
