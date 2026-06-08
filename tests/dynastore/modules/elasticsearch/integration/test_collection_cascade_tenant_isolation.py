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

"""Live-ES verification of tenant-safe collection cascade cleanup (#1764 / #1750).

Drives :meth:`CollectionElasticsearchDriver.drop_storage` against a REAL
Elasticsearch / OpenSearch cluster — no app lifespan and no database. The test
builds its own async client from ``ES_HOST`` / ``ES_PORT`` (default
``localhost:9200``) and a throwaway index, so it exercises the actual
``delete_by_query`` (``term: {catalog_id}`` + ``routing``) path the routing-driven
cascade relies on.

This pins the #1750 repro at *document* granularity, which the index-level
``test_es_cascade_cleanup.py`` does not: collection docs for two catalogs A and
B share the singleton ``{prefix}-collections`` index; hard-dropping A's storage
must remove only A's docs and leave B's untouched.

Requires a live ES instance (``@pytest.mark.elasticsearch``); skipped otherwise.
Self-contained on purpose — it does not use the app-driven fixtures in this
directory's ``conftest.py``.
"""
from __future__ import annotations

import os
import uuid

import pytest

pytestmark = [pytest.mark.elasticsearch, pytest.mark.asyncio]


def _es_client():
    """Build a standalone async client mirroring ``client._build_client`` env."""
    from opensearchpy import AsyncOpenSearch

    host = os.environ.get("ES_HOST", "localhost")
    port = int(os.environ.get("ES_PORT", "9200"))
    use_ssl = os.environ.get("ES_USE_SSL", "false").strip().lower() in ("1", "true", "yes")
    scheme = "https" if use_ssl else "http"
    return AsyncOpenSearch(
        hosts=[f"{scheme}://{host}:{port}"],
        use_ssl=use_ssl,
        verify_certs=False,
    )


async def _count(client, index: str, catalog_id: str) -> int:
    """Count docs for *catalog_id* using the SAME term+routing shape as drop_storage."""
    res = await client.count(
        index=index,
        body={"query": {"term": {"catalog_id": catalog_id}}},
        params={"routing": catalog_id},
    )
    return int(res["count"])


async def test_drop_storage_removes_only_target_catalog_collection_docs():
    """A hard ``drop_storage`` on catalog A removes A's collection docs and
    leaves catalog B's docs in the shared ``{prefix}-collections`` index."""
    from unittest.mock import patch

    from dynastore.modules.elasticsearch.collection_es_driver import (
        CollectionElasticsearchDriver,
    )

    client = _es_client()
    index = f"test-1764-collections-{uuid.uuid4().hex[:8]}"
    cat_a = "cat-1764-a"
    cat_b = "cat-1764-b"

    # ``catalog_id`` MUST be a keyword for the ``term`` filter to match — this
    # mirrors the production collections-index mapping.
    await client.indices.create(
        index=index,
        body={
            "settings": {"number_of_shards": 1, "number_of_replicas": 0},
            "mappings": {
                "properties": {
                    "catalog_id": {"type": "keyword"},
                    "collection_id": {"type": "keyword"},
                }
            },
        },
    )
    try:
        # Two collection docs per catalog, each routed by catalog_id and using
        # the production composite ``{catalog_id}:{collection_id}`` doc id.
        for cat in (cat_a, cat_b):
            for n in (1, 2):
                await client.index(
                    index=index,
                    id=f"{cat}:col{n}",
                    body={"catalog_id": cat, "collection_id": f"col{n}"},
                    params={"routing": cat, "refresh": "true"},
                )

        assert await _count(client, index, cat_a) == 2
        assert await _count(client, index, cat_b) == 2

        # Hard-drop catalog A's collection storage through the real driver path
        # (catalog scope → delete_by_query with term catalog_id + routing).
        with patch.object(
            CollectionElasticsearchDriver, "_index_name", return_value=index
        ), patch.object(
            CollectionElasticsearchDriver, "_get_client", return_value=client
        ):
            await CollectionElasticsearchDriver().drop_storage(
                cat_a, collection_id=None, soft=False
            )

        await client.indices.refresh(index=index)

        # The tenant-isolation guarantee of #1750: A gone, B fully intact.
        assert await _count(client, index, cat_a) == 0, (
            "catalog A's collection docs must be removed by drop_storage(A)"
        )
        assert await _count(client, index, cat_b) == 2, (
            "catalog B's collection docs must survive a delete scoped to catalog A"
        )
    finally:
        await client.indices.delete(index=index, params={"ignore_unavailable": "true"})
        await client.close()
