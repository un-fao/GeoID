"""Disambiguation of the 'Could not resolve storage' error.

When ``_apply_query_transformations`` cannot resolve physical storage,
the raised ValueError must name which leg failed (phys_schema vs
phys_table) and whether a db_resource was threaded through.  Without
that, every miss surfaces as the same opaque 500 in production logs and
we cannot tell whether to fix a catalog row, a driver config, or a
caller that forgot to pass its conn.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.catalog.item_service import ItemService
from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
from dynastore.models.query_builder import QueryRequest


def _build_service_with_resolvers(phys_schema, phys_table):
    svc = ItemService(engine=MagicMock())
    # Patch the abstract resolvers on this instance so we control both legs.
    svc._resolve_physical_schema = AsyncMock(return_value=phys_schema)
    svc._resolve_physical_table = AsyncMock(return_value=phys_table)
    return svc


@pytest.mark.parametrize(
    "phys_schema,phys_table,expected_substrings",
    [
        (None, "t_x", ["phys_schema=None", "phys_table='t_x'"]),
        ("s_x", None, ["phys_schema='s_x'", "phys_table=None"]),
        (None, None, ["phys_schema=None", "phys_table=None"]),
    ],
)
@pytest.mark.asyncio
async def test_resolve_storage_error_names_missing_leg(
    phys_schema, phys_table, expected_substrings,
):
    svc = _build_service_with_resolvers(phys_schema, phys_table)
    col_config = ItemsPostgresqlDriverConfig()

    # Bypass discovery — no transformers needed for this code path.
    with patch(
        "dynastore.tools.discovery.get_protocols", return_value=[],
    ):
        with pytest.raises(ValueError) as excinfo:
            await svc._apply_query_transformations(
                QueryRequest(),  # bare request — no cql_filter, no transforms
                {"catalog_id": "cat", "collection_id": "col"},
                catalog_id="cat",
                collection_id="col",
                col_config=col_config,
                db_resource=MagicMock(),
            )

    msg = str(excinfo.value)
    assert "Could not resolve storage for cat:col" in msg
    for substr in expected_substrings:
        assert substr in msg, f"missing {substr!r} in: {msg}"
    assert "db_resource=passed" in msg


@pytest.mark.asyncio
async def test_resolve_storage_error_marks_absent_db_resource():
    svc = _build_service_with_resolvers(None, None)
    col_config = ItemsPostgresqlDriverConfig()

    with patch(
        "dynastore.tools.discovery.get_protocols", return_value=[],
    ):
        with pytest.raises(ValueError, match="db_resource=absent"):
            await svc._apply_query_transformations(
                QueryRequest(),
                {"catalog_id": "cat", "collection_id": "col"},
                catalog_id="cat",
                collection_id="col",
                col_config=col_config,
                db_resource=None,
            )
