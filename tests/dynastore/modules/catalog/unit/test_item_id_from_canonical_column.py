"""Regression: the row-mapper hub must read the wire id from the canonical
``id`` column.

The streaming/optimized SELECT aliases the identity expression to ``id``
(``<expr> AS id`` — default ``h.geoid``, or the COALESCE'd external_id when the
read policy flips). The result row therefore carries an ``id`` key, not a bare
``geoid`` key. The hub initialiser previously read ``row_dict.get("geoid")``,
which was absent → ``feature.id`` became ``None`` → the OGC items list rendered
self links ending in ``/items/None``.

These tests pin the hub reading ``id`` first with a ``geoid`` fallback, and
guard against the None-id regression.
"""

from unittest.mock import MagicMock

from dynastore.modules.catalog.item_service import ItemService
from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig


def _svc() -> ItemService:
    return ItemService(engine=MagicMock())


def test_hub_reads_id_from_aliased_id_column():
    # The streaming select emits `<expr> AS id`, so the row carries `id`.
    feature = _svc().map_row_to_feature({"id": "geoid-123"}, ItemsPostgresqlDriverConfig())
    assert feature.id == "geoid-123"


def test_hub_falls_back_to_geoid_key():
    # Raw/legacy rows that still carry a bare `geoid` column must keep working.
    feature = _svc().map_row_to_feature({"geoid": "geoid-456"}, ItemsPostgresqlDriverConfig())
    assert feature.id == "geoid-456"


def test_hub_never_yields_none_id_when_identity_present():
    feature = _svc().map_row_to_feature({"id": "geoid-789"}, ItemsPostgresqlDriverConfig())
    assert feature.id is not None
    assert feature.model_dump().get("id") == "geoid-789"
