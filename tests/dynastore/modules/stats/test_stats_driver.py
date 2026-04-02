import pytest
import asyncio
from datetime import datetime, timezone
from dynastore.tools.protocol_helpers import get_engine
from dynastore.modules.stats.storage import PostgresStatsDriver, AccessRecord


@pytest.mark.asyncio
async def test_stats_driver_flush_optional_conn(task_app_state, db_reset_session):
    engine = get_engine(task_app_state)
    driver = PostgresStatsDriver(engine)
    # Enable flushing for this specific test
    driver._is_testing = False

    # Initialize Schema & Tables manually for this test
    from dynastore.modules.stats.storage import (
        CREATE_ACCESS_LOGS_TABLE,
        CREATE_AGGREGATES_TABLE,
    )
    from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler

    async with engine.begin() as conn:
        await CREATE_ACCESS_LOGS_TABLE.execute(conn, schema="catalog")
        await CREATE_AGGREGATES_TABLE.execute(conn, schema="catalog")

    from dynastore.tools.identifiers import generate_id_hex
    test_path = f"/test/stats/{generate_id_hex()[:8]}"

    # 1. Create a dummy record
    record = AccessRecord(
        timestamp=datetime.now(timezone.utc),
        catalog_id="_system_",
        method="GET",
        path=test_path,
        status_code=200,
        processing_time_ms=10.5,
        source_ip="127.0.0.1",
        details={"test": "true"},
    )

    # 2. Buffer it
    await driver.buffer_record(record)

    # 3. Flush WITHOUT conn (should resolve internal engine)
    await driver.flush()

    # 4. Verify it's in the DB
    # We need to manually check.
    from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler

    async with engine.begin() as conn:
        count = await DQLQuery(
            "SELECT COUNT(*) FROM catalog.access_logs WHERE path = :path",
            result_handler=ResultHandler.SCALAR,
        ).execute(conn, path=test_path)

    assert count == 1, "Record should have been flushed to DB"
