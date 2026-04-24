import pytest
from dynastore.modules.catalog.log_manager import LogsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.modules.db_config.query_executor import managed_transaction, DQLQuery, ResultHandler
from dynastore.tools.protocol_helpers import get_engine


@pytest.mark.asyncio
@pytest.mark.enable_modules("db_config", "db", "catalog", "stats")
async def test_system_logs_creation(app_lifespan_module):
    """Verifies that the system_logs table is created and can be queried."""
    # app_lifespan_module yields app.state
    engine = get_engine(app_lifespan_module)
    assert engine is not None
    
    async with managed_transaction(engine) as conn:
        # Check if table exists
        res = await DQLQuery(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'catalog' AND table_name = 'system_logs')",
            result_handler=ResultHandler.SCALAR
        ).execute(conn)
        assert res is True

@pytest.mark.asyncio
@pytest.mark.enable_modules("db_config", "db", "catalog", "stats")
async def test_log_event_system(app_lifespan_module):
    """Verifies that logging to _system_ works correctly."""
    logs_service = get_protocol(LogsProtocol)
    assert logs_service is not None
    
    await logs_service.log_event(
        catalog_id="_system_",
        event_type="test_event",
        level="INFO",
        message="Verification test message",
        immediate=True
    )
    
    # Flush to be sure
    await logs_service.flush()
    
    engine = get_engine(app_lifespan_module)
    async with managed_transaction(engine) as conn:
        logs = await DQLQuery(
            "SELECT message FROM catalog.system_logs WHERE event_type = 'test_event' ORDER BY timestamp DESC LIMIT 1",
            result_handler=ResultHandler.ALL_SCALARS
        ).execute(conn)
        assert len(logs) > 0
        assert logs[0] == "Verification test message"
