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

import pytest
from dynastore.modules.catalog.log_manager import LogsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.modules.db_config.query_executor import managed_transaction, DQLQuery, ResultHandler
from dynastore.tools.protocol_helpers import get_engine

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.enable_modules("db_config", "db", "catalog", "stats"),
]


async def test_system_logs_creation(app_lifespan_module):
    """Verifies that the system_logs table is created and can be queried."""
    engine = get_engine(app_lifespan_module)
    assert engine is not None

    async with managed_transaction(engine) as conn:
        res = await DQLQuery(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'catalog' AND table_name = 'system_logs')",
            result_handler=ResultHandler.SCALAR
        ).execute(conn)
        assert res is True

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

    await logs_service.flush()

    engine = get_engine(app_lifespan_module)
    async with managed_transaction(engine) as conn:
        logs = await DQLQuery(
            "SELECT message FROM catalog.system_logs WHERE event_type = 'test_event' ORDER BY timestamp DESC LIMIT 1",
            result_handler=ResultHandler.ALL_SCALARS
        ).execute(conn)
        assert len(logs) > 0
        assert logs[0] == "Verification test message"
