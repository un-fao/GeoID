import asyncio
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.pool import StaticPool
from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    DDLQuery,
    managed_transaction,
    ResultHandler,
)


@pytest_asyncio.fixture
async def shared_engine(db_url):
    """
    Creates an engine with a StaticPool to simulate a shared physical connection
    across all sessions and tasks. This is the hardest case for concurrency.
    """
    url = db_url.replace("postgresql://", "postgresql+asyncpg://")
    engine = create_async_engine(
        url,
        poolclass=StaticPool,
        connect_args={"server_settings": {"application_name": "concurrency_test"}},
        pool_reset_on_return="rollback",  # Ensure clean state for asyncpg
    )
    yield engine
    await engine.dispose()


@pytest.mark.asyncio
async def test_reentrancy_same_task(shared_engine):
    """Verifies that a single task can acquire the lock multiple times (reentrancy)."""
    async with managed_transaction(shared_engine) as conn:
        # First query
        res1 = await DQLQuery("SELECT 1", result_handler=ResultHandler.SCALAR).execute(
            conn
        )
        assert res1 == 1

        # Nested transaction (should be reentrant)
        async with managed_transaction(conn) as nested_conn:
            res2 = await DQLQuery(
                "SELECT 2", result_handler=ResultHandler.SCALAR
            ).execute(nested_conn)
            assert res2 == 2

            # Deeply nested manual lock
            res3 = await DQLQuery(
                "SELECT 3", result_handler=ResultHandler.SCALAR
            ).execute(nested_conn)
            assert res3 == 3


@pytest.mark.asyncio
async def test_concurrent_tasks_shared_engine(shared_engine):
    """
    Verifies that multiple concurrent tasks using the same Engine (shared StaticPool connection)
    are properly serialized and do not cause 'another operation in progress' errors.
    """

    async def run_query(task_id, delay=0.1):
        # We use a query that takes some time to execute
        sql = f"SELECT pg_sleep({delay}), {task_id}"
        async with managed_transaction(shared_engine) as conn:
            res = await DQLQuery(sql, result_handler=ResultHandler.ONE).execute(conn)
            return res[1]

    # Spawn 5 concurrent tasks
    tasks = [run_query(i) for i in range(5)]
    results = await asyncio.gather(*tasks)

    assert len(results) == 5
    assert set(results) == {0, 1, 2, 3, 4}


@pytest.mark.asyncio
async def test_absolute_root_convergence(shared_engine):
    """
    Verifies that different wrappers for the same physical connection converge on the same lock.
    """
    async with shared_engine.connect() as conn1:
        # Get raw connection to verify resolution
        # Create a session from the same connection
        session = AsyncSession(bind=conn1)

        # Attempting to lock the session should be REENTRANT because it's the same task
        # and we've already locked conn1. If convergence works, this won't block.
        async with asyncio.timeout(2):  # 2 second timeout to detect deadlock
            res = await DQLQuery(
                "SELECT 'convergence'", result_handler=ResultHandler.SCALAR
            ).execute(session)
            assert res == "convergence"


@pytest.mark.asyncio
async def test_concurrent_ddl_and_dml(shared_engine):
    """
    Verifies that DDL (which takes locks) and DML can run concurrently
    without causing asyncpg InterfaceErrors.
    """
    await DDLQuery(
        "CREATE TABLE IF NOT EXISTS test_concurrency (id serial primary key, val text)"
    ).execute(shared_engine)

    async def do_ddl():
        # DDL that might be slow or contention-heavy
        await DDLQuery("COMMENT ON TABLE test_concurrency IS 'testing'").execute(
            shared_engine
        )
        return "ddl_ok"

    async def do_dml(val):
        await DQLQuery(
            "INSERT INTO test_concurrency (val) VALUES (:val)",
            result_handler=ResultHandler.NONE,
        ).execute(shared_engine, val=val)
        return "dml_ok"

    # Run many mixed operations
    ops = []
    for i in range(10):
        ops.append(do_ddl())
        ops.append(do_dml(f"val_{i}"))

    results = await asyncio.gather(*ops)
    assert len(results) == 20
    assert "ddl_ok" in results
    assert "dml_ok" in results

    # Cleanup
    await DDLQuery("DROP TABLE test_concurrency").execute(shared_engine)


@pytest.mark.asyncio
async def test_managed_transaction_serialization(shared_engine):
    """
    Explicitly test that managed_transaction prevents overlapping 'begin()' calls
    on a shared engine with StaticPool.
    """

    async def concurrent_begin(task_id):
        # managed_transaction should serialize the start of the transaction
        async with managed_transaction(shared_engine) as conn:
            # We are now in a transaction
            await asyncio.sleep(0.05)
            await DQLQuery("SELECT 1", result_handler=ResultHandler.SCALAR).execute(
                conn
            )
        return task_id

    # If serialization fails, one of these will hit InterfaceError during BEGIN
    tasks = [concurrent_begin(i) for i in range(10)]
    results = await asyncio.gather(*tasks)
    assert len(results) == 10
