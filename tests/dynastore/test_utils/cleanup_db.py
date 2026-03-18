import asyncio
import os
import sys
import logging
from sqlalchemy.ext.asyncio import create_async_engine
from dynastore.modules.db_config.query_executor import managed_transaction, DDLQuery

# Add project root and src to path if running directly
root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
if root not in sys.path:
    sys.path.append(root)
sys.path.append(os.path.join(root, "src"))

from tests.dynastore.test_utils.cleanup_registry import CleanupRegistry

# Import modules from tests/ to register their cleanup handlers
try:
    import tests.dynastore.extensions.auth.cleanup
    import tests.dynastore.modules.apikey.cleanup
    import tests.dynastore.modules.catalog.cleanup
except ImportError:
    pass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://testuser:testpassword@localhost:54320/gis_dev")
if "asyncpg" not in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")

async def cleanup_db():
    engine = create_async_engine(DATABASE_URL)

    logger.info("Starting database cleanup...")

    # Run cleanup without holding a global lock that blocks other workers.
    # Each handler uses its own nested transactions with IF EXISTS / CASCADE logic.
    try:
        await CleanupRegistry.run_all(engine)
    except Exception as e:
        logger.warning(f"Cleanup failed (non-critical): {e}")

    logger.info("Database cleanup complete.")
    await engine.dispose()

if __name__ == "__main__":
    asyncio.run(cleanup_db())
