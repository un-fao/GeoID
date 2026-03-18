from tests.dynastore.test_utils.cleanup_registry import CleanupRegistry
from dynastore.modules.db_config.query_executor import DDLQuery, managed_nested_transaction
from dynastore.modules.db_config.locking_tools import check_table_exists
import logging

logger = logging.getLogger(__name__)

@CleanupRegistry.register
async def cleanup_apikey(engine):
    """
    Cleans up the apikey and users schemas.
    """
    # CRITICAL: In parallel execution (pytest-xdist), we skip truncation of shared tables
    # because one worker finishing a module would destroy the state for other active workers.
    import os
    if os.environ.get("PYTEST_XDIST_WORKER"):
        logger.info("Parallel worker detected. Skipping shared table truncation for apikey/users.")
        return

    async with managed_nested_transaction(engine) as conn:
        logger.info("Cleaning up apikey and users schemas...")
        
        # Tables to truncate in order of dependency
        tables = [
            ("apikey", "identity_policies"),
            ("apikey", "identity_roles"),
            ("apikey", "identity_authorization"),
            ("apikey", "refresh_tokens"),
            ("apikey", "api_keys"),
            ("apikey", "identity_links"),
            ("apikey", "principals"),
            ("apikey", "role_hierarchy"),
            ("apikey", "roles"),
            ("apikey", "policies"),
            ("users", "oauth_tokens"),
            ("users", "oauth_codes"),
            ("users", "users")
        ]
        
        for schema, table in tables:
            try:
                async with managed_nested_transaction(conn) as nested:
                    if await check_table_exists(nested, table, schema=schema):
                        await DDLQuery(f'DELETE FROM "{schema}"."{table}";').execute(nested)
                        logger.debug(f"Truncated table {schema}.{table}")
            except Exception as e:
                logger.debug(f"Could not truncate {schema}.{table}: {e}")

    # Also clean up partitions if they were created and are not the 'global' ones
    # For now, TRUNCATE CASCADE on the parent table should handle it.
