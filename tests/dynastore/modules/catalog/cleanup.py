from tests.dynastore.test_utils.cleanup_registry import CleanupRegistry
from dynastore.modules.db_config.query_executor import DDLQuery, DQLQuery, ResultHandler, managed_nested_transaction
from dynastore.modules.db_config.locking_tools import check_table_exists, force_truncate_table, force_drop_schema
import logging

logger = logging.getLogger(__name__)

@CleanupRegistry.register
async def cleanup_catalog(engine):
    """
    Cleans up the catalog schema and drops tenant schemas.
    """
    # CRITICAL: In parallel execution (pytest-xidst), we skip global schema deletion 
    # to avoid workers destroying each other's environment.
    import os
    if os.environ.get("PYTEST_XDIST_WORKER"):
        logger.info("Parallel worker detected. Skipping catalog/tenant cleanup.")
        return

    async with managed_nested_transaction(engine) as conn:
        logger.info("Cleaning up catalog schema and dropping tenant schemas...")

    try:
        # Target schemas matching the generated physical name format: s_ + 8 base36 chars
        # (see generate_physical_name in catalog_service.py)
        query = DQLQuery(
            "SELECT nspname FROM pg_namespace "
            "WHERE nspname ~ '^s_[0-9a-z]{{8}}$';",
            result_handler=ResultHandler.ALL_SCALARS
        )
        # Use a fresh connection to get the list of schemas
        async with managed_nested_transaction(engine) as conn:
            schemas = await query.execute(conn)
        
        schemas: list[str] = list(schemas)
        if not schemas:
            logger.debug("No tenant schemas found to clean up.")
        else:
            logger.info(f"Found {len(schemas)} tenant schemas to drop. Terminating access and dropping in batches...")

        # Step 1: Force terminate backend access sequentially
        # We use a dedicated connection and the imported `force_drop_schema` 
        # or `terminate_backends_locking_schema` (by using force_drop_schema it's automatic).
        # We will terminate the locks first.
        from dynastore.modules.db_config.locking_tools import terminate_backends_locking_schema
        for schema in schemas:
            try:
                # Terminate backends without starting long transactions
                async with managed_nested_transaction(engine) as conn:
                    await terminate_backends_locking_schema(conn, schema)
            except Exception as e:
                logger.debug(f"Failed to terminate backends for {schema} (ignored): {e}")

        # Step 2: Drop schemas in batches to avoid exceeding max_locks_per_transaction
        BATCH_SIZE = 20
        failed: list[str] = []
        batch: list[str] = []
        for schema in schemas:
            batch.append(schema)
            if len(batch) >= BATCH_SIZE:
                drop_sql = "\n".join(f'DROP SCHEMA IF EXISTS "{s}" CASCADE;' for s in batch)
                try:
                    async with managed_nested_transaction(engine) as conn:
                        await DDLQuery(drop_sql).execute(conn)
                    logger.debug(f"Dropped batch of {len(batch)} schemas.")
                except Exception as e:
                    logger.warning(f"Batch drop failed ({e}), falling back to individual drops for this batch.")
                    failed.extend(batch)
                batch = []
        if batch:
            drop_sql = "\n".join(f'DROP SCHEMA IF EXISTS "{s}" CASCADE;' for s in batch)
            try:
                async with managed_nested_transaction(engine) as conn:
                    await DDLQuery(drop_sql).execute(conn)
                logger.debug(f"Dropped final batch of {len(batch)} schemas.")
            except Exception as e:
                logger.warning(f"Final batch drop failed ({e}), falling back to individual drops.")
                failed.extend(batch)

        # Fallback: drop individually any schemas that failed in batch
        for schema in failed:
            try:
                async with managed_nested_transaction(engine) as conn:
                    await force_drop_schema(conn, schema)
            except Exception as inner_e:
                logger.debug(f"Error dropping schema {schema}: {inner_e}")

        if schemas:
            logger.info(f"Finished dropping {len(schemas)} tenant schemas ({len(failed)} fell back to individual drops).")
        # Step 3: Truncate catalog metadata tables (always runs, even if no tenant schemas were found)
        metadata_tables = ["catalogs", "collections"]
        for table in metadata_tables:
            try:
                async with managed_nested_transaction(engine) as conn:
                    if await check_table_exists(conn, table, "catalog"):
                        logger.info(f"Truncating catalog.{table}...")
                        await force_truncate_table(conn, f"catalog.{table}")
            except Exception as e:
                logger.debug(f"Failed to truncate catalog.{table} (ignored): {e}")

    except Exception as e:
        logger.error(f"Error during tenant schema discovery: {e}")
