#    Copyright 2025 FAO
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

import logging
import os
import datetime
from dateutil.relativedelta import relativedelta
from typing import Literal
from dynastore.modules.db_config.query_executor import DDLQuery, DQLQuery, DbResource, ResultHandler, DbEngine, DbConnection
from dynastore.modules.db_config.exceptions import QueryExecutionError

from dynastore.modules.db_config.partition_tools import ensure_partition_exists # type: ignore
from dynastore.modules.db_config.locking_tools import acquire_startup_lock, check_schema_exists, acquire_lock_if_needed, check_cron_job_exists, check_extension_exists

logger = logging.getLogger(__name__)

async def execute_ddl_block(conn: DbResource, ddl_block: str, **kwargs):
    """
    Safely executes a block of DDL statements by splitting them by semicolon.
    This is necessary because asyncpg/SQLAlchemy prepared statements (used when keywords are passed)
    only support a single command per execution.
    """
    # Simple split works for our current templates which don't have semicolons in strings.
    for statement in ddl_block.split(';'):
        if stmt := statement.strip():
            await DDLQuery(stmt).execute(conn, **kwargs)

async def ensure_db_extension(conn: DbResource, extension_name: str):
    """
    Ensures a database extension exists.
    """
    try:
        logger.info(f"Ensuring extension '{extension_name}' exists...")
        await DDLQuery(f'CREATE EXTENSION IF NOT EXISTS "{extension_name}" CASCADE;').execute(conn)
    except Exception as e:
        logger.error(f"Failed to ensure extension '{extension_name}': {e}")
        raise

async def ensure_schema_exists(conn: DbResource, schema_name: str):
    """
    Ensures a database schema exists using a concurrency-safe advisory lock.

    This function is critical for modules that require their own schema. It
    acquires a lock specific to the schema name, then creates the schema
    using `IF NOT EXISTS`. This prevents race conditions during application
    startup where multiple services might try to create the same schema
    simultaneously.
    """
    async def check_schema():
        try:
            # Wrap in nested transaction to revert state on failure
            from dynastore.modules.db_config.query_executor import managed_nested_transaction
            async with managed_nested_transaction(conn):
                return await check_schema_exists(conn, schema_name)
        except Exception:
            return False

    async with acquire_lock_if_needed(conn, f"schema_{schema_name}", check_schema) as active_conn:
        if active_conn is False:
            return

        try:
            logger.info(f"Ensuring schema '{schema_name}' exists ...")
            target_conn = active_conn if not isinstance(active_conn, bool) else conn
            # The DDL executor will gracefully handle the "already exists" case
            # if another process created it while this one was waiting for the lock.
            await DDLQuery(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}";').execute(target_conn)
            logger.info(f"Schema '{schema_name}' is ready.")
        except Exception as e:
            logger.error(f"Failed to create or verify schema '{schema_name}' (via {'Engine' if isinstance(conn, DbEngine) else 'Connection'}): {e}")
            # Re-raise the exception to halt startup if schema creation fails,
            # as it's a critical dependency.
            raise

async def ensure_enum_type(conn: DbResource, type_name: str, labels: list[str]):
    """
    Safely ensures a custom ENUM type exists, handling concurrent creation.

    This function uses an isolated transaction (SAVEPOINT) to attempt the
    `CREATE TYPE` command. If the type already exists (as is common when
    multiple services start up), the sub-transaction is rolled back, but the
    main transaction remains healthy, preventing the application from crashing.
    """
    label_str = ", ".join([f"'{label}'" for label in labels])
    create_type_sql = f"CREATE TYPE {type_name} AS ENUM ({label_str});"

    try:
        from dynastore.modules.db_config.tools import isolated_transaction
        # Use the isolated_transaction context manager to wrap the DDL.
        async with isolated_transaction(conn):
            logger.debug(f"Attempting to create ENUM type '{type_name}'...")
            await DDLQuery(create_type_sql).execute(conn)
            logger.info(f"Successfully created ENUM type '{type_name}'.")
    except QueryExecutionError as e:
        logger.error(f"Unexpected error creating ENUM '{type_name}': {e}")
        raise
    
async def ensure_future_partitions(
    conn: DbResource,
    schema: str,
    table: str,
    interval: Literal["daily", "monthly", "yearly"] = "monthly",
    periods_ahead: int = 1,
    column: str = "timestamp",
    strategy: Literal['RANGE'] = 'RANGE'
):
    """Ensures future time-based partitions exist for a table."""
    try:
        from dynastore.modules.catalog.catalog_config import PartitionIntervalEnum

        today = datetime.date.today()
        interval_map = {
            PartitionIntervalEnum.MONTHLY.value: relativedelta(months=1),
            PartitionIntervalEnum.YEARLY.value: relativedelta(years=1),
            PartitionIntervalEnum.WEEKLY.value: relativedelta(weeks=1),
            PartitionIntervalEnum.DAILY.value: relativedelta(days=1),
        }
        delta = interval_map.get(interval, relativedelta(months=1)) # Default to monthly

        for i in range(periods_ahead + 1):
            target_date = today + (delta * i)
            # Safe call, ensure_partition_exists should handle basic concurrency, 
            # but we wrap here too just in case.
            await ensure_partition_exists(
                conn,
                table_name=table,
                schema=schema,
                strategy=strategy,
                partition_value=target_date,
                interval=interval
            )
        logger.info(f"Ensured {interval} partitions for {schema}.{table} up to {periods_ahead} periods ahead.")
    
    except Exception as e:
        import traceback
        traceback.print_exc()
        logger.error(f"Failed to ensure future partitions for {schema}.{table}: {e}")
        # Explicit request to re-raise to crash testing if this fails
        raise e

async def register_retention_policy(
    conn: DbResource, 
    schema: str, 
    table: str, 
    policy: str = "prune", 
    interval: str = "daily", 
    retention_period: str = "12 months",
    column: str = "timestamp",
    schedule_cron: str = "0 3 * * 0"
):
    try:
        # Use quoted identifiers for function/job names to handle mixed-case schemas safely
        func_name = f"maintain_partitions_{schema}_{table}"
        job_name = f"{policy}_{schema}_{table}"
        
        # Define existence check for the lock wrapper
        async def check_policy_exists():
            return await check_cron_job_exists(conn, job_name)

        # Use the concurrency-safe pattern
        async with acquire_lock_if_needed(conn, f"retention_policy_{job_name}", check_policy_exists) as should_create:
            if should_create:
                try:
                    # 1. Ensure pg_cron extension (optimized internal check)
                    try:
                        await ensure_db_extension(conn, "pg_cron")
                    except Exception as ignored:
                        # If pg_cron cannot be installed (e.g. missing libraries), we gracefully disable pruning
                        import traceback
                        traceback.print_exc()
                        logger.warning(f"pg_cron extension invalid or missing. Auto-pruning for {schema}.{table} DISABLED. Error: {ignored}")
                        return

                    # IMPORTANT: We must quote identifiers because schema might be mixed-case (Base62)
                    create_func_sql = f"""
                    CREATE OR REPLACE FUNCTION "{schema}"."{func_name}"() RETURNS void AS $$
                    DECLARE
                        row RECORD;
                        cutoff_date DATE;
                    BEGIN
                        cutoff_date := date_trunc('{interval}', NOW()) - INTERVAL '{retention_period}';
                        -- nspname match in pg_namespace is a literal string comparison, so case must match exactly.
                        FOR row IN SELECT relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = '{schema}' AND c.relkind = 'r' AND c.relname ~ '^{table}_\\d{{4}}_\\d{{2}}$' LOOP
                            DECLARE
                                date_str TEXT;
                                part_date DATE;
                            BEGIN
                                date_str := substring(row.relname from '\\d{{4}}_\\d{{2}}$');
                                part_date := to_date(date_str, 'YYYY_MM');
                                IF part_date < cutoff_date THEN
                                    RAISE NOTICE 'Pruning old partition: %.%', '{schema}', row.relname;
                                    -- Quote the schema in the dynamic SQL
                                    EXECUTE format('DROP TABLE "{schema}".%I', row.relname);
                                END IF;
                            EXCEPTION WHEN OTHERS THEN
                                RAISE WARNING 'Failed to process partition %.%: %', '{schema}', row.relname, SQLERRM;
                            END;
                        END LOOP;
                    END;
                    $$ LANGUAGE plpgsql;
                    """
                    
                    safe_sql = create_func_sql.replace("{", "{{").replace("}", "}}")
                    await DDLQuery(safe_sql).execute(conn)
                    
                    # SAFE UNSCHEDULE:
                    # Instead of calling cron.unschedule directly from Python (which might fail and abort transaction),
                    # we execute a SQL block that checks existence first.
                    safe_unschedule_sql = f"""
                    DO $$
                    BEGIN
                        IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = '{job_name}') THEN
                            PERFORM cron.unschedule('{job_name}');
                        END IF;
                    END;
                    $$;
                    """
                    await DDLQuery(safe_unschedule_sql).execute(conn)
                    
                    # Now safe to schedule
                    # Quote the function call in the cron job command
                    schedule_sql = f"SELECT cron.schedule(:job_name, :schedule, 'SELECT \"{schema}\".\"{func_name}\"()')"
                    await DQLQuery(schedule_sql, result_handler=ResultHandler.SCALAR).execute(conn, job_name=job_name, schedule=schedule_cron)
                    
                    logger.info(f"Registered retention policy for {schema}.{table}")
                except Exception as e:
                    # Only swallow specific cron errors
                    if "function cron.schedule" in str(e) or "schema \"cron\" does not exist" in str(e):
                        logger.warning(f"pg_cron not available. Auto-pruning for {schema}.{table} DISABLED.")
                    else:
                        # Reraise everything else to the outer try/except
                        raise e
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise e
async def ensure_global_cron_cleanup(conn: DbResource, schedule_cron: str = "0 4 * * *"):
    """
    Registers a global maintenance job to clean up orphaned cron jobs.
    This job runs daily and removes cron jobs for schemas/tables that no longer exist.
    """
    func_name = "cleanup_orphaned_cron_jobs"
    job_name = "system_cleanup_orphaned_cron_jobs"
    
    # Define existence check for the lock wrapper
    async def check_cleanup_job_exists():
        return await check_cron_job_exists(conn, job_name)

    async with acquire_lock_if_needed(conn, f"global_cleanup_{job_name}", check_cleanup_job_exists) as should_create:
        if should_create:
            try:
                try:
                    await ensure_db_extension(conn, "pg_cron")
                except Exception as ignored:
                    logger.warning(f"pg_cron extension invalid or missing. Global cleanup DISABLED. Error: {ignored}")
                    return
                
                # Create the cleanup function
                # This function iterates over all cron jobs, attempts to parse the schema name
                # from the job name (assuming 'prune_[schema]_[table]' format), and
                # unschedules the job if the schema does not exist.
                create_func_sql = """
                CREATE OR REPLACE FUNCTION public.cleanup_orphaned_cron_jobs() RETURNS void AS $$
                DECLARE
                    job_rec RECORD;
                    parts TEXT[];
                    target_schema TEXT;
                    schema_exists BOOLEAN;
                BEGIN
                    FOR job_rec IN SELECT jobid, jobname FROM cron.job LOOP
                        -- Try to parse 'prune_[schema]_[table]'
                        -- We look for jobs starting with 'prune_'
                        IF job_rec.jobname LIKE 'prune_%' THEN
                            parts := string_to_array(job_rec.jobname, '_');
                            -- Assuming format: prune_schemaname_tablename...
                            -- Minimum parts: prune, schema, table (3 parts)
                            IF array_length(parts, 1) >= 3 THEN
                                target_schema := parts[2];
                                
                                -- Check if schema exists
                                SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = target_schema) INTO schema_exists;
                                
                                IF NOT schema_exists THEN
                                    RAISE NOTICE 'Removing orphaned cron job % (Schema % not found)', job_rec.jobname, target_schema;
                                    PERFORM cron.unschedule(job_rec.jobid);
                                END IF;
                            END IF;
                        END IF;
                    END LOOP;
                END;
                $$ LANGUAGE plpgsql;
                """
                
                await DDLQuery(create_func_sql).execute(conn)
                
                # Schedule the job safely
                safe_unschedule_sql = f"""
                DO $$
                BEGIN
                    IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = '{job_name}') THEN
                        PERFORM cron.unschedule('{job_name}');
                    END IF;
                END
                $$;
                """
                await DDLQuery(safe_unschedule_sql).execute(conn)
                
                schedule_sql = f"SELECT cron.schedule(:job_name, :schedule, 'SELECT public.{func_name}()')"
                await DQLQuery(schedule_sql, result_handler=ResultHandler.SCALAR).execute(conn, job_name=job_name, schedule=schedule_cron)
                
                logger.info("Registered global orphaned cron job cleanup task.")
                
            except Exception as e:
                if "function cron.schedule" in str(e) or "schema \"cron\" does not exist" in str(e):
                    logger.warning("pg_cron not available. Global cleanup disabled.")
                else:
                    logger.error(f"Failed to register global cleanup job: {e}")
                    # Don't raise, this is non-critical

async def register_cron_job(
    conn: DbResource,
    job_name: str,
    schedule: str,
    command: str
):
    """
    Registers a generic pg_cron job safely using the check-before-lock pattern.
    """
    async def check_exists():
        return await check_cron_job_exists(conn, job_name)

    async with acquire_lock_if_needed(conn, f"cron_job_{job_name}", check_exists) as should_create:
        if should_create:
            try:
                await ensure_db_extension(conn, "pg_cron")
                
                # Safe unschedule first
                unschedule_sql = f"""
                DO $$
                BEGIN
                    IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = '{job_name}') THEN
                        PERFORM cron.unschedule('{job_name}');
                    END IF;
                END
                $$;
                """
                await DDLQuery(unschedule_sql).execute(conn)
                
                # Schedule
                schedule_sql = "SELECT cron.schedule(:job_name, :schedule, :command)"
                await DQLQuery(schedule_sql, result_handler=ResultHandler.SCALAR).execute(
                    conn, 
                    job_name=job_name, 
                    schedule=schedule, 
                    command=command
                )
                logger.info(f"Registered cron job '{job_name}' with schedule '{schedule}'.")
            except Exception as e:
                if "function cron.schedule" in str(e) or "schema \"cron\" does not exist" in str(e):
                    logger.warning(f"pg_cron not available. Job '{job_name}' NOT registered.")
                else:
                    logger.error(f"Failed to register cron job '{job_name}': {e}")
                    raise
