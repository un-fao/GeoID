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
from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DQLQuery,
    DbResource,
    ResultHandler,
    DbEngine,
    DbConnection,
)
from dynastore.modules.db_config.exceptions import QueryExecutionError

from dynastore.modules.db_config.partition_tools import ensure_partition_exists  # type: ignore
from dynastore.modules.db_config.locking_tools import (
    acquire_startup_lock,
    check_schema_exists,
    acquire_lock_if_needed,
    check_cron_job_exists,
    check_extension_exists,
)

logger = logging.getLogger(__name__)

# --- Legacy Compatibility (Deprecating) ---


async def execute_ddl_block(conn: DbResource, ddl_block: str, **kwargs):
    """
    Deprecated: Use DDLQuery(ddl_block).execute(conn, **params) instead.
    This wrapper delegates to DDLQuery which handles splitting and locking centrally.
    """
    import warnings

    warnings.warn(
        "execute_ddl_block is deprecated and will be removed in a future version. "
        "Use DDLQuery(ddl_block).execute(conn, **params) instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    await DDLQuery(ddl_block).execute(conn, **kwargs)


async def ensure_db_extension(conn: DbResource, extension_name: str):
    """
    Ensures a database extension exists.
    """
    try:
        logger.info(f"Ensuring extension '{extension_name}' exists...")
        await DDLQuery(
            f'CREATE EXTENSION IF NOT EXISTS "{extension_name}" CASCADE;'
        ).execute(conn)
    except Exception as e:
        logger.error(f"Failed to ensure extension '{extension_name}': {e}")
        raise


async def ensure_schema_exists(conn: DbResource, schema_name: str):
    """
    Ensures a database schema exists using centralized DDL coordination.
    """
    await DDLQuery(
        f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"',
        check_query=f"SELECT 1 FROM pg_namespace WHERE nspname = '{schema_name}'",
        lock_key=f"schema_{schema_name}",
    ).execute(conn)
    logger.info(f"Schema '{schema_name}' verified/ready.")


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
    strategy: Literal["RANGE"] = "RANGE",
):
    """Ensures future time-based partitions exist for a table."""
    try:
        today = datetime.date.today()
        interval_map = {
            "monthly": relativedelta(months=1),
            "yearly": relativedelta(years=1),
            "weekly": relativedelta(weeks=1),
            "daily": relativedelta(days=1),
        }
        delta = interval_map.get(
            interval, relativedelta(months=1)
        )  # Default to monthly

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
                interval=interval,
            )
        logger.info(
            f"Ensured {interval} partitions for {schema}.{table} up to {periods_ahead} periods ahead."
        )

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
    schedule_cron: str = "0 3 * * 0",
):
    try:
        # Use quoted identifiers for function/job names to handle mixed-case schemas safely
        func_name = f"maintain_partitions_{schema}_{table}"
        job_name = f"{policy}_{schema}_{table}"

        # Check if policy already exists before acquiring lock
        async def check_policy_exists():
            from .locking_tools import check_cron_job_exists

            return await check_cron_job_exists(conn, job_name)

        # Use the centralized DDLQuery system
        # IMPORTANT: We must quote identifiers because schema might be mixed-case (Base62)
        create_func_sql = f"""
        CREATE OR REPLACE FUNCTION "{schema}"."{func_name}"() RETURNS void AS $$
        DECLARE
            row RECORD;
            cutoff_date DATE;
        BEGIN
            cutoff_date := date_trunc('{interval}', NOW()) - INTERVAL '{retention_period}';
            -- nspname match in pg_namespace is a literal string comparison, so case must match exactly.
            FOR row IN SELECT relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = '{schema}' AND c.relkind = 'r' AND c.relname ~ '^{table}_\\d{{{{4}}}}_\\d{{{{2}}}}$' LOOP
                DECLARE
                    date_str TEXT;
                    part_date DATE;
                BEGIN
                    date_str := substring(row.relname from '\\d{{{{4}}}}_\\d{{{{2}}}}$');
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

        # We wrap the multi-statement block in a DDLQuery.
        # DDLExecutor will handle the splitting respecting the functional body ($$).
        policy_ddl = f"""
        {create_func_sql};
        
        -- SAFE UNSCHEDULE
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = '{job_name}') THEN
                PERFORM cron.unschedule('{job_name}');
            END IF;
        END;
        $$;
        
        -- Schedule via DQL (cron.schedule returns value)
        SELECT cron.schedule('{job_name}', '{schedule_cron}', $CMD$SELECT "{schema}"."{func_name}"()$CMD$);
        """

        await DDLQuery(
            policy_ddl,
            check_query=check_policy_exists,
            lock_key=f"retention_policy_{job_name}",
        ).execute(conn)

        logger.info(f"Registered retention policy for {schema}.{table}")
    except Exception as e:
        import traceback

        traceback.print_exc()
        raise e


async def ensure_global_cron_cleanup(
    conn: DbResource, schedule_cron: str = "0 4 * * *"
):
    """
    Registers a global maintenance job to clean up orphaned cron jobs.
    This job runs daily and removes cron jobs for schemas/tables that no longer exist.
    """
    func_name = "cleanup_orphaned_cron_jobs"
    job_name = "system_cleanup_orphaned_cron_jobs"

    # Define existence check for the lock wrapper
    async def check_cleanup_job_exists():
        return await check_cron_job_exists(conn, job_name)

    cleanup_ddl = f"""
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

    -- SAFE UNSCHEDULE
    DO $$
    BEGIN
        IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = '{job_name}') THEN
            PERFORM cron.unschedule('{job_name}');
        END IF;
    END;
    $$;

    SELECT cron.schedule('{job_name}', '{schedule_cron}', $CMD$SELECT public.{func_name}()$CMD$);
    """

    await DDLQuery(
        cleanup_ddl,
        check_query=check_cleanup_job_exists,
        lock_key=f"global_cleanup_{job_name}",
    ).execute(conn)
    logger.info("Registered global orphaned cron job cleanup task.")


async def register_cron_job(
    conn: DbResource, job_name: str, schedule: str, command: str
):
    """
    Registers a generic pg_cron job safely using the check-before-lock pattern.
    """
    command = command.strip().rstrip(";")

    async def check_exists():
        from .locking_tools import check_cron_job_exists

        return await check_cron_job_exists(conn, job_name)

    cron_ddl = f"""
    -- SAFE UNSCHEDULE
    DO $$
    BEGIN
        IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = '{job_name}') THEN
            PERFORM cron.unschedule('{job_name}');
        END IF;
    END;
    $$;

    SELECT cron.schedule('{job_name}', '{schedule}', $CMD${command}$CMD$);
    """

    await DDLQuery(
        cron_ddl, check_query=check_exists, lock_key=f"cron_job_{job_name}"
    ).execute(conn)
