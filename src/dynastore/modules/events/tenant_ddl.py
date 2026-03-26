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

# ==============================================================================
#  TENANT INITIALIZATION (Events Slice)
#
#  Design: Two flat tables per tenant schema (no partitions).
#  - catalog_events:    for catalog-scoped events
#  - collection_events: for collection-scoped events
#
#  Tables:
#   - catalog_events:    flat table for catalog-scoped events.
#   - collection_events: LIST partitioned table by collection_id.
#
#  Maintenance (pg_cron):
#   - Weekly: Archive stale PENDING events (> 30 days) → dead letter.
#   - Daily:  Delete CONSUMED events older than EVENT_RETENTION_DAYS (default 7).
#   - Monthly: Purge dead letter entries older than 1 year.
# ==============================================================================

import os

EVENT_RETENTION_DAYS = int(os.getenv("EVENT_RETENTION_DAYS", "7"))

# ----- Catalog-level events (flat, no partitions) -----
TENANT_CATALOG_EVENTS_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.catalog_events (
    id          BIGSERIAL PRIMARY KEY,
    event_type  VARCHAR      NOT NULL,
    catalog_id  VARCHAR,
    payload     JSONB,
    created_at  TIMESTAMPTZ  DEFAULT NOW(),
    status      VARCHAR      DEFAULT 'PENDING'
);
CREATE INDEX IF NOT EXISTS idx_catalog_events_type ON {schema}.catalog_events(event_type);
CREATE INDEX IF NOT EXISTS idx_catalog_events_cat_id ON {schema}.catalog_events(catalog_id);
"""

TENANT_CATALOG_EVENTS_DL_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.catalog_events_dead_letter (
    id                  BIGINT       NOT NULL,
    event_type          VARCHAR      NOT NULL,
    catalog_id          VARCHAR,
    payload             JSONB,
    created_at          TIMESTAMPTZ  NOT NULL,
    archived_at         TIMESTAMPTZ  DEFAULT NOW(),
    original_status     VARCHAR
);
CREATE INDEX IF NOT EXISTS idx_catalog_events_dl_type ON {schema}.catalog_events_dead_letter(event_type);
CREATE INDEX IF NOT EXISTS idx_catalog_events_dl_cat_id ON {schema}.catalog_events_dead_letter(catalog_id);
"""

# ----- Collection-level events (partitioned by collection_id via LIST) -----
# Each collection gets its own partition created at collection-creation time.
# A DEFAULT partition captures events before the per-collection partition exists.
# The dead letter stays flat and is rotated by pg_cron monthly.
TENANT_COLLECTION_EVENTS_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.collection_events (
    id              BIGSERIAL       NOT NULL,
    event_type      VARCHAR         NOT NULL,
    catalog_id      VARCHAR,
    collection_id   VARCHAR         NOT NULL,
    item_id         VARCHAR,
    payload         JSONB,
    created_at      TIMESTAMPTZ     DEFAULT NOW(),
    status          VARCHAR         DEFAULT 'PENDING',
    PRIMARY KEY (collection_id, id)
) PARTITION BY LIST (collection_id);
CREATE INDEX IF NOT EXISTS idx_collection_events_type ON {schema}.collection_events(event_type);
CREATE INDEX IF NOT EXISTS idx_collection_events_cat_id ON {schema}.collection_events(catalog_id);
CREATE INDEX IF NOT EXISTS idx_collection_events_col_id ON {schema}.collection_events(collection_id);
CREATE INDEX IF NOT EXISTS idx_collection_events_item_id ON {schema}.collection_events(item_id);
"""

TENANT_COLLECTION_EVENTS_DEFAULT_PARTITION_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.collection_events_default
    PARTITION OF {schema}.collection_events DEFAULT;
"""

TENANT_COLLECTION_EVENTS_DL_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.collection_events_dead_letter (
    id                  BIGINT       NOT NULL,
    event_type          VARCHAR      NOT NULL,
    catalog_id          VARCHAR,
    collection_id       VARCHAR,
    item_id             VARCHAR,
    payload             JSONB,
    created_at          TIMESTAMPTZ  NOT NULL,
    archived_at         TIMESTAMPTZ  DEFAULT NOW(),
    original_status     VARCHAR
);
CREATE INDEX IF NOT EXISTS idx_collection_events_dl_type ON {schema}.collection_events_dead_letter(event_type);
CREATE INDEX IF NOT EXISTS idx_collection_events_dl_cat_id ON {schema}.collection_events_dead_letter(catalog_id);
CREATE INDEX IF NOT EXISTS idx_collection_events_dl_col_id ON {schema}.collection_events_dead_letter(collection_id);
CREATE INDEX IF NOT EXISTS idx_collection_events_dl_item_id ON {schema}.collection_events_dead_letter(item_id);
"""


def build_tenant_notify_ddl(schema: str) -> tuple[str, str, str]:
    """Returns (func_name, notify_func_ddl, triggers_ddl) for catalog and collection tables.

    NOTE: Statement-level triggers are no longer required for collection_events (partitioned)
    since we are on PostgreSQL 16+. Row-level triggers on the parent table fire
    individually for each partition insertion.
    """
    func_name = f"notify_tenant_event_{schema}"
    notify_func = f"""
    CREATE OR REPLACE FUNCTION "{schema}"."{func_name}"() RETURNS TRIGGER AS $$
    BEGIN
        PERFORM pg_notify('dynastore_events_channel', TG_TABLE_SCHEMA);
        RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;
    """
    triggers = f"""
    CREATE TRIGGER trg_catalog_events_insert
    AFTER INSERT ON "{schema}".catalog_events
    FOR EACH ROW EXECUTE FUNCTION "{schema}"."{func_name}"();

    CREATE TRIGGER trg_collection_events_insert
    AFTER INSERT ON "{schema}".collection_events
    FOR EACH ROW EXECUTE FUNCTION "{schema}"."{func_name}"();
    """
    return func_name, notify_func, triggers


def build_tenant_cron_ddl(schema: str) -> list[tuple[str, str, str]]:
    """
    Returns a list of (job_name, schedule, sql_command) tuples for tenant cron jobs.

    Jobs registered:
      - archive_catalog_events_{schema}:    move PENDING > 30 days → dead_letter
      - archive_collection_events_{schema}: move PENDING > 30 days → dead_letter
      - cleanup_consumed_catalog_events_{schema}: delete CONSUMED > EVENT_RETENTION_DAYS
      - cleanup_consumed_collection_events_{schema}: delete CONSUMED > EVENT_RETENTION_DAYS
      - cleanup_catalog_events_dl_{schema}: delete dead_letter > 1 year
      - cleanup_collection_events_dl_{schema}: delete dead_letter > 1 year
    """
    s = schema
    return [
        (
            f"archive_catalog_events_{s}",
            "0 2 * * 0",  # Sundays 02:00
            (
                f'INSERT INTO "{s}".catalog_events_dead_letter '
                f"(id, event_type, catalog_id, payload, created_at, original_status) "
                f"SELECT id, event_type, catalog_id, payload, created_at, status "
                f'FROM "{s}".catalog_events '
                f"WHERE status = 'PENDING' AND created_at < NOW() - INTERVAL '30 days'; "
                f'DELETE FROM "{s}".catalog_events '
                f"WHERE status = 'PENDING' AND created_at < NOW() - INTERVAL '30 days';"
            ),
        ),
        (
            f"archive_collection_events_{s}",
            "0 2 * * 0",  # Sundays 02:00
            (
                f'INSERT INTO "{s}".collection_events_dead_letter '
                f"(id, event_type, catalog_id, collection_id, item_id, payload, created_at, original_status) "
                f"SELECT id, event_type, catalog_id, collection_id, item_id, payload, created_at, status "
                f'FROM "{s}".collection_events '
                f"WHERE status = 'PENDING' AND created_at < NOW() - INTERVAL '30 days'; "
                f'DELETE FROM "{s}".collection_events '
                f"WHERE status = 'PENDING' AND created_at < NOW() - INTERVAL '30 days';"
            ),
        ),
        (
            f"cleanup_consumed_catalog_events_{s}",
            "0 4 * * *",  # Daily 04:00
            (
                f'DELETE FROM "{s}".catalog_events '
                f"WHERE status = 'CONSUMED' AND created_at < NOW() - INTERVAL '{EVENT_RETENTION_DAYS} days';"
            ),
        ),
        (
            f"cleanup_consumed_collection_events_{s}",
            "0 4 * * *",  # Daily 04:00
            (
                f'DELETE FROM "{s}".collection_events '
                f"WHERE status = 'CONSUMED' AND created_at < NOW() - INTERVAL '{EVENT_RETENTION_DAYS} days';"
            ),
        ),
        (
            f"cleanup_catalog_events_dl_{s}",
            "0 3 1 * *",  # 1st of month 03:00
            f"DELETE FROM \"{s}\".catalog_events_dead_letter WHERE archived_at < NOW() - INTERVAL '1 year';",
        ),
        (
            f"cleanup_collection_events_dl_{s}",
            "0 3 1 * *",  # 1st of month 03:00
            f"DELETE FROM \"{s}\".collection_events_dead_letter WHERE archived_at < NOW() - INTERVAL '1 year';",
        ),
    ]
