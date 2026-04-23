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

"""STAC-slice metadata DDL — relocated from ``modules/catalog/db_init/metadata_domain_split.py``.

When the STAC module is installed (and its initializer wired into the
catalog provisioning lifecycle in PR 1b), the per-tenant initializer here
runs alongside the core-tables initializer to create the STAC sidecar
tables. Without the STAC module, the STAC tables are never created —
operators get a "STAC fields not part of this collection's schema"
loud failure rather than silent drops.

Currently dormant: the canonical DDL still lives in ``metadata_domain_split.py``
and runs unconditionally from ``CatalogService.create_catalog``. PR 1b
flips the wiring: the core file drops the STAC DDL, and ``StacModule``
hooks ``ensure_stac_metadata_tables`` into the catalog provisioning path.
"""

from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DbResource,
)


CATALOG_METADATA_STAC_DDL = """
CREATE TABLE IF NOT EXISTS catalog.catalog_metadata_stac (
    catalog_id      VARCHAR PRIMARY KEY REFERENCES catalog.catalogs(id) ON DELETE CASCADE,
    stac_version    VARCHAR(20),
    stac_extensions JSONB,
    conforms_to     JSONB,
    links           JSONB,
    assets          JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


TENANT_METADATA_STAC_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.collection_metadata_stac (
    collection_id   VARCHAR PRIMARY KEY,
    stac_version    VARCHAR(20),
    stac_extensions JSONB,
    extent          JSONB,
    providers       JSONB,
    summaries       JSONB,
    links           JSONB,
    assets          JSONB,
    item_assets     JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


async def ensure_global_stac_metadata_tables(conn: DbResource) -> None:
    """Apply the global STAC DDL (``catalog.catalog_metadata_stac``).

    Idempotent. PR 1b wires this into ``StacModule.lifespan`` so it runs
    once on module init alongside ``ensure_global_metadata_core_tables``.
    """
    await DDLQuery(CATALOG_METADATA_STAC_DDL).execute(conn)


async def ensure_tenant_stac_metadata_tables(conn: DbResource, schema: str) -> None:
    """Apply the per-tenant STAC DDL (``{schema}.collection_metadata_stac``).

    Idempotent. PR 1b registers this with the catalog-creation lifecycle
    so it runs alongside ``ensure_tenant_metadata_core_tables`` when a
    new tenant schema is created — only if the STAC module is installed.
    """
    await DDLQuery(TENANT_METADATA_STAC_DDL).execute(conn, schema=schema)
