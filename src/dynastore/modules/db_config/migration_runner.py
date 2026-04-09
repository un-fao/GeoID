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

"""
db_config/migration_runner.py

Versioned database migration engine for DynaStore.

## Design Principles

### Admin-only migration trigger
Migrations are NEVER auto-applied at startup.  Startup performs a status check
only (``check_migration_status``), logging warnings if pending migrations exist.
All migrations must be explicitly triggered via the admin API or task system
by calling ``run_migrations``.

### Two migration scopes

1. **Global migrations** — applied once to the shared database (extensions,
   global tables like ``platform.schema_migrations``, ``platform.app_state``).
   Registered via ``register_module_migrations()``.

2. **Tenant migrations** — SQL scripts containing ``{schema}`` placeholders,
   applied to every existing tenant schema (one per catalog).
   Registered via ``register_tenant_migrations()``.
   Tracked per-tenant in ``{schema}.schema_migrations``.

### Speed-first startup (Cloud Run)
On a normal restart the startup path costs a single lightweight DB query.
No file I/O, no advisory lock, no migration execution.

### Multiple applications, one database
* ``platform.app_state`` — one row per app instance (keyed by ``app_key``).
* ``platform.schema_migrations`` — authoritative global history.
* ``{schema}.schema_migrations`` — authoritative per-tenant history.

### Cross-app notifications via pg_notify
After applying migrations, broadcasts on ``dynastore.migrations`` channel.

### Tamper detection
SHA-256 checksums verified for every already-applied script.

### Rollback support
For each ``v####__description.sql``, an optional ``v####__description__rollback.sql``
can exist.  Rollback SQL is stored in ``schema_migrations.rollback_sql`` on apply.
Rollback can be triggered via admin API.

### Dry-run mode
``run_migrations(dry_run=True)`` returns the list of pending scripts without
executing them, for admin review before committing.

## Usage

Global migrations (in module ``__init__.py``)::

    from dynastore.modules.db_config.migration_runner import register_module_migrations
    register_module_migrations("catalog", "dynastore.modules.catalog.migrations")

Tenant migrations (in module ``__init__.py``)::

    from dynastore.modules.db_config.migration_runner import register_tenant_migrations
    register_tenant_migrations("catalog", "dynastore.modules.catalog.tenant_migrations")

## Adding a new migration

1. Create ``migrations/v####__short_description.sql`` in your module.
2. Write **idempotent**, **additive** SQL.
3. Never edit a script already applied to production — checksum guard blocks startup.
4. Optionally create ``v####__short_description__rollback.sql`` with reversal DDL.
"""

import hashlib
import importlib.resources
import json
import logging
import os
import re
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Literal, Optional, Tuple

from sqlalchemy.ext.asyncio import AsyncEngine

from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DQLQuery,
    DbResource,
    ResultHandler,
    managed_transaction,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class MigrationError(RuntimeError):
    """Raised when a migration script fails."""


class MigrationChecksumError(MigrationError):
    """Raised when a previously-applied SQL file has been modified on disk."""


# ---------------------------------------------------------------------------
# Status enum
# ---------------------------------------------------------------------------


class MigrationStatus(str, Enum):
    """Result of a migration status check."""

    UP_TO_DATE = "up_to_date"
    PENDING_MIGRATIONS = "pending_migrations"
    DRIFT_DETECTED = "drift_detected"
    UNCHECKED = "unchecked"


# ---------------------------------------------------------------------------
# Global migration registry (populated at import time — zero db cost)
# ---------------------------------------------------------------------------

#: module_name → python package containing v####__*.sql files
_MIGRATION_SOURCES: Dict[str, str] = {}
#: Preserve registration order
_SOURCE_ORDER: List[str] = []


def register_module_migrations(module_name: str, package_path: str) -> None:
    """
    Register a module's global migrations package.

    Args:
        module_name:  Stable unique name used as primary-key in schema_migrations.
        package_path: Fully-qualified Python package containing v####__*.sql files.
    """
    if module_name in _MIGRATION_SOURCES:
        logger.debug(f"MigrationRunner: '{module_name}' already registered.")
        return
    _MIGRATION_SOURCES[module_name] = package_path
    _SOURCE_ORDER.append(module_name)
    logger.debug(f"MigrationRunner: Registered '{module_name}' → '{package_path}'.")


# ---------------------------------------------------------------------------
# Tenant migration registry
# ---------------------------------------------------------------------------

#: module_name → python package containing v####__*.sql files with {schema} placeholders
_TENANT_MIGRATION_SOURCES: Dict[str, str] = {}
_TENANT_SOURCE_ORDER: List[str] = []


def register_tenant_migrations(module_name: str, package_path: str) -> None:
    """
    Register a module's tenant migrations package.

    Tenant migration SQL scripts contain ``{schema}`` placeholders that will be
    substituted with each tenant's physical schema name.

    Args:
        module_name:  Stable unique name (e.g. "catalog_tenant").
        package_path: Fully-qualified Python package containing v####__*.sql files.
    """
    if module_name in _TENANT_MIGRATION_SOURCES:
        logger.debug(f"MigrationRunner: tenant '{module_name}' already registered.")
        return
    _TENANT_MIGRATION_SOURCES[module_name] = package_path
    _TENANT_SOURCE_ORDER.append(module_name)
    logger.debug(
        f"MigrationRunner: Registered tenant '{module_name}' → '{package_path}'."
    )


# ---------------------------------------------------------------------------
# Manifest hash computation (in-process, zero I/O)
# ---------------------------------------------------------------------------

_expected_manifest_cache: Optional[Dict[str, str]] = None
_expected_tenant_manifest_cache: Optional[Dict[str, str]] = None


def _clear_manifest_cache() -> None:
    """Test helper: clear all cached manifests."""
    global _expected_manifest_cache, _expected_tenant_manifest_cache
    _expected_manifest_cache = None
    _expected_tenant_manifest_cache = None


def _compute_manifest_hash(manifest: Dict[str, str]) -> str:
    """Stable, order-independent hash of a module→version manifest dict."""
    stable = json.dumps(dict(sorted(manifest.items())), separators=(",", ":"))
    return hashlib.sha256(stable.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Script discovery
# ---------------------------------------------------------------------------

_VERSION_RE = re.compile(r"^(v\d{4})__(.+?)\.sql$", re.IGNORECASE)
_ROLLBACK_RE = re.compile(r"^(v\d{4})__(.+?)__rollback\.sql$", re.IGNORECASE)


def _sha256(content: str) -> str:
    return hashlib.sha256(content.encode()).hexdigest()


@dataclass
class _MigrationScript:
    module: str
    version: str
    description: str
    sql: str
    checksum: str
    rollback_sql: Optional[str] = None
    is_tenant: bool = False


def _discover_scripts_for_module(
    module_name: str, package_path: str, is_tenant: bool = False
) -> List[_MigrationScript]:
    """Discover forward and rollback SQL scripts from a Python package."""
    forward_scripts: Dict[str, _MigrationScript] = {}
    rollback_sqls: Dict[str, str] = {}

    try:
        pkg = importlib.resources.files(package_path)
        for resource in pkg.iterdir():
            # Check for rollback scripts first (more specific pattern)
            rm = _ROLLBACK_RE.match(resource.name)
            if rm:
                ver = rm.group(1).lower()
                rollback_sqls[ver] = resource.read_text(encoding="utf-8")
                continue

            # Check for forward scripts
            m = _VERSION_RE.match(resource.name)
            if not m:
                continue

            # Skip rollback files that passed the forward regex
            if "__rollback" in resource.name.lower():
                continue

            sql = resource.read_text(encoding="utf-8")
            ver = m.group(1).lower()
            forward_scripts[ver] = _MigrationScript(
                module=module_name,
                version=ver,
                description=m.group(2).replace("_", " "),
                sql=sql,
                checksum=_sha256(sql),
                is_tenant=is_tenant,
            )
    except Exception as e:
        logger.warning(
            f"MigrationRunner: Cannot load '{module_name}' from '{package_path}': {e}"
        )

    # Attach rollback SQL to forward scripts
    for ver, rollback in rollback_sqls.items():
        if ver in forward_scripts:
            forward_scripts[ver].rollback_sql = rollback

    result = list(forward_scripts.values())
    result.sort(key=lambda s: s.version)
    return result


def _discover_all_global_scripts() -> List[_MigrationScript]:
    all_scripts: List[_MigrationScript] = []
    for module_name in _SOURCE_ORDER:
        all_scripts.extend(
            _discover_scripts_for_module(
                module_name, _MIGRATION_SOURCES[module_name], is_tenant=False
            )
        )
    return all_scripts


def _discover_all_tenant_scripts() -> List[_MigrationScript]:
    all_scripts: List[_MigrationScript] = []
    for module_name in _TENANT_SOURCE_ORDER:
        all_scripts.extend(
            _discover_scripts_for_module(
                module_name,
                _TENANT_MIGRATION_SOURCES[module_name],
                is_tenant=True,
            )
        )
    return all_scripts


def _build_expected_manifest(scripts: List[_MigrationScript]) -> Dict[str, str]:
    manifest: Dict[str, str] = {}
    for s in scripts:
        if s.module not in manifest or s.version > manifest[s.module]:
            manifest[s.module] = s.version
    return manifest


# ---------------------------------------------------------------------------
# Fast-path: peek max versions from filenames only (no SQL reads)
# ---------------------------------------------------------------------------


def _peek_max_version_for_module(package_path: str) -> Optional[str]:
    """Return the max version string found in a package by listing filenames only."""
    try:
        pkg = importlib.resources.files(package_path)
        max_ver: Optional[str] = None
        for resource in pkg.iterdir():
            m = _VERSION_RE.match(resource.name)
            if not m or "__rollback" in resource.name.lower():
                continue
            ver = m.group(1).lower()
            if max_ver is None or ver > max_ver:
                max_ver = ver
        return max_ver
    except Exception:
        return None


def _get_expected_manifest() -> Dict[str, str]:
    """Return expected global manifest without reading SQL file contents."""
    global _expected_manifest_cache
    if _expected_manifest_cache is not None:
        return _expected_manifest_cache

    manifest: Dict[str, str] = {}
    for module_name in _SOURCE_ORDER:
        ver = _peek_max_version_for_module(_MIGRATION_SOURCES[module_name])
        if ver:
            manifest[module_name] = ver
    _expected_manifest_cache = manifest
    return manifest


def _get_expected_tenant_manifest() -> Dict[str, str]:
    """Return expected tenant manifest without reading SQL file contents."""
    global _expected_tenant_manifest_cache
    if _expected_tenant_manifest_cache is not None:
        return _expected_tenant_manifest_cache

    manifest: Dict[str, str] = {}
    for module_name in _TENANT_SOURCE_ORDER:
        ver = _peek_max_version_for_module(_TENANT_MIGRATION_SOURCES[module_name])
        if ver:
            manifest[module_name] = ver
    _expected_tenant_manifest_cache = manifest
    return manifest


# ---------------------------------------------------------------------------
# DDL for tracking tables
# ---------------------------------------------------------------------------

# All global tracking tables live in the "platform" schema (never "public").
PLATFORM_SCHEMA = "platform"

_PLATFORM_SCHEMA_DDL = 'CREATE SCHEMA IF NOT EXISTS "platform";'

_SCHEMA_MIGRATIONS_DDL = """
CREATE TABLE IF NOT EXISTS platform.schema_migrations (
    module       VARCHAR(100) NOT NULL,
    version      VARCHAR(10)  NOT NULL,
    description  VARCHAR(255) NOT NULL,
    applied_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    applied_by   VARCHAR(200) NOT NULL DEFAULT 'unknown',
    checksum     VARCHAR(64)  NOT NULL,
    rollback_sql TEXT,
    PRIMARY KEY (module, version)
);
"""

_APP_STATE_DDL = """
CREATE TABLE IF NOT EXISTS platform.app_state (
    app_key            VARCHAR(200) PRIMARY KEY,
    module_manifest    JSONB        NOT NULL DEFAULT '{}',
    manifest_hash      VARCHAR(16)  NOT NULL DEFAULT '',
    tenant_manifest    JSONB        NOT NULL DEFAULT '{}',
    tenant_hash        VARCHAR(16)  NOT NULL DEFAULT '',
    app_version        VARCHAR(50),
    last_seen_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
"""

# Per-tenant tracking table (created in each tenant schema)
_TENANT_SCHEMA_MIGRATIONS_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.schema_migrations (
    module       VARCHAR(100) NOT NULL,
    version      VARCHAR(10)  NOT NULL,
    description  VARCHAR(255) NOT NULL,
    applied_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    applied_by   VARCHAR(200) NOT NULL DEFAULT 'unknown',
    checksum     VARCHAR(64)  NOT NULL,
    rollback_sql TEXT,
    PRIMARY KEY (module, version)
);
"""

# Advisory lock key
_MIGRATION_LOCK_KEY = 0xD7_A5_7085

# pg_notify channel name
_NOTIFY_CHANNEL = "dynastore.migrations"


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


_MIGRATE_PUBLIC_TO_PLATFORM_DDL = """
DO $$
BEGIN
    -- Move schema_migrations from public to platform if it exists there
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'schema_migrations')
       AND NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'platform' AND tablename = 'schema_migrations')
    THEN
        ALTER TABLE public.schema_migrations SET SCHEMA platform;
        RAISE NOTICE 'Moved public.schema_migrations -> platform.schema_migrations';
    END IF;

    -- Move app_state from public to platform if it exists there
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'app_state')
       AND NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'platform' AND tablename = 'app_state')
    THEN
        ALTER TABLE public.app_state SET SCHEMA platform;
        RAISE NOTICE 'Moved public.app_state -> platform.app_state';
    END IF;

    -- Move event_subscriptions from public to platform if it exists there
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'event_subscriptions')
       AND NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'platform' AND tablename = 'event_subscriptions')
    THEN
        ALTER TABLE public.event_subscriptions SET SCHEMA platform;
        RAISE NOTICE 'Moved public.event_subscriptions -> platform.event_subscriptions';
    END IF;

    -- Move stored procedures from public to platform
    IF EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public' AND p.proname = 'update_collection_extents')
       AND NOT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'platform' AND p.proname = 'update_collection_extents')
    THEN
        ALTER FUNCTION public.update_collection_extents() SET SCHEMA platform;
        RAISE NOTICE 'Moved public.update_collection_extents -> platform';
    END IF;

    IF EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public' AND p.proname = 'asset_cleanup')
       AND NOT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'platform' AND p.proname = 'asset_cleanup')
    THEN
        ALTER FUNCTION public.asset_cleanup() SET SCHEMA platform;
        RAISE NOTICE 'Moved public.asset_cleanup -> platform';
    END IF;

    IF EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public' AND p.proname = 'cleanup_orphaned_cron_jobs')
       AND NOT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'platform' AND p.proname = 'cleanup_orphaned_cron_jobs')
    THEN
        ALTER FUNCTION public.cleanup_orphaned_cron_jobs() SET SCHEMA platform;
        RAISE NOTICE 'Moved public.cleanup_orphaned_cron_jobs -> platform';
    END IF;
END;
$$;
"""


async def _ensure_tracking_tables(engine: DbResource) -> None:
    async with managed_transaction(engine) as conn:
        # 1. Create the platform schema
        await DDLQuery(_PLATFORM_SCHEMA_DDL).execute(conn)
        # 2. Migrate legacy objects from public → platform (one-time, idempotent)
        await DDLQuery(_MIGRATE_PUBLIC_TO_PLATFORM_DDL).execute(conn)
        # 3. Ensure tracking tables exist in platform schema
        await DDLQuery(_SCHEMA_MIGRATIONS_DDL).execute(conn)
        await DDLQuery(_APP_STATE_DDL).execute(conn)


async def _ensure_tenant_tracking_table(engine: DbResource, schema: str) -> None:
    async with managed_transaction(engine) as conn:
        await DDLQuery(_TENANT_SCHEMA_MIGRATIONS_DDL).execute(conn, schema=schema)


async def _get_stored_manifest_hash(
    engine: DbResource, app_key: str
) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (global_hash, tenant_hash) for this app_key.
    Returns (None, None) if the row or table doesn't exist.
    """
    from .locking_tools import check_table_exists

    if not await check_table_exists(engine, "app_state", PLATFORM_SCHEMA):
        return None, None

    sql = "SELECT manifest_hash, tenant_hash FROM platform.app_state WHERE app_key = :app_key;"
    try:
        async with managed_transaction(engine) as conn:
            row = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
                conn, app_key=app_key
            )
        if row:
            return row.get("manifest_hash"), row.get("tenant_hash")
        return None, None
    except Exception:
        # tenant_hash column may not exist on older DBs — fall back gracefully
        return None, None


async def _get_stored_manifest(
    engine: DbResource, app_key: str
) -> Optional[Dict[str, str]]:
    sql = "SELECT module_manifest FROM platform.app_state WHERE app_key = :app_key;"
    async with managed_transaction(engine) as conn:
        row = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
            conn, app_key=app_key
        )
    if row is None:
        return None
    manifest = row["module_manifest"]
    return manifest if isinstance(manifest, dict) else json.loads(manifest)


async def _upsert_app_state(
    engine: DbResource,
    app_key: str,
    manifest: Dict[str, str],
    tenant_manifest: Optional[Dict[str, str]] = None,
) -> None:
    mhash = _compute_manifest_hash(manifest)
    thash = _compute_manifest_hash(tenant_manifest) if tenant_manifest else ""
    tmanifest_json = json.dumps(tenant_manifest) if tenant_manifest else "{}"
    sql = """
        INSERT INTO platform.app_state
            (app_key, module_manifest, manifest_hash, tenant_manifest, tenant_hash, last_seen_at)
        VALUES (:app_key, CAST(:manifest AS jsonb), :mhash,
                CAST(:tmanifest AS jsonb), :thash, NOW())
        ON CONFLICT (app_key) DO UPDATE
            SET module_manifest  = EXCLUDED.module_manifest,
                manifest_hash    = EXCLUDED.manifest_hash,
                tenant_manifest  = EXCLUDED.tenant_manifest,
                tenant_hash      = EXCLUDED.tenant_hash,
                last_seen_at     = NOW();
    """
    async with managed_transaction(engine) as conn:
        await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
            conn,
            app_key=app_key,
            manifest=json.dumps(manifest),
            mhash=mhash,
            tmanifest=tmanifest_json,
            thash=thash,
        )


async def _get_applied_versions(
    engine: DbResource, schema: str = PLATFORM_SCHEMA
) -> Dict[Tuple[str, str], str]:
    sql = f'SELECT module, version, checksum FROM "{schema}".schema_migrations;'
    async with managed_transaction(engine) as conn:
        rows = (
            await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(conn)
            or []
        )
    return {(r["module"], r["version"]): r["checksum"] for r in rows}


def _verify_checksum(
    script: _MigrationScript, applied: Dict[Tuple[str, str], str]
) -> None:
    stored = applied.get((script.module, script.version))
    if stored and stored != script.checksum:
        raise MigrationChecksumError(
            f"MigrationRunner: [{script.module}] '{script.version}' checksum mismatch. "
            f"The SQL file was modified after it was applied. "
            f"Stored: {stored[:12]}…  Current: {script.checksum[:12]}…  "
            f"Resolve manually before restarting."
        )


async def _apply_migration(
    engine: DbResource,
    script: _MigrationScript,
    app_key: str,
    schema: str = PLATFORM_SCHEMA,
) -> None:
    scope = "tenant" if script.is_tenant else "global"
    logger.info(
        f"MigrationRunner: [{script.module}] Applying '{script.version}' "
        f"({scope}) — {script.description} …"
    )
    try:
        sql = script.sql
        rollback_sql = script.rollback_sql
        if script.is_tenant and schema != PLATFORM_SCHEMA:
            sql = sql.replace("{schema}", schema)
            if rollback_sql:
                rollback_sql = rollback_sql.replace("{schema}", schema)

        tracking_schema = schema if script.is_tenant else PLATFORM_SCHEMA

        async with managed_transaction(engine) as conn:
            await DDLQuery(sql).execute(conn)
            await DQLQuery(
                f"""
                INSERT INTO "{tracking_schema}".schema_migrations
                    (module, version, description, applied_by, checksum, rollback_sql)
                VALUES (:module, :version, :description, :applied_by, :checksum, :rollback_sql)
                ON CONFLICT (module, version) DO NOTHING;
                """,
                result_handler=ResultHandler.NONE,
            ).execute(
                conn,
                module=script.module,
                version=script.version,
                description=script.description,
                applied_by=app_key,
                checksum=script.checksum,
                rollback_sql=rollback_sql,
            )
        logger.info(
            f"MigrationRunner: [{script.module}] '{script.version}' applied ({scope})."
        )
    except Exception as e:
        raise MigrationError(
            f"MigrationRunner: [{script.module}] Failed '{script.version}' ({scope}): {e}"
        ) from e


async def _notify_other_instances(
    engine: DbResource, app_key: str, manifest: Dict[str, str]
) -> None:
    payload = json.dumps(
        {"app_key": app_key, "manifest": manifest},
        separators=(",", ":"),
    )
    try:
        async with managed_transaction(engine) as conn:
            await DQLQuery(
                "SELECT pg_notify(:channel, :payload);",
                result_handler=ResultHandler.NONE,
            ).execute(conn, channel=_NOTIFY_CHANNEL, payload=payload)
        logger.info(
            f"MigrationRunner: [{app_key}] Broadcast migration event on "
            f"channel '{_NOTIFY_CHANNEL}'."
        )
    except Exception as e:
        logger.warning(f"MigrationRunner: pg_notify failed (non-fatal): {e}")


async def _acquire_advisory_lock(conn: DbResource) -> bool:
    r = await DQLQuery(
        "SELECT pg_try_advisory_lock(:key) AS acquired;",
        result_handler=ResultHandler.ONE_DICT,
    ).execute(conn, key=_MIGRATION_LOCK_KEY)
    return bool(r and r["acquired"])


async def _release_advisory_lock(conn: DbResource) -> None:
    await DQLQuery(
        "SELECT pg_advisory_unlock(:key);",
        result_handler=ResultHandler.NONE,
    ).execute(conn, key=_MIGRATION_LOCK_KEY)


async def _get_all_tenant_schemas(engine: DbResource) -> List[str]:
    """Return all physical schemas from catalog.catalogs, if the table exists."""
    from .locking_tools import check_table_exists

    if not await check_table_exists(engine, "catalogs", "catalog"):
        return []

    sql = "SELECT physical_schema FROM catalog.catalogs WHERE physical_schema IS NOT NULL;"
    async with managed_transaction(engine) as conn:
        rows = (
            await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(conn)
            or []
        )
    return [r["physical_schema"] for r in rows]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def get_expected_manifest() -> Dict[str, str]:
    """Return the expected global manifest (no SQL file reads involved)."""
    return _get_expected_manifest()


async def get_expected_tenant_manifest() -> Dict[str, str]:
    """Return the expected tenant manifest (no SQL file reads involved)."""
    return _get_expected_tenant_manifest()


async def get_stored_manifest(
    engine: DbResource, app_key: Optional[str] = None
) -> Optional[Dict[str, str]]:
    app_key = app_key or os.getenv("NAME", "unknown-app")
    try:
        return await _get_stored_manifest(engine, app_key)
    except Exception:
        return None


async def is_up_to_date(
    engine: DbResource, app_key: Optional[str] = None
) -> bool:
    """
    Ultra-cheap check: compares stored manifest hashes (global + tenant)
    against expected hashes.
    """
    expected = _get_expected_manifest()
    expected_hash = _compute_manifest_hash(expected)
    expected_tenant = _get_expected_tenant_manifest()
    expected_tenant_hash = (
        _compute_manifest_hash(expected_tenant) if expected_tenant else ""
    )

    app_key = app_key or os.getenv("NAME", "unknown-app")
    try:
        stored_hash, stored_tenant_hash = await _get_stored_manifest_hash(
            engine, app_key
        )
    except Exception:
        return False

    global_ok = stored_hash == expected_hash
    tenant_ok = (stored_tenant_hash or "") == expected_tenant_hash
    return global_ok and tenant_ok


async def check_migration_status(
    engine: DbResource, app_key: Optional[str] = None
) -> MigrationStatus:
    """
    Check migration status WITHOUT applying anything.
    Logs warnings if pending migrations are detected.
    This is the function called at startup instead of run_migrations.

    Returns:
        MigrationStatus enum value.
    """
    app_key = app_key or os.getenv("NAME", "unknown-app")

    # Ensure tracking tables exist and all columns are present (idempotent DDL).
    # This also upgrades pre-existing app_state rows that lack tenant_hash/tenant_manifest.
    try:
        await _ensure_tracking_tables(engine)
    except Exception as e:
        logger.warning(
            f"MigrationRunner: [{app_key}] Could not ensure tracking tables: {e}"
        )
        return MigrationStatus.UNCHECKED

    try:
        up_to_date = await is_up_to_date(engine, app_key)
    except Exception as e:
        logger.warning(
            f"MigrationRunner: [{app_key}] Status check failed: {e}"
        )
        return MigrationStatus.UNCHECKED

    if up_to_date:
        logger.info(
            f"MigrationRunner: [{app_key}] Database is up to date."
        )
        # Touch last_seen_at
        try:
            async with managed_transaction(engine) as conn:
                await DQLQuery(
                    "UPDATE platform.app_state SET last_seen_at = NOW() WHERE app_key = :app_key;",
                    result_handler=ResultHandler.NONE,
                ).execute(conn, app_key=app_key)
        except Exception:
            pass
        return MigrationStatus.UP_TO_DATE

    # Determine if it's pending or drift
    expected = _get_expected_manifest()
    stored = await get_stored_manifest(engine, app_key)

    if stored is None:
        logger.warning(
            f"MigrationRunner: [{app_key}] No stored manifest found. "
            f"Database requires initial migration via admin API. "
            f"Expected modules: {list(expected.keys())}"
        )
        return MigrationStatus.PENDING_MIGRATIONS

    # Check for removed modules (drift)
    removed = set(stored.keys()) - set(expected.keys())
    if removed:
        logger.warning(
            f"MigrationRunner: [{app_key}] DRIFT DETECTED — modules removed: {removed}. "
            f"Previously registered modules are no longer loaded."
        )
        return MigrationStatus.DRIFT_DETECTED

    # Pending: new or updated modules
    pending_modules = []
    for mod, ver in expected.items():
        stored_ver = stored.get(mod)
        if stored_ver is None or stored_ver < ver:
            pending_modules.append(f"{mod}: {stored_ver or 'none'} → {ver}")

    if pending_modules:
        logger.warning(
            f"MigrationRunner: [{app_key}] PENDING MIGRATIONS detected. "
            f"Trigger via admin API. Pending: {', '.join(pending_modules)}"
        )
        return MigrationStatus.PENDING_MIGRATIONS

    # Tenant migrations pending
    logger.warning(
        f"MigrationRunner: [{app_key}] Tenant migrations pending. "
        f"Trigger via admin API."
    )
    return MigrationStatus.PENDING_MIGRATIONS


async def get_pending_migrations(
    engine: DbResource,
    app_key: Optional[str] = None,
) -> Dict:
    """
    Return a structured report of all pending migrations (global + tenant).
    Used by admin API for dry-run preview.

    Returns::

        {
            "global": [{"module": ..., "version": ..., "description": ...}, ...],
            "tenant": {
                "schema_name": [{"module": ..., "version": ..., "description": ...}, ...],
                ...
            }
        }
    """
    await _ensure_tracking_tables(engine)
    result: Dict = {"global": [], "tenant": {}}

    # Global pending
    scripts = _discover_all_global_scripts()
    applied = await _get_applied_versions(engine, PLATFORM_SCHEMA)
    for script in scripts:
        _verify_checksum(script, applied)
    pending = [s for s in scripts if (s.module, s.version) not in applied]
    result["global"] = [
        {"module": s.module, "version": s.version, "description": s.description}
        for s in pending
    ]

    # Tenant pending
    if _TENANT_SOURCE_ORDER:
        tenant_scripts = _discover_all_tenant_scripts()

        schemas = await _get_all_tenant_schemas(engine)
        for schema in schemas:
            try:
                await _ensure_tenant_tracking_table(engine, schema)
                t_applied = await _get_applied_versions(engine, schema)
                t_pending = [
                    s for s in tenant_scripts if (s.module, s.version) not in t_applied
                ]
                if t_pending:
                    result["tenant"][schema] = [
                        {
                            "module": s.module,
                            "version": s.version,
                            "description": s.description,
                        }
                        for s in t_pending
                    ]
            except Exception as e:
                logger.warning(
                    f"MigrationRunner: Cannot check tenant schema '{schema}': {e}"
                )
                result["tenant"][schema] = [{"error": str(e)}]

    return result


async def get_migration_history(
    engine: DbResource, schema: str = PLATFORM_SCHEMA
) -> List[Dict]:
    """Return full migration history from a schema's tracking table."""
    sql = f"""
        SELECT module, version, description, applied_at, applied_by, checksum,
               CASE WHEN rollback_sql IS NOT NULL THEN true ELSE false END AS has_rollback
        FROM "{schema}".schema_migrations
        ORDER BY applied_at;
    """
    async with managed_transaction(engine) as conn:
        rows = (
            await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(conn)
            or []
        )
    return rows


async def run_migrations(
    engine: DbResource,
    app_key: Optional[str] = None,
    max_wait_seconds: int = 60,
    dry_run: bool = False,
    scope: Literal["all", "global", "tenant"] = "all",
) -> Dict:
    """
    Apply pending migrations. Must be triggered explicitly (admin API / task).

    Args:
        engine:           Database engine or connection.
        app_key:          Application identifier (defaults to ``NAME`` env var).
        max_wait_seconds: Max seconds to wait for advisory lock.
        dry_run:          If True, return pending list without executing.
        scope:            "all" (default), "global", or "tenant".

    Returns:
        Report of applied (or would-be-applied) migrations.
    """
    import asyncio

    app_key = app_key or os.getenv("NAME", "unknown-app")

    # If dry_run, just return the pending report
    if dry_run:
        return await get_pending_migrations(engine, app_key)

    # Ensure tracking tables exist
    await _ensure_tracking_tables(engine)

    report: Dict = {"global": [], "tenant": {}}

    # ── Acquire advisory lock ────────────────────────────────────────────────
    logger.info(f"MigrationRunner: [{app_key}] Acquiring migration lock …")
    async with managed_transaction(engine) as lock_conn:
        acquired = False
        waited = 0.0
        interval = 0.25
        while not acquired:
            acquired = await _acquire_advisory_lock(lock_conn)
            if not acquired:
                if waited >= max_wait_seconds:
                    raise MigrationError(
                        f"MigrationRunner: Could not acquire migration lock after "
                        f"{max_wait_seconds}s."
                    )
                logger.info(
                    f"MigrationRunner: Lock held by another instance. "
                    f"Waiting {interval:.2f}s (total: {waited:.2f}s) …"
                )
                await asyncio.sleep(interval)
                waited += interval
                interval = min(interval * 2, 10.0)

        try:
            # ── Global migrations ────────────────────────────────────────────
            if scope in ("all", "global"):
                report["global"] = await _run_global_migrations(engine, app_key)

            # ── Tenant migrations ────────────────────────────────────────────
            if scope in ("all", "tenant") and _TENANT_SOURCE_ORDER:
                report["tenant"] = await _run_tenant_migrations(engine, app_key)

            # ── Update app state ─────────────────────────────────────────────
            expected_manifest = _build_expected_manifest(
                _discover_all_global_scripts()
            )
            expected_tenant_manifest = (
                _build_expected_manifest(_discover_all_tenant_scripts())
                if _TENANT_SOURCE_ORDER
                else None
            )
            await _upsert_app_state(
                engine, app_key, expected_manifest, expected_tenant_manifest
            )

            # Notify other instances
            await _notify_other_instances(engine, app_key, expected_manifest)

            logger.info(
                f"MigrationRunner: [{app_key}] Migration complete. "
                f"Global: {expected_manifest}, "
                f"Tenant: {expected_tenant_manifest or 'none'}"
            )

        except MigrationError:
            raise
        finally:
            await _release_advisory_lock(lock_conn)

    return report


async def _run_global_migrations(
    engine: DbResource, app_key: str
) -> List[Dict[str, str]]:
    """Apply pending global migrations. Returns list of applied scripts."""
    scripts = _discover_all_global_scripts()
    applied = await _get_applied_versions(engine, PLATFORM_SCHEMA)

    # Tamper check
    for script in scripts:
        _verify_checksum(script, applied)

    pending = [s for s in scripts if (s.module, s.version) not in applied]
    applied_report = []

    if pending:
        logger.info(
            f"MigrationRunner: [{app_key}] Applying {len(pending)} global script(s): "
            + ", ".join(f"[{s.module}] {s.version}" for s in pending)
        )
        for script in pending:
            await _apply_migration(engine, script, app_key, schema=PLATFORM_SCHEMA)
            applied_report.append(
                {
                    "module": script.module,
                    "version": script.version,
                    "description": script.description,
                }
            )
    else:
        logger.info(f"MigrationRunner: [{app_key}] Global migrations up to date.")

    return applied_report


async def _run_tenant_migrations(
    engine: DbResource, app_key: str
) -> Dict[str, List[Dict[str, str]]]:
    """Apply pending tenant migrations to all tenant schemas."""
    tenant_scripts = _discover_all_tenant_scripts()
    if not tenant_scripts:
        return {}

    schemas = await _get_all_tenant_schemas(engine)
    if not schemas:
        logger.info(
            f"MigrationRunner: [{app_key}] No tenant schemas found. "
            f"Tenant migrations will apply when catalogs are created."
        )
        return {}

    tenant_report: Dict[str, List[Dict[str, str]]] = {}

    for schema in schemas:
        try:
            await _ensure_tenant_tracking_table(engine, schema)
            applied = await _get_applied_versions(engine, schema)

            # Tamper check
            for script in tenant_scripts:
                _verify_checksum(script, applied)

            pending = [
                s for s in tenant_scripts if (s.module, s.version) not in applied
            ]

            if pending:
                logger.info(
                    f"MigrationRunner: [{app_key}] Applying {len(pending)} tenant "
                    f"script(s) to schema '{schema}': "
                    + ", ".join(f"[{s.module}] {s.version}" for s in pending)
                )
                schema_report = []
                for script in pending:
                    await _apply_migration(
                        engine, script, app_key, schema=schema
                    )
                    schema_report.append(
                        {
                            "module": script.module,
                            "version": script.version,
                            "description": script.description,
                        }
                    )
                tenant_report[schema] = schema_report
        except Exception as e:
            logger.error(
                f"MigrationRunner: [{app_key}] Failed tenant migration for "
                f"schema '{schema}': {e}",
                exc_info=True,
            )
            raise MigrationError(
                f"Tenant migration failed for schema '{schema}': {e}"
            ) from e

    return tenant_report


async def rollback_migration(
    engine: DbResource,
    module: str,
    version: str,
    app_key: Optional[str] = None,
    schema: str = PLATFORM_SCHEMA,
) -> Dict[str, str]:
    """
    Rollback a specific migration using its stored rollback_sql.

    Args:
        engine:  Database engine.
        module:  Module name (e.g. "catalog").
        version: Version to rollback (e.g. "v0002").
        app_key: Application identifier.
        schema:  Schema containing the tracking table ("platform" for global).

    Returns:
        Dict with rollback result info.

    Raises:
        MigrationError: If rollback SQL is not available or execution fails.
    """
    app_key = app_key or os.getenv("NAME", "unknown-app")

    # Fetch rollback SQL
    sql = f"""
        SELECT rollback_sql FROM "{schema}".schema_migrations
        WHERE module = :module AND version = :version;
    """
    async with managed_transaction(engine) as conn:
        row = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
            conn, module=module, version=version
        )

    if not row or not row.get("rollback_sql"):
        raise MigrationError(
            f"No rollback SQL available for [{module}] {version} in schema '{schema}'."
        )

    rollback_sql = row["rollback_sql"]
    if schema != PLATFORM_SCHEMA:
        rollback_sql = rollback_sql.replace("{schema}", schema)

    logger.info(
        f"MigrationRunner: [{app_key}] Rolling back [{module}] {version} "
        f"in schema '{schema}' …"
    )

    try:
        async with managed_transaction(engine) as conn:
            await DDLQuery(rollback_sql).execute(conn)
            await DQLQuery(
                f"""
                DELETE FROM "{schema}".schema_migrations
                WHERE module = :module AND version = :version;
                """,
                result_handler=ResultHandler.NONE,
            ).execute(conn, module=module, version=version)

        logger.info(
            f"MigrationRunner: [{app_key}] Rollback [{module}] {version} complete."
        )

        # Invalidate manifest cache so next status check picks up the change
        _clear_manifest_cache()

        return {
            "module": module,
            "version": version,
            "schema": schema,
            "status": "rolled_back",
        }
    except Exception as e:
        raise MigrationError(
            f"Rollback failed for [{module}] {version} in schema '{schema}': {e}"
        ) from e
