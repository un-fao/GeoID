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

Ultra-fast, versioned database migration engine for DynaStore.

## Design Principles

### Speed-first startup (Cloud Run)
The startup path is optimised so that on a normal restart (no pending migrations)
the cost is a *single* lightweight DB query — no file I/O, no advisory lock:

    1. FAST-PATH: SELECT the stored manifest from ``public.app_state``
       using a single query.  If it already matches the *expected manifest hash*
       that was computed **only from process-local data** (Python package
       registrations), return immediately.  Zero disk reads of SQL files.
    2. FILE-SCAN PATH: Only when the fast-path fails do we scan the SQL files
       from disk, compute checksums, and enter the slow (locking) path.

### Multiple applications, one database
Many Cloud Run services (api, worker, admin …) can share the same PG database:

* ``public.app_state``  — one row **per app instance** (keyed by ``app_key``).
  Records what version each running instance last applied.
* ``public.schema_migrations`` — authoritative history; one row per
  (module, version) applied by *any* app.  Each row also records the
  ``app_key`` that applied it.

### Cross-app notifications via pg_notify
When an instance applies migrations it broadcasts a notification on the
``dynastore.migrations`` channel, including the new manifest JSON.
Other instances subscribed to this channel can react (e.g. log a warning,
gracefully restart, or update their own in-process cache).

### Advisory lock (slow-path only)
An exclusive advisory lock is acquired **only** when migrations are pending.
This prevents two instances from migrating concurrently.

### Tamper detection
SHA-256 checksums are verified for every already-applied script.
A modified SQL file raises ``MigrationChecksumError`` and blocks startup.

### Conservative / rollback-safe
Each SQL script runs in its own transaction.  If a script fails the
transaction is rolled back and a ``MigrationError`` is raised, which
prevents the module from coming online.

## Usage

In your module's ``__init__.py``:

    from dynastore.modules.db_config.migration_runner import register_module_migrations
    register_module_migrations("catalog", "dynastore.modules.catalog.migrations")

## Adding a new migration

1. Create a file  ``migrations/v####__short_description.sql``  in your module.
2. Write **idempotent**, **additive** SQL (prefer ADD COLUMN IF NOT EXISTS,
   CREATE TABLE IF NOT EXISTS … ).
3. Never edit a script that has already been applied to a production database —
   the checksum guard will block startup.
4. Bump the version counter by 1 from the last script in your module.

## Versioning scheme

    v0001  — initial baseline
    v0002  — next migration
    …

Versions are padded four-digit integers so lexicographic and numeric sort agree.
"""

import hashlib
import importlib.resources
import json
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

from sqlalchemy.ext.asyncio import AsyncEngine

from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DQLQuery,
    ResultHandler,
    managed_transaction,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class MigrationError(RuntimeError):
    """Raised when a migration script fails.  The module must not start."""


class MigrationChecksumError(MigrationError):
    """
    Raised when a previously-applied SQL file has been modified on disk.
    Resolve the discrepancy manually before restarting.
    """


# ---------------------------------------------------------------------------
# Module registry (populated at import time — zero db cost)
# ---------------------------------------------------------------------------

#: module_name → python package containing v####__*.sql files
_MIGRATION_SOURCES: Dict[str, str] = {
    "core": "dynastore.modules.db_config.migrations",
}
#: Preserve registration order (core always first)
_SOURCE_ORDER: List[str] = ["core"]


def register_module_migrations(module_name: str, package_path: str) -> None:
    """
    Register a module's migrations package.  Call from the module's __init__.py.

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
# Fast-path manifest hash (in-process, zero I/O)
# ---------------------------------------------------------------------------

#: Cache of {module_name: max_version} computed from package registrations.
#: This is resolved lazily from _MIGRATION_SOURCES — by listing package
#: resources — the FIRST time we need it.  Subsequent calls reuse the cache.
_expected_manifest_cache: Optional[Dict[str, str]] = None


def _clear_manifest_cache() -> None:  # test helper
    global _expected_manifest_cache
    _expected_manifest_cache = None


def _compute_manifest_hash(manifest: Dict[str, str]) -> str:
    """Stable, order-independent hash of a module→version manifest dict."""
    stable = json.dumps(dict(sorted(manifest.items())), separators=(",", ":"))
    return hashlib.sha256(stable.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Per-module version discovery (only used on slow path)
# ---------------------------------------------------------------------------


_VERSION_RE = re.compile(r"^(v\d{4})__(.+?)\.sql$", re.IGNORECASE)


def _sha256(content: str) -> str:
    return hashlib.sha256(content.encode()).hexdigest()


@dataclass
class _MigrationScript:
    module: str
    version: str
    description: str
    sql: str
    checksum: str


def _discover_scripts_for_module(module_name: str, package_path: str) -> List[_MigrationScript]:
    scripts: List[_MigrationScript] = []
    try:
        pkg = importlib.resources.files(package_path)
        for resource in pkg.iterdir():
            m = _VERSION_RE.match(resource.name)
            if not m:
                continue
            sql = resource.read_text(encoding="utf-8")
            scripts.append(_MigrationScript(
                module=module_name,
                version=m.group(1).lower(),
                description=m.group(2).replace("_", " "),
                sql=sql,
                checksum=_sha256(sql),
            ))
    except Exception as e:
        logger.warning(
            f"MigrationRunner: Cannot load '{module_name}' from '{package_path}': {e}"
        )
    scripts.sort(key=lambda s: s.version)
    return scripts


def _discover_all_scripts() -> List[_MigrationScript]:
    all_scripts: List[_MigrationScript] = []
    for module_name in _SOURCE_ORDER:
        all_scripts.extend(
            _discover_scripts_for_module(module_name, _MIGRATION_SOURCES[module_name])
        )
    return all_scripts


def _build_expected_manifest_from_scripts(scripts: List[_MigrationScript]) -> Dict[str, str]:
    manifest: Dict[str, str] = {}
    for s in scripts:
        if s.module not in manifest or s.version > manifest[s.module]:
            manifest[s.module] = s.version
    return manifest


# ---------------------------------------------------------------------------
# Fast-path: estimate expected manifest WITHOUT reading SQL files
#
# Strategy: list resource names only (no file reads) and extract the version
# from the filename.  If the listing itself fails we fall back to the full
# script scan.
# ---------------------------------------------------------------------------


def _peek_max_version_for_module(package_path: str) -> Optional[str]:
    """Return the max version string found in a package by listing filenames only."""
    try:
        pkg = importlib.resources.files(package_path)
        max_ver: Optional[str] = None
        for resource in pkg.iterdir():
            m = _VERSION_RE.match(resource.name)
            if not m:
                continue
            ver = m.group(1).lower()
            if max_ver is None or ver > max_ver:
                max_ver = ver
        return max_ver
    except Exception:
        return None


def _get_expected_manifest() -> Dict[str, str]:
    """
    Return the expected manifest WITHOUT reading any SQL file contents.
    Results are cached for the process lifetime (safe: registrations are
    fixed after import-time).
    """
    global _expected_manifest_cache
    if _expected_manifest_cache is not None:
        return _expected_manifest_cache

    manifest: Dict[str, str] = {}
    for module_name in _SOURCE_ORDER:
        package_path = _MIGRATION_SOURCES[module_name]
        ver = _peek_max_version_for_module(package_path)
        if ver:
            manifest[module_name] = ver
    _expected_manifest_cache = manifest
    return manifest


# ---------------------------------------------------------------------------
# DDL for tracking tables
# ---------------------------------------------------------------------------

_SCHEMA_MIGRATIONS_DDL = """
CREATE TABLE IF NOT EXISTS public.schema_migrations (
    module      VARCHAR(100) NOT NULL,
    version     VARCHAR(10)  NOT NULL,
    description VARCHAR(255) NOT NULL,
    applied_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    applied_by  VARCHAR(200) NOT NULL DEFAULT 'unknown',
    checksum    VARCHAR(64)  NOT NULL,
    PRIMARY KEY (module, version)
);
"""

# One row per app_key (Cloud Run service name / instance label).
# module_manifest is the union of *all* module→max_version applied by any
# app using this database, so all clients can detect drift.
_APP_STATE_DDL = """
CREATE TABLE IF NOT EXISTS public.app_state (
    app_key         VARCHAR(200) PRIMARY KEY,
    module_manifest JSONB        NOT NULL DEFAULT '{{}}',
    manifest_hash   VARCHAR(16)  NOT NULL DEFAULT '',
    app_version     VARCHAR(50),
    last_seen_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
"""

# Advisory lock key ("DynaStore migrations" — stable arbitrary bigint)
_MIGRATION_LOCK_KEY = 0xD7_A5_7085

# pg_notify channel name
_NOTIFY_CHANNEL = "dynastore.migrations"


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


async def _ensure_tracking_tables(engine: AsyncEngine) -> None:
    async with managed_transaction(engine) as conn:
        await DDLQuery(_SCHEMA_MIGRATIONS_DDL).execute(conn)
        await DDLQuery(_APP_STATE_DDL).execute(conn)


async def _get_stored_manifest_hash(engine: AsyncEngine, app_key: str) -> Optional[str]:
    """
    Ultra-cheap fast-path check: returns only the stored hash for this app_key.
    Single indexed lookup — no JSON parsing of full manifest.
    Returns None if the column doesn't exist yet (pre-v0002 databases fall
    back to the slow path transparently).
    """
    sql = "SELECT manifest_hash FROM public.app_state WHERE app_key = :app_key;"
    try:
        async with managed_transaction(engine) as conn:
            row = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
                conn, app_key=app_key
            )
        return row["manifest_hash"] if row else None
    except Exception:
        # Column may not exist yet (pre-v0002) — fall back to slow path
        return None


async def _get_stored_manifest(engine: AsyncEngine, app_key: str) -> Optional[Dict[str, str]]:
    sql = "SELECT module_manifest FROM public.app_state WHERE app_key = :app_key;"
    async with managed_transaction(engine) as conn:
        row = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
            conn, app_key=app_key
        )
    if row is None:
        return None
    manifest = row["module_manifest"]
    return manifest if isinstance(manifest, dict) else json.loads(manifest)


async def _upsert_app_state(
    engine: AsyncEngine, app_key: str, manifest: Dict[str, str]
) -> None:
    mhash = _compute_manifest_hash(manifest)
    sql = """
        INSERT INTO public.app_state (app_key, module_manifest, manifest_hash, last_seen_at)
        VALUES (:app_key, CAST(:manifest AS jsonb), :mhash, NOW())
        ON CONFLICT (app_key) DO UPDATE
            SET module_manifest = EXCLUDED.module_manifest,
                manifest_hash   = EXCLUDED.manifest_hash,
                last_seen_at    = NOW();
    """
    async with managed_transaction(engine) as conn:
        await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
            conn, app_key=app_key, manifest=json.dumps(manifest), mhash=mhash
        )


async def _get_applied_versions(engine: AsyncEngine) -> Dict[Tuple[str, str], str]:
    sql = "SELECT module, version, checksum FROM public.schema_migrations;"
    async with managed_transaction(engine) as conn:
        rows = await DQLQuery(sql, result_handler=ResultHandler.ALL_DICTS).execute(conn) or []
    return {(r["module"], r["version"]): r["checksum"] for r in rows}


def _verify_checksum(script: _MigrationScript, applied: Dict[Tuple[str, str], str]) -> None:
    stored = applied.get((script.module, script.version))
    if stored and stored != script.checksum:
        raise MigrationChecksumError(
            f"MigrationRunner: [{script.module}] '{script.version}' checksum mismatch. "
            f"The SQL file was modified after it was applied. "
            f"Stored: {stored[:12]}…  Current: {script.checksum[:12]}…  "
            f"Resolve manually before restarting."
        )


async def _apply_migration(engine: AsyncEngine, script: _MigrationScript, app_key: str) -> None:
    logger.info(
        f"MigrationRunner: [{script.module}] Applying '{script.version}' "
        f"— {script.description} …"
    )
    try:
        async with managed_transaction(engine) as conn:
            await DDLQuery(script.sql).execute(conn)
            await DQLQuery(
                """
                INSERT INTO public.schema_migrations
                    (module, version, description, applied_by, checksum)
                VALUES (:module, :version, :description, :applied_by, :checksum)
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
            )
        logger.info(f"MigrationRunner: [{script.module}] '{script.version}' applied.")
    except Exception as e:
        raise MigrationError(
            f"MigrationRunner: [{script.module}] Failed '{script.version}': {e}"
        ) from e


async def _notify_other_instances(
    engine: AsyncEngine, app_key: str, manifest: Dict[str, str]
) -> None:
    """
    Broadcast a pg_notify on ``dynastore.migrations`` so other running
    instances can react (e.g. log a warning, schedule a graceful restart).
    The payload is a compact JSON object.
    """
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
        # Non-fatal — don't block the migration
        logger.warning(f"MigrationRunner: pg_notify failed (non-fatal): {e}")


async def _acquire_advisory_lock(conn) -> bool:
    r = await DQLQuery(
        "SELECT pg_try_advisory_lock(:key) AS acquired;",
        result_handler=ResultHandler.ONE_DICT,
    ).execute(conn, key=_MIGRATION_LOCK_KEY)
    return bool(r and r["acquired"])


async def _release_advisory_lock(conn) -> None:
    await DQLQuery(
        "SELECT pg_advisory_unlock(:key);",
        result_handler=ResultHandler.NONE,
    ).execute(conn, key=_MIGRATION_LOCK_KEY)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def get_expected_manifest() -> Dict[str, str]:
    """Return the expected manifest (no SQL file reads involved)."""
    return _get_expected_manifest()


async def get_stored_manifest(
    engine: AsyncEngine, app_key: Optional[str] = None
) -> Optional[Dict[str, str]]:
    app_key = app_key or os.getenv("NAME", "unknown-app")
    try:
        return await _get_stored_manifest(engine, app_key)
    except Exception:
        return None


async def is_up_to_date(engine: AsyncEngine, app_key: Optional[str] = None) -> bool:
    """
    Ultra-cheap check: compares the stored manifest HASH for this app_key
    against the expected manifest hash.  Only two strings to compare —
    no JSON decoding, no file reads.
    """
    expected = _get_expected_manifest()
    expected_hash = _compute_manifest_hash(expected)
    app_key = app_key or os.getenv("NAME", "unknown-app")
    try:
        stored_hash = await _get_stored_manifest_hash(engine, app_key)
    except Exception:
        return False
    return stored_hash == expected_hash


async def run_migrations(
    engine: AsyncEngine,
    app_key: Optional[str] = None,
    max_wait_seconds: int = 60,
) -> None:
    """
    Apply pending migrations for all registered modules.

    ## Startup path (Cloud Run optimised)

    1. **Ultra-fast path** (normal restart — ~1 SELECT, no file read, no lock):
       Compute expected manifest hash in-process from package resource *names*
       only (no SQL file contents read), then compare against the stored
       ``manifest_hash`` column in a single indexed lookup.
       → **Match**: touch ``last_seen_at`` and return immediately.

    2. **File-scan + lock path** (first run or new migration):
       Read SQL files, verify checksums, acquire advisory lock, apply pending
       scripts, broadcast pg_notify to other instances, update app_state.

    ## Multi-app behaviour
    Multiple Cloud Run services can share the same database.
    Each uses a distinct ``app_key`` (set via the ``NAME`` env var).
    ``schema_migrations`` is the shared authoritative history.
    ``app_state`` tracks per-app last-seen version so operators can monitor drift.

    Args:
        engine:           Async database engine.
        app_key:          Application identifier (defaults to ``NAME`` env var).
        max_wait_seconds: Max seconds to wait for advisory lock before failing.
    """
    import asyncio

    app_key = app_key or os.getenv("NAME", "unknown-app")

    # ── STEP 1: compute expected manifest in-process (no file reads) ──────────
    expected_manifest = _get_expected_manifest()
    expected_hash = _compute_manifest_hash(expected_manifest)

    logger.debug(
        f"MigrationRunner: [{app_key}] Expected manifest hash: {expected_hash} "
        f"manifest: {expected_manifest}"
    )

    # ── STEP 2: Ensure tracking tables exist (idempotent DDL) ────────────────
    # This is always needed on first-ever run.  After that, PG does nothing.
    await _ensure_tracking_tables(engine)

    # ── STEP 3: ULTRA-FAST PATH — single hash comparison, no file reads ───────
    stored_hash = await _get_stored_manifest_hash(engine, app_key)
    if stored_hash == expected_hash:
        logger.info(f"MigrationRunner: [{app_key}] Up to date (hash match) — fast path.")
        # Touch last_seen_at so operators can detect stale/dead instances
        async with managed_transaction(engine) as conn:
            await DQLQuery(
                "UPDATE public.app_state SET last_seen_at = NOW() WHERE app_key = :app_key;",
                result_handler=ResultHandler.NONE,
            ).execute(conn, app_key=app_key)
        return

    # ── STEP 4: FILE-SCAN PATH — read SQL files, verify checksums ────────────
    logger.info(
        f"MigrationRunner: [{app_key}] Manifest hash mismatch "
        f"(stored={stored_hash!r}, expected={expected_hash!r}) — scanning migration files …"
    )
    scripts = _discover_all_scripts()
    # Re-compute manifest from actual files to catch registration mis-matches
    expected_manifest = _build_expected_manifest_from_scripts(scripts)
    expected_hash = _compute_manifest_hash(expected_manifest)

    # ── STEP 5: SLOW PATH — advisory lock, apply, notify ─────────────────────
    logger.info(
        f"MigrationRunner: [{app_key}] Acquiring migration lock …"
    )
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
            applied = await _get_applied_versions(engine)

            # Tamper-check all previously applied scripts
            for script in scripts:
                _verify_checksum(script, applied)

            # Detect removed modules
            stored_manifest = await _get_stored_manifest(engine, app_key) or {}
            for mod in stored_manifest:
                if mod not in expected_manifest:
                    logger.warning(
                        f"MigrationRunner: [{app_key}] Module '{mod}' was previously "
                        f"registered but is no longer loaded. "
                        f"Migration history preserved in schema_migrations."
                    )

            # Apply only the pending delta
            pending = [s for s in scripts if (s.module, s.version) not in applied]
            if pending:
                logger.info(
                    f"MigrationRunner: [{app_key}] Applying {len(pending)} script(s): "
                    + ", ".join(f"[{s.module}] {s.version}" for s in pending)
                )
                for script in pending:
                    await _apply_migration(engine, script, app_key)

                # Notify other running instances
                await _notify_other_instances(engine, app_key, expected_manifest)
            else:
                logger.info(
                    f"MigrationRunner: [{app_key}] Scripts already applied by another instance."
                )

            # Update this app's manifest record
            await _upsert_app_state(engine, app_key, expected_manifest)
            logger.info(
                f"MigrationRunner: [{app_key}] Manifest updated → {expected_manifest}"
            )

        except MigrationError:
            raise  # re-raise so module startup fails
        finally:
            await _release_advisory_lock(lock_conn)
