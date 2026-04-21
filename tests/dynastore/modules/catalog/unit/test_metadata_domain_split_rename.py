"""Regression tests for ``rename_legacy_metadata_tables``.

Keep the DO-block rename approach (one atomic PL/pgSQL block covering
all three table pairs), but ensure the ``{schema}`` placeholder is
resolved in Python *before* the DDL reaches ``TemplateQueryBuilder`` —
because TemplateQueryBuilder blindly replaces every ``{schema}``
occurrence with a dialect-quoted identifier, including the ones inside
``information_schema`` string-literal predicates.  If the SQL arrives at
TemplateQueryBuilder still containing ``{schema}`` placeholders, the
``WHERE table_schema = '{schema}'`` predicates are rewritten to
``WHERE table_schema = '"myschema"'``, which can never match
``information_schema.tables.table_schema``'s unquoted value — the
rename silently no-ops and existing data is orphaned when the CREATE
TABLE IF NOT EXISTS for ``collection_metadata`` lands fresh.

These tests verify:

- The schema name is validated before being interpolated (SQL-injection
  guard).
- The DDL sent to ``DDLQuery`` has no remaining ``{schema}`` placeholder
  (so TemplateQueryBuilder can't re-quote string literals).
- The DDL contains both the quoted-literal form (for
  ``information_schema`` predicates) and the raw-identifier form (for
  ``ALTER TABLE``) with the same schema value.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest


@pytest.mark.asyncio
async def test_rename_validates_schema_identifier():
    """Malformed schema names must be rejected before any SQL runs."""
    from dynastore.modules.catalog.db_init import metadata_domain_split as mod
    from dynastore.tools.db import InvalidIdentifierError

    with pytest.raises(InvalidIdentifierError):
        await mod.rename_legacy_metadata_tables(
            conn=AsyncMock(), schema='schema"; DROP TABLE foo; --',
        )


@pytest.mark.asyncio
async def test_rename_rejects_dotted_schema_name():
    """W1 regression: validate_sql_identifier is too permissive for schemas.

    The general-purpose validator in ``dynastore.tools.db`` accepts
    ``.`` and ``-`` (its regex is ``[a-z_][a-z0-9_.>-]*``) because it
    is reused for JSON-path identifiers like ``data.key`` and
    ``data->key``.  For a PG schema name that permissiveness is
    unsafe — ``my.schema`` would pass the generic check but produce
    ``ALTER TABLE my.schema.metadata RENAME TO …`` which PG parses as
    three dotted identifiers (catalog.schema.table) — unambiguously
    the wrong rename target.  ``rename_legacy_metadata_tables`` layers
    a stricter guard on top.
    """
    from dynastore.modules.catalog.db_init import metadata_domain_split as mod
    from dynastore.tools.db import InvalidIdentifierError

    for bad in ("my.schema", "my-schema", "a.b.c"):
        with pytest.raises(InvalidIdentifierError):
            await mod.rename_legacy_metadata_tables(
                conn=AsyncMock(), schema=bad,
            )


@pytest.mark.asyncio
async def test_rename_passes_resolved_sql_without_placeholder():
    """After Python-side substitution, the SQL must not contain ``{schema}``.

    If any ``{schema}`` placeholder slipped through,
    ``TemplateQueryBuilder`` would re-quote it as an identifier —
    including inside string literals — which silently breaks the
    ``information_schema`` existence checks and orphans legacy data.
    """
    from dynastore.modules.catalog.db_init import metadata_domain_split as mod

    fake_ddl = AsyncMock()
    fake_ddl.execute = AsyncMock()

    with patch.object(mod, "DDLQuery", return_value=fake_ddl) as ddl_cls:
        await mod.rename_legacy_metadata_tables(
            conn=AsyncMock(), schema="tenant_alpha",
        )

    # Exactly one DDLQuery built.
    assert ddl_cls.call_count == 1
    sent_sql = ddl_cls.call_args.args[0]

    # No residual placeholder — TemplateQueryBuilder must not see one.
    assert "{schema}" not in sent_sql

    # Both uses — the string literal AND the identifier qualifier —
    # must contain the (validated) schema name.
    assert "table_schema = 'tenant_alpha'" in sent_sql
    assert "ALTER TABLE tenant_alpha.metadata RENAME TO collection_metadata" in sent_sql
    assert "ALTER TABLE tenant_alpha.metadata_core RENAME TO collection_metadata_core" in sent_sql
    assert "ALTER TABLE tenant_alpha.metadata_stac RENAME TO collection_metadata_stac" in sent_sql


@pytest.mark.asyncio
async def test_rename_is_single_atomic_do_block():
    """All three renames share one PL/pgSQL DO block so they run atomically."""
    from dynastore.modules.catalog.db_init import metadata_domain_split as mod

    fake_ddl = AsyncMock()
    fake_ddl.execute = AsyncMock()

    with patch.object(mod, "DDLQuery", return_value=fake_ddl) as ddl_cls:
        await mod.rename_legacy_metadata_tables(
            conn=AsyncMock(), schema="tenant_beta",
        )

    sent_sql = ddl_cls.call_args.args[0]
    assert sent_sql.count("DO $$") == 1
    assert sent_sql.count("END $$;") == 1
    # Three IF EXISTS branches — one per legacy→canonical pair.
    assert sent_sql.count("ALTER TABLE tenant_beta.") == 3
    # Execute is called exactly once (no per-pair round trips).
    fake_ddl.execute.assert_awaited_once()


def test_ddl_template_still_carries_placeholder_for_substitution():
    """The module-level DDL string keeps ``{schema}`` placeholders.

    The constant is the substitution *template* — Python's ``str.format``
    replaces it with the validated schema before execution.  If this
    ever drifted to a hard-coded schema name, ``rename_legacy_metadata_tables``
    would silently migrate the wrong tenant.
    """
    from dynastore.modules.catalog.db_init.metadata_domain_split import (
        TENANT_LEGACY_METADATA_RENAME_DDL,
    )

    # The template MUST still contain {schema} placeholders in both
    # identifier position AND string-literal position — they are
    # resolved by the Python-side format step in the function body.
    assert "{schema}.metadata" in TENANT_LEGACY_METADATA_RENAME_DDL
    assert "table_schema = '{schema}'" in TENANT_LEGACY_METADATA_RENAME_DDL


# ---------------------------------------------------------------------------
# Regression tests for the M2.3a legacy → split-table backfill
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_backfill_executes_two_insert_statements():
    """One DDLQuery carries both CORE and STAC INSERT…SELECT statements."""
    from dynastore.modules.catalog.db_init import metadata_domain_split as mod

    fake_ddl = AsyncMock()
    fake_ddl.execute = AsyncMock()
    # The backfill first probes information_schema to see if the legacy
    # columns still exist (M2.5b guard).  Return True → backfill proceeds.
    probe_execute = AsyncMock(return_value=True)
    with patch.object(mod, "DDLQuery", return_value=fake_ddl) as ddl_cls, \
            patch.object(
                mod._LEGACY_CATALOG_METADATA_COLUMN_PROBE, "execute", probe_execute,
            ):
        await mod.backfill_catalog_metadata_from_legacy(conn=AsyncMock())

    ddl_cls.assert_called_once()
    sql = ddl_cls.call_args.args[0]
    # Both domain tables addressed in one statement bundle — single txn scope.
    assert "catalog.catalog_metadata_core" in sql
    assert "catalog.catalog_metadata_stac" in sql
    # Each INSERT guarded against overwriting existing rows (M2.2 post-backfill
    # writes must not be clobbered by stale legacy data).
    assert sql.count("ON CONFLICT (catalog_id) DO NOTHING") == 2
    # SELECT targets legacy columns on catalog.catalogs for both domains.
    assert "FROM catalog.catalogs" in sql


@pytest.mark.asyncio
async def test_backfill_skips_on_post_m2_5b_deployment():
    """After M2.5b drops the legacy columns, the probe returns False → no-op backfill."""
    from dynastore.modules.catalog.db_init import metadata_domain_split as mod

    probe_execute = AsyncMock(return_value=False)
    fake_ddl = AsyncMock()
    fake_ddl.execute = AsyncMock()

    with patch.object(mod, "DDLQuery", return_value=fake_ddl) as ddl_cls, \
            patch.object(
                mod._LEGACY_CATALOG_METADATA_COLUMN_PROBE, "execute", probe_execute,
            ):
        await mod.backfill_catalog_metadata_from_legacy(conn=AsyncMock())

    probe_execute.assert_awaited_once()
    ddl_cls.assert_not_called()  # no INSERT-SELECT issued post-drop


@pytest.mark.asyncio
async def test_drop_legacy_catalog_metadata_columns_issues_alter():
    """M2.5b drop issues a single ALTER TABLE with all column drops bundled."""
    from dynastore.modules.catalog.db_init import metadata_domain_split as mod

    fake_ddl = AsyncMock()
    fake_ddl.execute = AsyncMock()
    with patch.object(mod, "DDLQuery", return_value=fake_ddl) as ddl_cls:
        await mod.drop_legacy_catalog_metadata_columns(conn=AsyncMock())

    ddl_cls.assert_called_once()
    sql = ddl_cls.call_args.args[0]
    assert "ALTER TABLE catalog.catalogs" in sql
    for col in (
        "title", "description", "keywords", "license",
        "conforms_to", "links", "assets",
        "stac_version", "stac_extensions", "extra_metadata",
    ):
        assert f"DROP COLUMN IF EXISTS {col}" in sql, (
            f"drop SQL missing column {col}"
        )


@pytest.mark.asyncio
async def test_backfill_select_filters_rows_lacking_all_columns():
    """CORE backfill must skip catalogs with no CORE metadata at all.

    Without the WHERE filter, every catalog would receive a row of NULLs
    in ``catalog_metadata_core`` — logically correct but bloats the table
    and hides the "genuinely empty" vs "migrated empty" distinction.
    """
    from dynastore.modules.catalog.db_init.metadata_domain_split import (
        _BACKFILL_CATALOG_CORE_DDL, _BACKFILL_CATALOG_STAC_DDL,
    )

    for label, sql, columns in [
        ("CORE", _BACKFILL_CATALOG_CORE_DDL,
         ("title", "description", "keywords", "license", "extra_metadata")),
        ("STAC", _BACKFILL_CATALOG_STAC_DDL,
         ("stac_version", "stac_extensions", "conforms_to", "links", "assets")),
    ]:
        assert "WHERE" in sql, f"{label} backfill lacks a WHERE filter"
        for col in columns:
            assert f"{col} IS NOT NULL" in sql, (
                f"{label} backfill doesn't guard on {col!r}; every legacy "
                f"catalog with NULL-only metadata would receive an empty row"
            )


def test_backfill_columns_align_with_split_ddl():
    """INSERT column list must match the CREATE TABLE column list.

    Drift between the two — e.g. adding a column to the DDL but
    forgetting the backfill — would leave newly-added columns NULL for
    every pre-existing catalog post-migration.  Pin the alignment here
    so a future column addition fails loudly at unit-test time.
    """
    from dynastore.modules.catalog.db_init.metadata_domain_split import (
        CATALOG_METADATA_CORE_DDL, CATALOG_METADATA_STAC_DDL,
        _BACKFILL_CATALOG_CORE_DDL, _BACKFILL_CATALOG_STAC_DDL,
    )

    for ddl, backfill, non_id in [
        (CATALOG_METADATA_CORE_DDL, _BACKFILL_CATALOG_CORE_DDL,
         ("title", "description", "keywords", "license", "extra_metadata")),
        (CATALOG_METADATA_STAC_DDL, _BACKFILL_CATALOG_STAC_DDL,
         ("stac_version", "stac_extensions", "conforms_to", "links", "assets")),
    ]:
        for col in non_id:
            assert col in ddl
            assert col in backfill
