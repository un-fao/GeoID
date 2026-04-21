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
