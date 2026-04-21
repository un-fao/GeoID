"""Regression tests for Phase 2's ``rename_legacy_metadata_tables``.

The earlier revision used a PL/pgSQL DO block with ``WHERE
table_schema = '{schema}'`` literals — TemplateQueryBuilder's regex
replaced every ``{schema}`` occurrence (including inside the string
literal) with the dialect-quoted identifier form, so the existence
check never matched and the rename silently no-op'd.  These tests
exercise the replacement implementation, which uses:

- A single parameterised DQL probe against ``information_schema.tables``
  (schema + table_name as bind parameters — no template substitution
  inside string literals), and
- A targeted ``DDLQuery`` per rename with ``{schema}`` appearing only as
  a table-qualifier identifier.

The tests mock the DQL / DDL executors so no live database is required.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest


@pytest.mark.asyncio
async def test_rename_skips_when_both_legacy_and_canonical_are_missing():
    """Fresh tenant: probe returns False for every legacy → canonical pair."""
    from dynastore.modules.catalog.db_init import metadata_domain_split as mod

    probe = AsyncMock(return_value=False)  # legacy exists? no
    ddl_execute = AsyncMock()

    with patch.object(mod._TABLE_EXISTS_QUERY, "execute", probe):
        fake_ddl_query = AsyncMock()
        fake_ddl_query.execute = ddl_execute
        with patch.object(mod, "DDLQuery", return_value=fake_ddl_query):
            await mod.rename_legacy_metadata_tables(conn=AsyncMock(), schema="fresh")

    # One probe per pair (3 pairs) — all return False → no DDL issued.
    assert probe.await_count == 3
    ddl_execute.assert_not_called()


@pytest.mark.asyncio
async def test_rename_issues_alter_when_legacy_present_and_canonical_missing():
    """Existing tenant: legacy table present, canonical absent → ALTER runs."""
    from dynastore.modules.catalog.db_init import metadata_domain_split as mod

    # Return True for the legacy probe, False for the canonical probe.
    # Probes alternate legacy-then-canonical per pair, per the source.
    probe_results = [
        True,  False,   # metadata exists, collection_metadata does not
        True,  False,   # metadata_core exists, collection_metadata_core does not
        True,  False,   # metadata_stac exists, collection_metadata_stac does not
    ]
    probe = AsyncMock(side_effect=probe_results)

    fake_ddl = AsyncMock()
    fake_ddl.execute = AsyncMock()

    with patch.object(mod._TABLE_EXISTS_QUERY, "execute", probe):
        with patch.object(mod, "DDLQuery", return_value=fake_ddl) as ddl_cls:
            await mod.rename_legacy_metadata_tables(
                conn=AsyncMock(), schema="tenant_alpha",
            )

    # One ALTER per pair (3 pairs).
    assert ddl_cls.call_count == 3

    # Sanity-check the SQL shape of the first ALTER issued.
    first_sql = ddl_cls.call_args_list[0].args[0]
    assert first_sql.startswith("ALTER TABLE {schema}.metadata ")
    assert "RENAME TO collection_metadata;" in first_sql


@pytest.mark.asyncio
async def test_rename_skips_pair_when_both_present():
    """Both legacy and canonical tables present: no ALTER issued."""
    from dynastore.modules.catalog.db_init import metadata_domain_split as mod

    # For each pair: legacy probe True, canonical probe True.
    probe_results = [True, True, True, True, True, True]
    probe = AsyncMock(side_effect=probe_results)

    fake_ddl = AsyncMock()
    fake_ddl.execute = AsyncMock()

    with patch.object(mod._TABLE_EXISTS_QUERY, "execute", probe):
        with patch.object(mod, "DDLQuery", return_value=fake_ddl) as ddl_cls:
            await mod.rename_legacy_metadata_tables(
                conn=AsyncMock(), schema="tenant_beta",
            )

    ddl_cls.assert_not_called()


@pytest.mark.asyncio
async def test_rename_handles_mixed_state_per_pair_independently():
    """metadata renamed, metadata_core still legacy — second pair runs, first skips."""
    from dynastore.modules.catalog.db_init import metadata_domain_split as mod

    probe_results = [
        False,          # metadata gone (already renamed last run)
        True,  False,   # metadata_core exists, collection_metadata_core does not
        True,  False,   # metadata_stac exists, collection_metadata_stac does not
    ]
    probe = AsyncMock(side_effect=probe_results)

    fake_ddl = AsyncMock()
    fake_ddl.execute = AsyncMock()

    with patch.object(mod._TABLE_EXISTS_QUERY, "execute", probe):
        with patch.object(mod, "DDLQuery", return_value=fake_ddl) as ddl_cls:
            await mod.rename_legacy_metadata_tables(
                conn=AsyncMock(), schema="mixed",
            )

    assert ddl_cls.call_count == 2  # two ALTERs (core + stac), not three


@pytest.mark.asyncio
async def test_rename_rejects_invalid_schema_identifier():
    """The function must validate the schema name before touching SQL."""
    from dynastore.modules.catalog.db_init import metadata_domain_split as mod
    from dynastore.tools.db import InvalidIdentifierError

    with pytest.raises(InvalidIdentifierError):
        await mod.rename_legacy_metadata_tables(
            conn=AsyncMock(), schema="schema_with_quote\";DROP TABLE foo;--",
        )


def test_legacy_rename_constant_contains_all_three_pairs():
    """Guard against accidentally shortening ``_LEGACY_METADATA_RENAMES``."""
    from dynastore.modules.catalog.db_init.metadata_domain_split import (
        _LEGACY_METADATA_RENAMES,
    )

    assert _LEGACY_METADATA_RENAMES == (
        ("metadata", "collection_metadata"),
        ("metadata_core", "collection_metadata_core"),
        ("metadata_stac", "collection_metadata_stac"),
    )


def test_no_plsqlbased_do_block_remains():
    """Confirm the broken PL/pgSQL DO-block constant is gone.

    The earlier revision exported a ``TENANT_LEGACY_METADATA_RENAME_DDL``
    string literal that referenced ``WHERE table_schema = '{schema}'``.
    TemplateQueryBuilder's regex substituted the placeholder inside the
    string literal with a dialect-quoted identifier, making the IF check
    always false.  Importing a stale module or copying the old constant
    into a follow-up patch would silently regress data migration, so
    assert the symbol is absent.
    """
    from dynastore.modules.catalog.db_init import metadata_domain_split as mod

    assert not hasattr(mod, "TENANT_LEGACY_METADATA_RENAME_DDL"), (
        "The DO-block constant must not be reintroduced; use the probe + "
        "rename orchestrator (rename_legacy_metadata_tables) instead."
    )
