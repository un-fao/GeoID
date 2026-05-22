"""#1198 — ``enforce_config_immutability`` must support restricting the
Immutable/WriteOnce comparison to an explicit set of fields.

Approach A for the first-write-at-tier freeze: when a tier has no stored row
yet, the inherited resolved config is used as the immutability baseline, but
the check is restricted to the fields the caller explicitly sent
(``__pydantic_fields_set__``) so a partial override does not raise false 409s
on Immutable fields a parent tier customized but the caller never touched.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncConnection

from dynastore.modules.db_config import platform_config_service as svc
from dynastore.modules.db_config.exceptions import ImmutableConfigError
from dynastore.modules.storage.driver_config import (
    ItemsWritePolicy,
    WriteConflictPolicy,
)
from dynastore.modules.storage.validity import ValiditySpec


def _conn() -> MagicMock:
    return MagicMock(spec=AsyncConnection)


@pytest.mark.asyncio
async def test_restrict_to_fields_skips_unlisted_immutable_change():
    """A changed Immutable field NOT in ``restrict_to_fields`` is ignored —
    this is what prevents false 409s on defaulted/untouched fields."""
    baseline = ItemsWritePolicy(validity=ValiditySpec(start_from="context"))
    incoming = ItemsWritePolicy(
        validity=ValiditySpec(start_from="properties.start_date"),  # Immutable change
        on_conflict=WriteConflictPolicy.REFUSE_RETURN,
    )
    with patch.object(svc, "is_materialized", new=AsyncMock(return_value=True)):
        # Only on_conflict (Mutable) is in scope → validity change is not checked.
        await svc.enforce_config_immutability(
            baseline, incoming,
            catalog_id="c", collection_id="x", conn=_conn(),
            restrict_to_fields={"on_conflict"},
        )  # must NOT raise


@pytest.mark.asyncio
async def test_restrict_to_fields_enforces_listed_immutable_change():
    """A changed Immutable field that IS in ``restrict_to_fields`` still
    raises when the resource is materialized."""
    baseline = ItemsWritePolicy(validity=ValiditySpec(start_from="context"))
    incoming = ItemsWritePolicy(
        validity=ValiditySpec(start_from="properties.start_date")
    )
    with patch.object(svc, "is_materialized", new=AsyncMock(return_value=True)):
        with pytest.raises(ImmutableConfigError) as ei:
            await svc.enforce_config_immutability(
                baseline, incoming,
                catalog_id="c", collection_id="x", conn=_conn(),
                restrict_to_fields={"validity"},
            )
    assert "validity" in str(ei.value)


@pytest.mark.asyncio
async def test_restrict_to_fields_none_checks_all_fields():
    """Default (``restrict_to_fields=None``) preserves the existing
    all-fields comparison."""
    baseline = ItemsWritePolicy(validity=ValiditySpec(start_from="context"))
    incoming = ItemsWritePolicy(
        validity=ValiditySpec(start_from="properties.start_date")
    )
    with patch.object(svc, "is_materialized", new=AsyncMock(return_value=True)):
        with pytest.raises(ImmutableConfigError):
            await svc.enforce_config_immutability(
                baseline, incoming,
                catalog_id="c", collection_id="x", conn=_conn(),
            )


@pytest.mark.asyncio
async def test_restrict_to_fields_not_materialized_allows_change():
    """The materialization gate still governs: an unmaterialized resource is
    editable even for a listed Immutable field."""
    baseline = ItemsWritePolicy(validity=ValiditySpec(start_from="context"))
    incoming = ItemsWritePolicy(
        validity=ValiditySpec(start_from="properties.start_date")
    )
    with patch.object(svc, "is_materialized", new=AsyncMock(return_value=False)):
        await svc.enforce_config_immutability(
            baseline, incoming,
            catalog_id="c", collection_id="x", conn=_conn(),
            restrict_to_fields={"validity"},
        )  # must NOT raise — not materialized
