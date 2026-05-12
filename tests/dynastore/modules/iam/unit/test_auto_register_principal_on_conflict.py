"""Test that _auto_register_principal uses the actual UUID from create_principal.

When `create_principal` encounters an ON CONFLICT (identifier already exists),
it returns the existing Principal instead of a new one. The fix ensures we
use the actual returned UUID, not the freshly allocated one, for all
downstream calls (create_identity_link, _apply_role_grants).

This prevents orphan identity_links that reference non-existent principal rows.
"""

from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest


@pytest.mark.asyncio
async def test_auto_register_principal_uses_returned_uuid_on_conflict():
    """
    Simulate create_principal returning a pre-existing UUID due to ON CONFLICT.
    Verify that create_identity_link is called with the returned UUID, not
    the freshly allocated one.
    """
    from dynastore.modules.iam.iam_service import IamService
    from dynastore.models.auth import Principal

    # Setup: create an IamService with mocked storage and dependencies
    service = IamService()

    # Mock storage layer
    service.storage = MagicMock()

    # Pre-existing principal (simulating ON CONFLICT return)
    existing_id = UUID("11111111-1111-1111-1111-111111111111")
    existing_principal = Principal(
        id=existing_id,
        provider="oidc",
        subject_id="user@example.com",
        display_name="user@example.com",
        roles=["user"],
        is_active=True,
        custom_policies=[],
        attributes={},
    )

    # Mock create_principal to return the existing principal
    service.storage.create_principal = AsyncMock(return_value=existing_principal)
    service.storage.create_identity_link = AsyncMock(return_value=True)
    service.storage.get_role = AsyncMock(return_value=None)
    service.storage.create_role = AsyncMock()

    # Mock _resolve_schema and _apply_role_grants
    service._resolve_schema = AsyncMock(return_value="iam")
    service._apply_role_grants = AsyncMock()

    # Call the method with identity dict
    identity = {
        "provider": "oidc",
        "sub": "user@example.com",
        "email": "user@example.com",
    }
    result = await service._auto_register_principal(
        identity=identity,
        catalog_id="_system_",
    )

    # Verify create_identity_link was called with the ACTUAL returned UUID,
    # not a freshly allocated one
    service.storage.create_identity_link.assert_called_once()
    call_args = service.storage.create_identity_link.call_args
    assert call_args[1]["principal_id"] == existing_id, (
        "create_identity_link should use the UUID returned by create_principal, "
        f"not a fresh one. Got {call_args[1]['principal_id']}, expected {existing_id}"
    )

    # Verify _apply_role_grants was also called with the correct UUID
    service._apply_role_grants.assert_called_once()
    apply_call_args = service._apply_role_grants.call_args
    assert apply_call_args[1]["principal_id"] == existing_id, (
        "_apply_role_grants should use the actual returned UUID"
    )

    # Verify the returned principal has the correct UUID
    assert result.id == existing_id, "Returned principal should have the actual UUID"


@pytest.mark.asyncio
async def test_auto_register_principal_uses_allocated_uuid_on_fresh_insert():
    """
    Verify that on a fresh insert (no ON CONFLICT), we can use either the
    allocated UUID or the returned one—both should be the same.
    """
    from dynastore.modules.iam.iam_service import IamService
    from dynastore.models.auth import Principal
    from uuid import uuid4

    service = IamService()
    service.storage = MagicMock()

    # Fresh principal with a new UUID
    fresh_id = uuid4()
    fresh_principal = Principal(
        id=fresh_id,
        provider="oidc",
        subject_id="newuser@example.com",
        display_name="newuser@example.com",
        roles=["user"],
        is_active=True,
        custom_policies=[],
        attributes={},
    )

    service.storage.create_principal = AsyncMock(return_value=fresh_principal)
    service.storage.create_identity_link = AsyncMock(return_value=True)
    service.storage.get_role = AsyncMock(return_value=None)
    service.storage.create_role = AsyncMock()

    service._resolve_schema = AsyncMock(return_value="iam")
    service._apply_role_grants = AsyncMock()

    identity = {
        "provider": "oidc",
        "sub": "newuser@example.com",
        "email": "newuser@example.com",
    }
    result = await service._auto_register_principal(
        identity=identity,
        catalog_id="_system_",
    )

    # Verify the returned UUID matches the fresh insert
    assert result.id == fresh_id, "Fresh insert should use the returned fresh UUID"

    # Verify create_identity_link was called with the correct UUID
    call_args = service.storage.create_identity_link.call_args
    assert call_args[1]["principal_id"] == fresh_id
