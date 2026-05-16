#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License");

"""Regression test for the admin extension's policy response projector.

Background: ``_policy_to_response`` was originally placed inside the
``AdminService`` class body (commit `88d10ef2`, PR #806) but called from
three FastAPI route handlers nested in the same class body. Python's
name resolution does not let nested functions see class-body names, so
``GET /admin/policies``, ``POST /admin/policies``, and ``PUT
/admin/policies/{id}`` all raised ``NameError`` at request time.

This test pins the helper at module scope so a future refactor that
tries to move it back inside the class fails fast on import collection
rather than at the next admin-panel call.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def test_policy_to_response_is_module_scoped():
    """The helper must be importable at module scope. A class-body
    definition would not be importable here and would NameError at
    request time when called from the nested route handlers."""
    from dynastore.extensions.admin import admin_service
    assert hasattr(admin_service, "_policy_to_response"), (
        "_policy_to_response must live at module scope — class-body "
        "placement broke /admin/policies (see PR fixing this)."
    )
    assert callable(admin_service._policy_to_response)


@pytest.mark.asyncio
async def test_list_policies_handler_does_not_nameerror_on_helper():
    """Empirical guard: the previous bug surfaced as a ``NameError``
    raised from the list_policies coroutine the moment it tried to
    project the first policy. Lock that path with a minimal stub.
    """
    from dynastore.extensions.admin.admin_service import AdminService

    fake_iam = MagicMock()
    fake_pm = MagicMock()
    fake_pm.list_policies = AsyncMock(return_value=[
        MagicMock(
            id="probe", description="d", actions=["GET"],
            resources=["/x"], effect="ALLOW",
            partition_key=None, conditions=[],
        )
    ])
    fake_iam.get_policy_service = MagicMock(return_value=fake_pm)

    with patch(
        "dynastore.extensions.admin.admin_service._iam",
        return_value=fake_iam,
    ):
        result = await AdminService.list_policies(catalog_id=None)

    assert len(result) == 1
    assert result[0].id == "probe"
