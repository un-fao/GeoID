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

"""Unit tests for audit-log schema enforcement (fix for #1492).

The audit_log table exists only in the platform ``iam`` schema.
Two contracts are verified DB-free:

1. ``PostgresIamStorage.log_audit_event`` always passes ``schema="iam"``
   to INSERT_AUDIT_EVENT — the ``schema`` parameter was removed from the
   public signature to make the invariant impossible to violate at the
   call site.

2. ``IamMiddleware._emit_audit`` no longer accepts or forwards a tenant
   schema, and the deny-path call site encodes ``catalog_id`` inside the
   ``detail`` dict instead.
"""

from __future__ import annotations

import asyncio
import inspect
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# PostgresIamStorage.log_audit_event — schema parameter removed
# ---------------------------------------------------------------------------

class TestLogAuditEventSchemaEnforcement:
    """log_audit_event must not accept a caller-supplied schema."""

    def test_schema_parameter_absent_from_signature(self) -> None:
        """The public signature must not expose a ``schema`` parameter."""
        from dynastore.modules.iam.postgres_iam_storage import PostgresIamStorage
        sig = inspect.signature(PostgresIamStorage.log_audit_event)
        assert "schema" not in sig.parameters, (
            "log_audit_event must not have a 'schema' parameter — "
            "audit_log is platform-only (iam schema hardcoded in the body)"
        )

    @pytest.mark.asyncio
    async def test_insert_receives_iam_schema(self) -> None:
        """The INSERT query is always invoked with schema='iam'."""
        from dynastore.modules.iam import postgres_iam_storage as mod

        captured: list[dict] = []

        async def _fake_execute(db: Any, **kwargs: Any) -> None:
            captured.append(kwargs)

        fake_query = MagicMock()
        fake_query.execute = _fake_execute

        # Minimal stub — skip real __init__ (needs DB engine)
        storage = object.__new__(mod.PostgresIamStorage)
        storage.engine = AsyncMock()  # type: ignore[attr-defined]

        with patch.object(mod, "INSERT_AUDIT_EVENT", fake_query), \
             patch.object(mod, "managed_transaction") as mock_ctx:
            # managed_transaction is an async context manager
            mock_ctx.return_value.__aenter__ = AsyncMock(return_value=AsyncMock())
            mock_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

            await storage.log_audit_event(  # type: ignore[attr-defined]
                event_type="authz_denied",
                principal_id="test-principal",
                ip_address="127.0.0.1",
                detail={"path": "/items"},
            )

        assert len(captured) == 1, "INSERT_AUDIT_EVENT.execute must be called once"
        assert captured[0]["schema"] == "iam", (
            f"Expected schema='iam', got {captured[0]['schema']!r}"
        )


# ---------------------------------------------------------------------------
# IamMiddleware._emit_audit — no schema forwarding; catalog_id in detail
# ---------------------------------------------------------------------------

class TestEmitAuditSignatureAndCallSite:
    """_emit_audit must not accept a schema and must forward catalog_id in detail."""

    def test_emit_audit_signature_has_no_schema(self) -> None:
        """The _emit_audit method must not expose a ``schema`` parameter."""
        from dynastore.extensions.iam.middleware import IamMiddleware
        sig = inspect.signature(IamMiddleware._emit_audit)
        assert "schema" not in sig.parameters, (
            "_emit_audit must not accept a schema parameter"
        )

    def test_emit_audit_includes_catalog_id_in_task(self) -> None:
        """The detail dict forwarded to log_audit_event must contain catalog_id."""
        from dynastore.extensions.iam import middleware as mw_mod

        logged_kwargs: list[dict] = []

        async def _fake_log(**kwargs: Any) -> None:
            logged_kwargs.append(kwargs)

        fake_storage = SimpleNamespace(log_audit_event=_fake_log)
        fake_manager = SimpleNamespace(storage=fake_storage)

        mw = object.__new__(mw_mod.IamMiddleware)
        mw._iam_manager = fake_manager  # type: ignore[attr-defined]

        async def _run() -> None:
            mw._emit_audit(  # type: ignore[attr-defined]
                "authz_denied",
                "principal-uuid",
                "10.0.0.1",
                {"path": "/items", "method": "GET", "reason": "DENY", "catalog_id": "cat42"},
            )
            # Allow the created task to run
            await asyncio.sleep(0)

        asyncio.run(_run())

        assert len(logged_kwargs) == 1
        detail = logged_kwargs[0].get("detail", {})
        assert "catalog_id" in detail, (
            "detail must include catalog_id so the tenant context is preserved "
            "without routing to a tenant schema"
        )
        assert detail["catalog_id"] == "cat42"
        # Crucially: no schema kwarg must reach log_audit_event
        assert "schema" not in logged_kwargs[0], (
            "log_audit_event must not receive a schema kwarg from _emit_audit"
        )
