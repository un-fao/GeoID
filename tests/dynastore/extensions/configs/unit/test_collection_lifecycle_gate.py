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

"""Regression tests for the collection lifecycle write-gate on the configs surface.

Pins that the three collection-scoped mutating handlers
(``_patch_collection_config``, ``update_collection_config``,
``delete_collection_config``) refuse writes when the collection is dead
before any config driver is reached.

All tests are pure unit: no live DB or FastAPI app required.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.extensions.configs import service as configs_svc_module
from dynastore.extensions.configs.service import ConfigsService
from dynastore.modules.catalog.collection_service import CollectionNotAliveError


# ---------------------------------------------------------------------------
# Scaffolding
# ---------------------------------------------------------------------------


def _make_service() -> ConfigsService:
    """Return a bare ConfigsService instance without triggering setup_routes."""
    from fastapi import FastAPI

    svc = ConfigsService.__new__(ConfigsService)
    svc.app = FastAPI()
    return svc


# ---------------------------------------------------------------------------
# delete_collection_config — blocked on tombstoned collection
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_collection_config_blocked_when_tombstoned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``require_collection_ready`` fires before ``configs.delete_config``."""
    dead_error = CollectionNotAliveError("cat1", "col1", "tombstoned")
    monkeypatch.setattr(
        configs_svc_module,
        "require_collection_ready",
        AsyncMock(side_effect=dead_error),
    )

    configs_proto = AsyncMock()
    configs_proto.delete_config = AsyncMock()

    svc = _make_service()
    # Inject configs protocol so the handler would reach delete_config if the
    # gate were absent.
    type(svc).configs = property(lambda self: configs_proto)  # type: ignore[method-assign]

    # We must also stub ``require_config_class`` so it doesn't blow up before
    # the gate fires; patch it in the configs service module namespace.
    with patch(
        "dynastore.extensions.configs.service.require_config_class",
        return_value=MagicMock(),
    ):
        with pytest.raises(CollectionNotAliveError) as excinfo:
            await svc.delete_collection_config("cat1", "col1", "some_plugin")

    assert excinfo.value.reason == "tombstoned"
    # The driver was never called.
    configs_proto.delete_config.assert_not_awaited()


@pytest.mark.asyncio
async def test_delete_collection_config_allowed_when_alive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the collection is alive, ``delete_config`` is reached and called."""
    monkeypatch.setattr(
        configs_svc_module,
        "require_collection_ready",
        AsyncMock(return_value=None),
    )
    # Also stub _invalidate_exposure so it doesn't interact with app state.
    monkeypatch.setattr(
        ConfigsService,
        "_invalidate_exposure",
        AsyncMock(return_value=None),
    )

    configs_proto = AsyncMock()
    configs_proto.delete_config = AsyncMock()

    svc = _make_service()
    type(svc).configs = property(lambda self: configs_proto)  # type: ignore[method-assign]

    fake_cls = MagicMock()
    fake_cls.__mro__ = [object]  # makes issubclass(fake_cls, EngineConfig) False

    with patch(
        "dynastore.extensions.configs.service.require_config_class",
        return_value=fake_cls,
    ):
        with patch.object(
            ConfigsService,
            "_reject_engine_write_at_tenant_scope",
            return_value=None,
        ):
            from fastapi.responses import Response

            result = await svc.delete_collection_config("cat1", "col1", "some_plugin")

    # The driver was reached.
    configs_proto.delete_config.assert_awaited_once()
    assert isinstance(result, Response)


# ---------------------------------------------------------------------------
# update_collection_config — gate fires on the configure-existing path only
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_collection_config_blocked_when_tombstoned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """On the default (``create_if_missing=False``) path the liveness gate
    blocks a write to a tombstoned collection before any persistence."""
    monkeypatch.setattr(
        configs_svc_module,
        "require_catalog_ready",
        AsyncMock(return_value=None),
    )
    dead_error = CollectionNotAliveError("cat1", "col1", "tombstoned")
    monkeypatch.setattr(
        configs_svc_module,
        "require_collection_ready",
        AsyncMock(side_effect=dead_error),
    )

    configs_proto = AsyncMock()
    configs_proto.set_config = AsyncMock()
    configs_proto.get_persisted_config = AsyncMock(return_value=None)

    svc = _make_service()
    type(svc).configs = property(lambda self: configs_proto)  # type: ignore[method-assign]

    with pytest.raises(CollectionNotAliveError) as excinfo:
        await svc.update_collection_config(
            "cat1", "col1", "some_plugin", body={}, create_if_missing=False
        )

    assert excinfo.value.reason == "tombstoned"
    configs_proto.set_config.assert_not_awaited()


@pytest.mark.asyncio
async def test_update_collection_config_create_if_missing_skips_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``create_if_missing=True`` is the explicit upfront-configure flow that
    JIT-creates the registry row, so the liveness gate MUST NOT run — a missing
    collection there is expected, not a write to a gone collection."""
    monkeypatch.setattr(
        configs_svc_module,
        "require_catalog_ready",
        AsyncMock(return_value=None),
    )
    gate = AsyncMock(side_effect=CollectionNotAliveError("cat1", "col1", "missing"))
    monkeypatch.setattr(configs_svc_module, "require_collection_ready", gate)
    monkeypatch.setattr(
        ConfigsService, "_invalidate_exposure", AsyncMock(return_value=None)
    )
    monkeypatch.setattr(
        ConfigsService,
        "_strip_response_envelopes",
        staticmethod(lambda body: dict(body)),
    )
    import dynastore.modules.storage.routing_config as _routing_config

    monkeypatch.setattr(
        _routing_config, "_compute_changed_op_keys", lambda *a, **k: set()
    )

    configs_proto = AsyncMock()
    configs_proto.get_persisted_config = AsyncMock(return_value=None)
    configs_proto.set_config = AsyncMock(return_value={"ok": True})

    svc = _make_service()
    type(svc).configs = property(lambda self: configs_proto)  # type: ignore[method-assign]

    fake_cls = MagicMock()
    fake_cls.model_validate = MagicMock(return_value=MagicMock())

    with patch(
        "dynastore.extensions.configs.service.require_config_class",
        return_value=fake_cls,
    ):
        with patch.object(
            ConfigsService, "_reject_engine_write_at_tenant_scope", return_value=None
        ):
            result = await svc.update_collection_config(
                "cat1", "col1", "some_plugin", body={}, create_if_missing=True
            )

    # The gate was bypassed and the write reached the persistence layer.
    gate.assert_not_awaited()
    configs_proto.set_config.assert_awaited_once()
    assert result == {"ok": True}
