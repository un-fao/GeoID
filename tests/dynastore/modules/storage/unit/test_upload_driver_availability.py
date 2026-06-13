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

"""Unit tests for upload-driver availability skipping (GAP 2).

``get_asset_upload_driver`` must not select an upload backend that reports
``upload_available() == False`` merely because it is registered first. An
uninitialised GCP module (no GCS credentials) returns ``False`` here so the
router falls through to the next ready backend (local disk) instead of
selecting GCS and failing every ``initiate_upload``.

``upload_available`` is intentionally distinct from the module-wide
``is_available()`` discovery gate so GCP stays discoverable as a
``StorageProtocol`` provider even when its upload role is unavailable.
"""
from __future__ import annotations

import pytest

from dynastore.modules.storage import router as router_mod


class GCPModule:
    """Stand-in for the GCS upload backend (class name → ``gcp_module``)."""

    def __init__(self, available: bool) -> None:
        self._available = available

    def upload_available(self) -> bool:
        return self._available


class LocalUploadModule:
    """Stand-in for the local-disk backend (class name → ``local_upload_module``).

    No ``upload_available`` on purpose — a missing probe must be treated as
    available by the router.
    """


def _patch_registry(impls, monkeypatch):
    """Patch get_protocols/get_protocol so the router sees ``impls`` in order."""
    monkeypatch.setattr(
        "dynastore.tools.discovery.get_protocols",
        lambda proto: list(impls),
    )
    monkeypatch.setattr(
        "dynastore.tools.discovery.get_protocol",
        lambda proto: impls[0] if impls else None,
    )


@pytest.mark.asyncio
async def test_skips_unavailable_gcp_falls_through_to_local(monkeypatch):
    gcp = GCPModule(available=False)
    local = LocalUploadModule()

    # Routing config order: gcp first, then local (the real auto-augment order).
    async def _ids(*_a, **_k):
        return [
            ("gcp_module", "fatal", "sync"),
            ("local_upload_module", "fatal", "sync"),
        ]

    monkeypatch.setattr(router_mod, "_resolve_driver_ids_cached", _ids)
    _patch_registry([gcp, local], monkeypatch)

    result = await router_mod.get_asset_upload_driver("cat1")
    assert result is local, "Unavailable GCS must be skipped for ready local."


@pytest.mark.asyncio
async def test_picks_gcp_when_available(monkeypatch):
    gcp = GCPModule(available=True)
    local = LocalUploadModule()

    async def _ids(*_a, **_k):
        return [
            ("gcp_module", "fatal", "sync"),
            ("local_upload_module", "fatal", "sync"),
        ]

    monkeypatch.setattr(router_mod, "_resolve_driver_ids_cached", _ids)
    _patch_registry([gcp, local], monkeypatch)

    result = await router_mod.get_asset_upload_driver("cat1")
    assert result is gcp, "Available GCS must still win per config order."


@pytest.mark.asyncio
async def test_fallback_skips_unavailable_first_registered(monkeypatch):
    """No routing entries resolve → first-registered fallback also skips
    an unavailable backend and returns the next available one."""
    gcp = GCPModule(available=False)
    local = LocalUploadModule()

    async def _ids(*_a, **_k):
        return []  # no routing config entries

    monkeypatch.setattr(router_mod, "_resolve_driver_ids_cached", _ids)
    _patch_registry([gcp, local], monkeypatch)

    result = await router_mod.get_asset_upload_driver("cat1")
    assert result is local


@pytest.mark.asyncio
async def test_missing_upload_available_treated_as_available(monkeypatch):
    """A backend with no ``upload_available`` method is treated as available."""
    local = LocalUploadModule()

    async def _ids(*_a, **_k):
        return [("local_upload_module", "fatal", "sync")]

    monkeypatch.setattr(router_mod, "_resolve_driver_ids_cached", _ids)
    _patch_registry([local], monkeypatch)

    result = await router_mod.get_asset_upload_driver("cat1")
    assert result is local
