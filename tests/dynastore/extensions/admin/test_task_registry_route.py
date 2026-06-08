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

"""The /admin/task-registry read view serializes registry rows. Auth is
enforced by IamMiddleware policy (covered by the admin policy suite), so this
test pins the handler's serialization path only — no DB or app needed."""
from __future__ import annotations

import pytest

from dynastore.extensions.admin import admin_service


@pytest.mark.asyncio
async def test_list_task_registry_serializes_rows(monkeypatch):
    fake_rows = [
        {"service": "worker", "task_key": "gdal", "kind": "process",
         "mandatory": False, "affinity_tier": None,
         "service_commit": "c1"},
    ]

    async def _fake_list_all(_engine):
        return fake_rows

    monkeypatch.setattr(admin_service, "_registry_list_all", _fake_list_all)
    monkeypatch.setattr(admin_service, "_platform_engine", lambda: object())

    out = await admin_service.list_task_registry()
    assert out == fake_rows


@pytest.mark.asyncio
async def test_list_task_registry_503_when_engine_unavailable(monkeypatch):
    from fastapi import HTTPException

    monkeypatch.setattr(admin_service, "_platform_engine", lambda: None)

    with pytest.raises(HTTPException) as exc:
        await admin_service.list_task_registry()
    assert exc.value.status_code == 503
