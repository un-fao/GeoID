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

"""Unit tests for IamExtension.lifespan graceful-degrade behavior.

When ``dynastore-ext-iam`` is installed in a SCOPE that lacks
``module_iam`` (deployment/packaging mismatch), the extension's
prerequisites (IamService, PermissionProtocol, DatabaseProtocol) are
absent. The extension must log a clear warning and yield cleanly
rather than hard-crashing the whole boot — the middleware already
runs in pass-through mode under the same conditions.
"""

from __future__ import annotations

import pytest

from dynastore.extensions.iam.service import IamExtension


@pytest.mark.asyncio
async def test_lifespan_yields_cleanly_when_iam_service_missing(monkeypatch, caplog):
    """No IamService → log warning + yield (no exception)."""
    import dynastore.extensions.iam.service as svc_mod

    monkeypatch.setattr(svc_mod, "get_protocol", lambda proto: None)

    ext = IamExtension()
    with caplog.at_level("WARNING", logger="dynastore.extensions.iam.service"):
        async with ext.lifespan(app=None):  # type: ignore[arg-type]
            pass

    msgs = [r.getMessage() for r in caplog.records]
    assert any("pass-through mode" in m for m in msgs)
    assert any("IamService" in m for m in msgs)


@pytest.mark.asyncio
async def test_lifespan_yields_cleanly_when_db_missing(monkeypatch, caplog):
    """IamService + PermissionProtocol present, DB missing → still pass-through."""
    import dynastore.extensions.iam.service as svc_mod

    class _Stub:
        pass

    def fake_get_protocol(proto):
        # Return a stub for IamService and PermissionProtocol, None for DatabaseProtocol.
        name = getattr(proto, "__name__", str(proto))
        if name == "DatabaseProtocol":
            return None
        return _Stub()

    monkeypatch.setattr(svc_mod, "get_protocol", fake_get_protocol)

    ext = IamExtension()
    with caplog.at_level("WARNING", logger="dynastore.extensions.iam.service"):
        async with ext.lifespan(app=None):  # type: ignore[arg-type]
            pass

    msgs = [r.getMessage() for r in caplog.records]
    assert any("DatabaseProtocol" in m and "pass-through mode" in m for m in msgs)
