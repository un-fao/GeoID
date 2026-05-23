#    Copyright 2025 FAO
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

"""Unit tests for the handle_or_raise() service-handler convenience wrapper."""

import pytest
from fastapi import HTTPException, Response

from dynastore.extensions.tools import exception_handlers
from dynastore.extensions.tools.exception_handlers import handle_or_raise


def test_raises_when_handle_exception_returns_httpexception(monkeypatch):
    """An HTTPException result must be raised, not returned."""
    monkeypatch.setattr(
        exception_handlers,
        "handle_exception",
        lambda *a, **k: HTTPException(status_code=404, detail="missing"),
    )
    with pytest.raises(HTTPException) as excinfo:
        handle_or_raise(ValueError("boom"), resource_name="Catalog")
    assert excinfo.value.status_code == 404


def test_returns_when_handle_exception_returns_response(monkeypatch):
    """A non-HTTPException Response result must be returned to the caller."""
    response = Response(content=b"structured-error", status_code=207)
    monkeypatch.setattr(
        exception_handlers, "handle_exception", lambda *a, **k: response
    )
    result = handle_or_raise(ValueError("boom"), resource_name="Catalog")
    assert result is response


def test_forwards_context_kwargs_to_handle_exception(monkeypatch):
    """resource_name/resource_id/operation must reach handle_exception verbatim."""
    captured = {}

    def _fake(exception, *, resource_name=None, resource_id=None, operation=None):
        captured["exception"] = exception
        captured["resource_name"] = resource_name
        captured["resource_id"] = resource_id
        captured["operation"] = operation
        return Response(status_code=200)

    monkeypatch.setattr(exception_handlers, "handle_exception", _fake)
    err = ValueError("boom")
    handle_or_raise(
        err, resource_name="Collection", resource_id="cat:col", operation="create"
    )
    assert captured == {
        "exception": err,
        "resource_name": "Collection",
        "resource_id": "cat:col",
        "operation": "create",
    }
