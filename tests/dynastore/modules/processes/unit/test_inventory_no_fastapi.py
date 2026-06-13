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

"""Regression tests confirming Tier-C FastAPI coupling is removed from
the core processes/inventory and web/web_module modules.

These tests are framework-free: they must pass with no FastAPI installed.
"""
from __future__ import annotations

import asyncio
import inspect
import sys
import types


def test_inventory_has_no_request_parameter():
    """build_process_inventory_entries must not accept a `request` parameter."""
    from dynastore.modules.processes.inventory import build_process_inventory_entries

    sig = inspect.signature(build_process_inventory_entries)
    assert "request" not in sig.parameters, (
        "build_process_inventory_entries still has a `request` parameter — "
        "FastAPI coupling not fully removed."
    )


def test_inventory_module_does_not_import_fastapi():
    """The inventory module must import without touching fastapi."""
    # Force reimport from scratch to detect any top-level fastapi import.
    mod_name = "dynastore.modules.processes.inventory"
    # Remove cached module so we exercise the import path.
    saved = sys.modules.pop(mod_name, None)
    fastapi_saved = sys.modules.get("fastapi")

    # Temporarily mask fastapi so an accidental import would raise.
    sentinel = types.ModuleType("fastapi")

    def _raise(*a, **kw):  # noqa: ANN202
        raise ImportError("fastapi must not be imported by core modules")

    sentinel.__getattr__ = _raise  # type: ignore[attr-defined]

    sys.modules["fastapi"] = sentinel
    try:
        import importlib
        importlib.import_module(mod_name)
    finally:
        # Restore originals regardless of outcome.
        if saved is not None:
            sys.modules[mod_name] = saved
        else:
            sys.modules.pop(mod_name, None)
        if fastapi_saved is not None:
            sys.modules["fastapi"] = fastapi_saved
        else:
            sys.modules.pop("fastapi", None)


def test_web_module_get_page_content_duck_types_response_body():
    """get_web_page_content must decode a .body attribute without isinstance(x, Response)."""
    from dynastore.modules.web.web_module import WebModule

    class FakeResponseWithBody:
        """Minimal duck-type stand-in for a FastAPI/Starlette Response."""
        body = b"hello from body"

    wm = WebModule()
    page_id = "test-duck-type"
    wm.web_pages[page_id] = {
        "config": type("Cfg", (), {"enabled": True, "is_embed": False})(),
        "providers": [
            (0, lambda: FakeResponseWithBody(), False),
        ],
    }

    result = asyncio.get_event_loop().run_until_complete(
        wm.get_web_page_content(page_id, request=None)
    )
    assert result == "hello from body", (
        f"Expected 'hello from body', got {result!r}"
    )


def test_web_module_get_page_content_handles_memoryview_body():
    """get_web_page_content decodes memoryview .body correctly."""
    from dynastore.modules.web.web_module import WebModule

    class FakeResponseWithMemoryViewBody:
        body = memoryview(b"memoryview body")

    wm = WebModule()
    page_id = "test-memoryview"
    wm.web_pages[page_id] = {
        "config": type("Cfg", (), {"enabled": True, "is_embed": False})(),
        "providers": [
            (0, lambda: FakeResponseWithMemoryViewBody(), False),
        ],
    }

    result = asyncio.get_event_loop().run_until_complete(
        wm.get_web_page_content(page_id, request=None)
    )
    assert result == "memoryview body"


def test_web_module_get_page_content_falls_back_to_str():
    """Plain return values (str/dict/etc.) go through str() unchanged."""
    from dynastore.modules.web.web_module import WebModule

    wm = WebModule()
    page_id = "test-str-fallback"
    wm.web_pages[page_id] = {
        "config": type("Cfg", (), {"enabled": True, "is_embed": False})(),
        "providers": [
            (0, lambda: "plain string content", False),
        ],
    }

    result = asyncio.get_event_loop().run_until_complete(
        wm.get_web_page_content(page_id, request=None)
    )
    assert result == "plain string content"
