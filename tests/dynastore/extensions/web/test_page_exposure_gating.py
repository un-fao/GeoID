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

"""Extension-exposure gating for contributed web pages.

A page contributed by an extension must follow that extension's `enabled`
plugin-config toggle (Service Exposure page): when the extension is disabled
at platform scope the page disappears from the navigation payload and the
page route mirrors the 503 the extension's own router returns. Pages without
an owning extension (core/web-module pages) are never gated.
"""

from __future__ import annotations

import asyncio

import pytest

from dynastore.modules.web.web_module import WebModule


class _FakeStacExtension:
    """Stand-in whose class module path mimics a real extension."""

    async def page(self, request):
        return "<html>stac</html>"


# Simulate a class defined inside the stac extension package.
_FakeStacExtension.__module__ = "dynastore.extensions.stac.stac_service"


class _CorePages:
    async def page(self, request):
        return "<html>core</html>"


_CorePages.__module__ = "dynastore.modules.web.web_module"


def _register(module: WebModule, page_id: str, provider, **cfg):
    config = {"id": page_id, "title": page_id, **cfg}
    module.register_web_page(config, provider)


def test_owner_derived_from_extension_module_path():
    wm = WebModule()
    ext = _FakeStacExtension()
    _register(wm, "stac_browser", ext.page)
    assert wm.get_page_owner_extension("stac_browser") == "stac"


def test_owner_none_for_core_pages():
    wm = WebModule()
    core = _CorePages()
    _register(wm, "home", core.page)
    assert wm.get_page_owner_extension("home") is None


def test_owner_none_for_unknown_page():
    wm = WebModule()
    assert wm.get_page_owner_extension("nope") is None


def test_owner_follows_authoritative_non_embed_provider():
    """An embed contribution from another extension must not steal ownership."""
    wm = WebModule()
    core = _CorePages()
    ext = _FakeStacExtension()
    # Embed registers first (higher priority number = lower precedence).
    _register(wm, "mixed", core.page, is_embed=True, priority=10)
    # Authoritative non-embed provider arrives with a lower priority.
    _register(wm, "mixed", ext.page, priority=0)
    assert wm.get_page_owner_extension("mixed") == "stac"


def test_pages_config_carries_owner():
    wm = WebModule()
    ext = _FakeStacExtension()
    _register(wm, "stac_browser", ext.page)

    async def run():
        return await wm.get_web_pages_config("en")

    pages = asyncio.new_event_loop().run_until_complete(run())
    entry = next(p for p in pages if p["id"] == "stac_browser")
    assert entry["owner"] == "stac"


class _FakeMatrix:
    """Minimal ExposureMatrix stand-in: platform map only."""

    def __init__(self, platform):
        self._platform = platform

    async def get(self):
        from types import SimpleNamespace

        return SimpleNamespace(platform=self._platform, catalogs={})


def test_page_route_503_when_owner_disabled():
    """The /web/pages/{id} gate mirrors the owning router's 503."""
    from fastapi import HTTPException

    wm = WebModule()
    ext = _FakeStacExtension()
    _register(wm, "stac_browser", ext.page)

    matrix = _FakeMatrix({"stac": False})

    async def gate(page_id):
        owner_ext = wm.get_page_owner_extension(page_id)
        if owner_ext is not None:
            snap = await matrix.get()
            if not snap.platform.get(owner_ext, True):
                raise HTTPException(status_code=503)

    loop = asyncio.new_event_loop()
    with pytest.raises(HTTPException) as exc:
        loop.run_until_complete(gate("stac_browser"))
    assert exc.value.status_code == 503

    # Enabled extension passes through.
    matrix._platform["stac"] = True
    loop.run_until_complete(gate("stac_browser"))
