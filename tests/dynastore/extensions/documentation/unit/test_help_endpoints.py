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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""Unit tests for the documentation help endpoints.

Exercises the rendered-README page served at ``/_help/{name}`` and the help-box
injected by ``enrich_extension_metadata``. No DB, no external HTTP — pure
FastAPI app construction against a temp README on disk.
"""

from fastapi import FastAPI
from fastapi.testclient import TestClient

from dynastore.extensions.documentation.service import (
    _render_help_box,
    setup_global_help_endpoint,
)


def test_render_help_box_contains_marker_and_url():
    html = _render_help_box("/extension-docs/catalog")

    # ``help-box`` is the idempotency marker relied on by enrich_extension_metadata.
    assert 'class="help-box"' in html
    assert 'href="/extension-docs/catalog"' in html
    assert "<b>Documentation</b>" in html


def test_global_help_handler_renders_markdown(tmp_path):
    readme = tmp_path / "readme.md"
    readme.write_text("# Catalog\n\nSome **bold** docs.\n", encoding="utf-8")

    app = FastAPI()
    setup_global_help_endpoint(app)
    app.state.extension_docs_registry["catalog"] = str(readme)

    client = TestClient(app)
    resp = client.get("/_help/catalog")

    assert resp.status_code == 200
    body = resp.text
    # Full-document template markers.
    assert "<!DOCTYPE html>" in body
    assert "<title>Catalog Extension Help</title>" in body
    assert "Back to Main API Docs" in body
    # Rendered markdown body.
    assert "<h1>Catalog</h1>" in body
    assert "<strong>bold</strong>" in body


def test_global_help_handler_missing_doc_returns_404():
    app = FastAPI()
    setup_global_help_endpoint(app)

    client = TestClient(app)
    resp = client.get("/_help/does-not-exist")

    assert resp.status_code == 404
    assert "Documentation not found" in resp.text
