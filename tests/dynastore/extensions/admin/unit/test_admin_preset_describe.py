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

"""Unit tests for GET /configs/presets/{name}.

Preset lifecycle endpoints moved from /admin/presets to /configs/presets.
Covers:
  - GET /configs/presets/{name} returns the standard preset detail shape.
  - Unknown preset name returns 404.
  - The stac preset exposes a non-null params_schema.
"""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from dynastore.extensions.configs.service import ConfigsService


def _app() -> FastAPI:
    app = FastAPI()
    svc = ConfigsService(app)
    app.include_router(svc.router)
    return app


# Use "public_catalog" as a known registered preset (always registered by import).


class TestGetPresetDetail:
    def test_get_known_preset_returns_200(self) -> None:
        """GET /configs/presets/{name} returns 200 with the preset detail."""
        client = TestClient(_app())
        resp = client.get("/configs/presets/public_catalog")
        assert resp.status_code == 200
        body = resp.json()
        assert "name" in body
        assert "description" in body
        assert "tier" in body

    def test_get_known_preset_has_catalog_scopable(self) -> None:
        """The catalog_scopable flag is present in the response."""
        client = TestClient(_app())
        resp = client.get("/configs/presets/public_catalog")
        assert resp.status_code == 200
        body = resp.json()
        assert "catalog_scopable" in body

    def test_unknown_preset_returns_404(self) -> None:
        client = TestClient(_app())
        resp = client.get("/configs/presets/does_not_exist_xyz")
        assert resp.status_code == 404

    def test_stac_preset_has_params_schema(self) -> None:
        """stac preset has params — params_schema must be non-null."""
        client = TestClient(_app())
        resp = client.get("/configs/presets/stac")
        assert resp.status_code == 200
        body = resp.json()
        assert body["params_schema"] is not None

    def test_existing_fields_present(self) -> None:
        """The response includes all standard fields."""
        client = TestClient(_app())
        resp = client.get("/configs/presets/public_catalog")
        body = resp.json()
        assert "name" in body
        assert "description" in body
        assert "tier" in body
        assert "keywords" in body
