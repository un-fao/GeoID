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

import pytest
from fastapi import FastAPI, APIRouter, Depends
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, MagicMock

from dynastore.extensions.tools.exposure_matrix import ExposureSnapshot
from dynastore.extensions.tools.exposure_route import make_exposure_dependency


@pytest.fixture
def app_with_gated_router():
    matrix = MagicMock()
    matrix.get = AsyncMock()
    router = APIRouter()

    @router.get("/tiles/{catalog_id}/ping", tags=["tiles"])
    async def ping(catalog_id: str):
        return {"ok": True, "catalog_id": catalog_id}

    app = FastAPI()
    app.include_router(
        router, dependencies=[Depends(make_exposure_dependency(matrix, "tiles"))]
    )
    return app, matrix


def test_allows_when_enabled(app_with_gated_router):
    app, matrix = app_with_gated_router
    matrix.get.return_value = ExposureSnapshot(
        platform={"tiles": True}, catalogs={}, loaded_at=0.0
    )
    r = TestClient(app).get("/tiles/catA/ping")
    assert r.status_code == 200


def test_503_when_platform_disabled(app_with_gated_router):
    app, matrix = app_with_gated_router
    matrix.get.return_value = ExposureSnapshot(
        platform={"tiles": False}, catalogs={}, loaded_at=0.0
    )
    r = TestClient(app).get("/tiles/catA/ping")
    assert r.status_code == 503
    assert "disabled on this platform" in r.json()["detail"]


def test_503_when_catalog_disabled_only(app_with_gated_router):
    app, matrix = app_with_gated_router
    matrix.get.return_value = ExposureSnapshot(
        platform={"tiles": True},
        catalogs={"catA": {"tiles": False}},
        loaded_at=0.0,
    )
    assert TestClient(app).get("/tiles/catA/ping").status_code == 503
    assert TestClient(app).get("/tiles/catB/ping").status_code == 200
