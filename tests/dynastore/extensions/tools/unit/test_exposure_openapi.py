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

from fastapi import FastAPI, APIRouter
from unittest.mock import MagicMock

from dynastore.extensions.tools.exposure_openapi import install_filtered_openapi
from dynastore.extensions.tools.exposure_matrix import ExposureSnapshot


def _make_app():
    app = FastAPI()
    tiles = APIRouter()
    @tiles.get("/tiles/ping", tags=["tiles"])
    async def tp(): return {}
    stac = APIRouter()
    @stac.get("/stac/ping", tags=["stac"])
    async def sp(): return {}
    app.include_router(tiles)
    app.include_router(stac)
    # ``install_filtered_openapi`` filters by PATH PREFIX, not by tag
    # (tag-based filtering would break routes that share tags across
    # extensions).  Production populates ``app.state.extension_prefixes``
    # during extension registration; the test fixture must seed the
    # same map, otherwise the filter has no prefix list to consult
    # and nothing gets filtered.
    app.state.extension_prefixes = [("/tiles", "tiles"), ("/stac", "stac")]
    return app


def test_filters_disabled_platform_tags():
    app = _make_app()
    matrix = MagicMock()
    matrix.get_sync = MagicMock(return_value=ExposureSnapshot(
        platform={"tiles": False, "stac": True}, catalogs={}, loaded_at=0.0))
    install_filtered_openapi(app, matrix)
    schema = app.openapi()
    assert "/stac/ping" in schema["paths"]
    assert "/tiles/ping" not in schema["paths"]


def test_keeps_all_when_nothing_disabled():
    app = _make_app()
    matrix = MagicMock()
    matrix.get_sync = MagicMock(return_value=ExposureSnapshot(
        platform={"tiles": True, "stac": True}, catalogs={}, loaded_at=0.0))
    install_filtered_openapi(app, matrix)
    schema = app.openapi()
    assert "/stac/ping" in schema["paths"]
    assert "/tiles/ping" in schema["paths"]
