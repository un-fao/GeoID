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
    # extensions).  Production code populates
    # ``app.state.extension_prefixes`` during extension registration;
    # the test fixture must seed the same map so the filter has
    # something to match against.  Without it, ``prefixes=[]`` →
    # every path maps to owner=None → nothing gets filtered.
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
