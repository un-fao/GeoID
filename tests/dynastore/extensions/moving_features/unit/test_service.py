"""Unit tests for extensions/moving_features/mf_service.py.

Tests service instantiation, route wiring, OGCServiceMixin, conformance URIs,
and landing page generation — no database required.
"""

import pytest
from unittest.mock import MagicMock

from fastapi import Request

from dynastore.extensions.moving_features.mf_service import (
    OGC_API_MOVING_FEATURES_URIS,
    MovingFeaturesService,
)
from dynastore.models.protocols import MovingFeaturesProtocol


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _build_service() -> MovingFeaturesService:
    return MovingFeaturesService()


def _make_request(url: str = "http://example.com/movingfeatures/") -> Request:
    req = MagicMock(spec=Request)
    req.url = url
    req.headers = MagicMock()
    req.headers.get = MagicMock(return_value="")
    return req


# ---------------------------------------------------------------------------
# Instantiation & route wiring
# ---------------------------------------------------------------------------

def test_service_instantiation():
    svc = _build_service()
    assert svc.prefix == "/movingfeatures"
    assert svc.router is not None


def test_router_prefix():
    svc = _build_service()
    assert svc.router.prefix == "/movingfeatures"


def test_self_app_stored():
    app_mock = MagicMock()
    svc = MovingFeaturesService(app=app_mock)
    assert svc.app is app_mock


def test_conformance_uris_present():
    svc = _build_service()
    assert "http://www.opengis.net/spec/ogcapi-movingfeatures-1/1.0/conf/core" in svc.conformance_uris
    assert "http://www.opengis.net/spec/ogcapi-movingfeatures-1/1.0/conf/mf-collection" in svc.conformance_uris
    assert "http://www.opengis.net/spec/ogcapi-movingfeatures-1/1.0/conf/tgsequence" in svc.conformance_uris


def test_conformance_uris_match_module_constant():
    assert MovingFeaturesService.conformance_uris == OGC_API_MOVING_FEATURES_URIS


def test_routes_registered():
    svc = _build_service()
    paths = {r.path for r in svc.router.routes}
    col = "/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}"

    assert "/movingfeatures/" in paths
    assert "/movingfeatures/conformance" in paths
    assert "/movingfeatures/catalogs/{catalog_id}/collections" in paths
    assert col in paths
    assert col + "/items" in paths
    assert col + "/items/{mf_id}" in paths
    assert col + "/items/{mf_id}/tgsequence" in paths


# ---------------------------------------------------------------------------
# OGCServiceMixin — landing page
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_landing_page_contains_self_link(monkeypatch):
    monkeypatch.setattr(
        "dynastore.extensions.ogc_base.get_root_url",
        lambda req: "http://example.com",
    )
    svc = _build_service()
    req = _make_request()
    page = await svc.get_landing_page(req)
    hrefs = [lnk.href for lnk in page.links]
    assert any("self" in lnk.rel for lnk in page.links)
    assert any("/movingfeatures/" in h for h in hrefs)


@pytest.mark.asyncio
async def test_landing_page_contains_conformance_link(monkeypatch):
    monkeypatch.setattr(
        "dynastore.extensions.ogc_base.get_root_url",
        lambda req: "http://example.com",
    )
    svc = _build_service()
    req = _make_request()
    page = await svc.get_landing_page(req)
    assert any("conformance" in lnk.rel for lnk in page.links)


@pytest.mark.asyncio
async def test_conformance_returns_uris():
    svc = _build_service()
    req = _make_request()
    conf = await svc.get_conformance(req)
    assert set(OGC_API_MOVING_FEATURES_URIS).issubset(set(conf.conformsTo))


# ---------------------------------------------------------------------------
# Protocol compliance
# ---------------------------------------------------------------------------

def test_service_is_moving_features_protocol_instance():
    svc = _build_service()
    assert isinstance(svc, MovingFeaturesProtocol)
