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

"""OGC API - 3D GeoVolumes extension for DynaStore.

Implements the OGCServiceMixin architecture for 3D GeoVolumes support,
including CityJSON content negotiation. Routes are registered in a later
task; this module provides the package skeleton and conformance declaration.
"""

import logging
from contextlib import asynccontextmanager
from typing import Optional

import cjio as _cjio_scope_gate  # noqa: F401  # SCOPE gate: extension_geovolumes requires cjio
import pyproj as _pyproj_scope_gate  # noqa: F401  # SCOPE gate: extension_geovolumes requires pyproj
_ = (_cjio_scope_gate, _pyproj_scope_gate)  # silence pyright "unused" — load-bearing for SCOPE filtering

from fastapi import APIRouter, FastAPI

from dynastore.extensions.ogc_base import OGCServiceMixin
from dynastore.extensions.protocols import ExtensionProtocol

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# OGC API - 3D GeoVolumes conformance URIs
# ---------------------------------------------------------------------------

OGC_API_GEOVOLUMES_URIS = [
    "http://www.opengis.net/spec/ogcapi-geovolumes-1/1.0/conf/core",
    "http://www.opengis.net/spec/ogcapi-geovolumes-1/1.0/conf/spatialquery",
]


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class GeoVolumesService(ExtensionProtocol, OGCServiceMixin):
    """OGC API - 3D GeoVolumes extension.

    Priority 175 — after Coverages (160), EDR (165), DGGS (170); before Joins (180).
    """

    priority: int = 175
    router: APIRouter

    # OGCServiceMixin class attributes
    conformance_uris = OGC_API_GEOVOLUMES_URIS
    prefix = "/geovolumes"
    protocol_title = "DynaStore OGC API - 3D GeoVolumes"
    protocol_description = "Access to 3D geospatial volumes and CityJSON data via OGC API - 3D GeoVolumes"

    def __init__(self, app: Optional[FastAPI] = None):
        super().__init__()
        self.app = app
        self.router = APIRouter(prefix="/geovolumes", tags=["OGC API - 3D GeoVolumes"])
        self._register_routes()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        logger.info("GeoVolumesService: policies registered.")
        yield

    # ------------------------------------------------------------------
    # Route registration
    # ------------------------------------------------------------------

    def _register_routes(self) -> None:
        pass
