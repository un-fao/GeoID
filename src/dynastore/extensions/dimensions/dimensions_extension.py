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

"""OGC Dimensions extension — exposes dimension generators as REST API.

Wraps the ogc-dimensions package (pip dependency) into a Dynastore
ExtensionProtocol so it can be deployed on the tools Cloud Run service.
"""

import logging

from fastapi import APIRouter, FastAPI

from dynastore.extensions.protocols import ExtensionProtocol

logger = logging.getLogger(__name__)


class DimensionsExtension(ExtensionProtocol):
    priority: int = 200

    def __init__(self, app: FastAPI):
        self.app = app

        from ogc_dimensions.api.routes import router as dimensions_router

        self.router = APIRouter(prefix="/dimensions", tags=["OGC Dimensions"])
        self.router.include_router(dimensions_router)

        logger.info("OGC Dimensions extension loaded — /dimensions endpoints available")
