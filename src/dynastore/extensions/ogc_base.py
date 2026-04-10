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

"""Optional mixin providing shared infrastructure for OGC-protocol extensions.

``ExtensionProtocol`` is the universal base for *all* DynaStore extensions
(Admin, Auth, GCP, Logs, …).  ``OGCServiceMixin`` is an **opt-in mixin**
that only OGC-specific extensions (Features, STAC, Records, Coverages, EDR,
…) add to their bases.  Non-OGC extensions are unaffected.

Usage::

    class CoveragesService(ExtensionProtocol, OGCServiceMixin):
        conformance_uris = [...]
        prefix = "/coverages"
        protocol_title = "DynaStore OGC API - Coverages"
        protocol_description = "Coverage data access via OGC API"
        ...
"""

import logging
from typing import Optional, List, cast

from fastapi import HTTPException, Request, Response, status

from dynastore.extensions.tools.conformance import register_conformance_uris
from dynastore.extensions.tools.ogc_common_models import Conformance, LandingPage
from dynastore.extensions.tools.url import get_root_url
from dynastore.models.protocols import CatalogsProtocol, ConfigsProtocol
from dynastore.models.shared_models import Link
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)


class OGCServiceMixin:
    """Shared helpers for OGC-protocol extensions.

    Subclasses set the following class attributes:

    * ``conformance_uris: List[str]`` — OGC conformance class URIs
    * ``prefix: str`` — router path prefix (e.g. ``"/features"``)
    * ``protocol_title: str`` — human-readable protocol name
    * ``protocol_description: str`` — one-line description
    """

    # --- Class attributes to be set by subclasses ---
    conformance_uris: List[str] = []
    prefix: str = ""
    protocol_title: str = ""
    protocol_description: str = ""

    # --- Cached protocol references (per-instance) ---
    _ogc_catalogs_protocol: Optional[CatalogsProtocol] = None
    _ogc_configs_protocol: Optional[ConfigsProtocol] = None

    # ------------------------------------------------------------------
    # Conformance registration
    # ------------------------------------------------------------------

    def _register_ogc_conformance(self) -> None:
        """Register this protocol's conformance URIs (call once in __init__)."""
        if self.conformance_uris:
            register_conformance_uris(self.conformance_uris)

    # ------------------------------------------------------------------
    # Lifecycle helpers
    # ------------------------------------------------------------------

    def register_policies(self) -> None:
        """Override in subclass to register IAM policies.  Default: no-op."""

    # ------------------------------------------------------------------
    # Protocol getters (cached, with standard error handling)
    # ------------------------------------------------------------------

    async def _get_catalogs_service(self) -> CatalogsProtocol:
        if self._ogc_catalogs_protocol is None:
            svc = get_protocol(CatalogsProtocol)
            if not svc:
                raise HTTPException(
                    status_code=500, detail="Catalogs service not available."
                )
            self._ogc_catalogs_protocol = svc
        return cast(CatalogsProtocol, self._ogc_catalogs_protocol)

    async def _get_configs_service(self) -> ConfigsProtocol:
        if self._ogc_configs_protocol is None:
            svc = get_protocol(ConfigsProtocol)
            if not svc:
                raise HTTPException(
                    status_code=500, detail="Configs service not available."
                )
            self._ogc_configs_protocol = svc
        return cast(ConfigsProtocol, self._ogc_configs_protocol)

    # ------------------------------------------------------------------
    # Standard OGC endpoint handlers
    # ------------------------------------------------------------------

    async def ogc_conformance_handler(self, request: Request) -> Conformance:
        """Standard conformance endpoint returning this protocol's URIs."""
        return Conformance(conformsTo=self.conformance_uris)

    async def ogc_landing_page_handler(self, request: Request) -> LandingPage:
        """Standard landing page with self, conformance, and service-doc links.

        Override in subclass if the protocol needs a custom landing page
        (e.g. STAC returns a root catalog, not a plain landing page).
        """
        root_url = get_root_url(request)
        return LandingPage(
            title=self.protocol_title,
            description=self.protocol_description,
            links=[
                Link(
                    href=f"{root_url}{self.prefix}/",
                    rel="self",
                    type="application/json",
                    title="This document",
                ),
                Link(
                    href=f"{root_url}{self.prefix}/conformance",
                    rel="conformance",
                    type="application/json",
                    title="Conformance classes",
                ),
                Link(
                    href=f"{root_url}/api",
                    rel="service-doc",
                    type="application/json",
                    title="API documentation",
                ),
            ],
        )

    # ------------------------------------------------------------------
    # Shared CRUD helpers
    # ------------------------------------------------------------------

    async def _delete_item(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        db_resource,
    ) -> Response:
        """Shared item deletion: delete + 404 check + 204 response.

        The caller is responsible for transaction management (e.g.
        ``managed_transaction``) — this mixin stays decoupled from
        ``modules.db_config``.
        """
        catalogs_svc = await self._get_catalogs_service()
        rows_affected = await catalogs_svc.delete_item(
            catalog_id, collection_id, item_id, db_resource=db_resource
        )
        if rows_affected == 0:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Item '{item_id}' not found.",
            )
        return Response(status_code=status.HTTP_204_NO_CONTENT)
