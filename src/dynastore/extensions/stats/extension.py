
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

import logging
from typing import Optional, Any
from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import APIRouter, HTTPException, Depends, Query
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.models.protocols.database import DatabaseProtocol
from dynastore.models.protocols.catalogs import CatalogsProtocol
from dynastore.modules.db_config.query_executor import DbResource

logger = logging.getLogger(__name__)

# Try to import StatsProtocol, create dummy if it doesn't exist yet for registry check
try:
    from dynastore.models.protocols.stats import StatsProtocol
except ImportError:
    class StatsProtocol:
        pass
class StatsExtension(ExtensionProtocol, StatsProtocol):
    priority: int = 100
    engine: Optional[DbResource] = None

    def __init__(self, app: Any = None):
        self.app = app
        self.router = APIRouter(prefix="/stats", tags=["Stats"])
        self._setup_routes()

    def _setup_routes(self):
        self.router.add_api_route(
            "/system",
            self.get_system_stats,
            methods=["GET"],
            summary="Retrieve global system-level stats",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}",
            self.get_catalog_stats,
            methods=["GET"],
            summary="Retrieve stats for a specific catalog",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}",
            self.get_collection_stats,
            methods=["GET"],
            summary="Retrieve stats for a specific collection",
        )

    @property
    def catalogs(self) -> CatalogsProtocol:
        return self.get_protocol(CatalogsProtocol)

    @property
    def database(self) -> DatabaseProtocol:
        return self.get_protocol(DatabaseProtocol)

    @property
    def stats_service(self) -> StatsProtocol:
        return self.get_protocol(StatsProtocol)

    async def log_access(
        self,
        request: Any,
        status_code: int,
        processing_time_ms: float,
        details=None,
        schema: str = "catalog",
    ) -> None:
        """Delegation to the underlying StatsService."""
        svc = self.stats_service
        if svc and svc is not self:
            await svc.log_access(request, status_code, processing_time_ms, details=details, schema=schema)

    def log_request_completion(
        self,
        request: Any,
        status_code: int,
        processing_time_ms: float,
        details=None,
        catalog_id=None,
    ) -> None:
        """Delegation to the underlying StatsService."""
        svc = self.stats_service
        if svc and svc is not self:
            svc.log_request_completion(
                request, status_code, processing_time_ms,
                details=details, catalog_id=catalog_id,
            )

    @asynccontextmanager
    async def lifespan(self, app: Any):
        db = self.database
        if db:
            self.engine = db.engine

        if not self.engine:
            logger.warning(
                "StatsExtension: No DB engine found via DatabaseProtocol. Extension disabled."
            )
            yield
            return

        logger.info("StatsExtension initialized.")
        yield

    async def _get_stats_summary(
        self,
        catalog_id: str,
        principal_id: Optional[str] = None,
        api_key_hash: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ):
        catalogs = self.catalogs
        
        if not self.stats_service:
            return {"total_requests": 0, "average_latency_ms": 0}

        schema = "catalog"
        if catalog_id and catalog_id != "_system_":
            try:
                schema = await catalogs.resolve_physical_schema(catalog_id) or "catalog"
            except Exception:
                pass

        summary = await self.stats_service.get_summary(
            schema=schema,
            catalog_id=catalog_id if catalog_id != "_system_" else None,
            principal_id=principal_id,
            api_key_hash=api_key_hash,
            start_date=start_date,
            end_date=end_date,
        )
        return summary.model_dump() if summary else {"total_requests": 0, "average_latency_ms": 0}

    async def get_system_stats(
        self,
        principal_id: Optional[str] = Query(None),
        api_key_hash: Optional[str] = Query(None),
        start_date: Optional[datetime] = Query(None),
        end_date: Optional[datetime] = Query(None),
    ):
        """Retrieve global system-level stats."""
        return await self._get_stats_summary(
            catalog_id="_system_",
            principal_id=principal_id,
            api_key_hash=api_key_hash,
            start_date=start_date,
            end_date=end_date,
        )

    async def get_catalog_stats(
        self,
        catalog_id: str,
        principal_id: Optional[str] = Query(None),
        api_key_hash: Optional[str] = Query(None),
        start_date: Optional[datetime] = Query(None),
        end_date: Optional[datetime] = Query(None),
    ):
        """Retrieve stats for a specific catalog."""
        return await self._get_stats_summary(
            catalog_id=catalog_id,
            principal_id=principal_id,
            api_key_hash=api_key_hash,
            start_date=start_date,
            end_date=end_date,
        )

    async def get_collection_stats(
        self,
        catalog_id: str,
        collection_id: str,
        principal_id: Optional[str] = Query(None),
        api_key_hash: Optional[str] = Query(None),
        start_date: Optional[datetime] = Query(None),
        end_date: Optional[datetime] = Query(None),
    ):
        """Retrieve stats for a specific collection."""
        return await self._get_stats_summary(
            catalog_id=catalog_id,
            principal_id=principal_id,
            api_key_hash=api_key_hash,
            start_date=start_date,
            end_date=end_date,
        )

