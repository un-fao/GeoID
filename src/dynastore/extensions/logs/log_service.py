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

"""
REST API endpoints for accessing and querying logs.
Provides endpoints for retrieving individual logs and listing logs with filtering.
"""

import logging
from typing import Optional, List
from contextlib import asynccontextmanager
from fastapi import APIRouter, HTTPException, Query, status, FastAPI, Depends
from dynastore.extensions.iam.guards import require_admin, require_sysadmin
from dynastore.models.protocols.logs import LogsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.extensions.protocols import ExtensionProtocol

logger = logging.getLogger(__name__)

class LogExtensionAPI(ExtensionProtocol):
    priority: int = 100
    """
    Exposes Logs API endpoints.
    """
    router: APIRouter

    def configure_app(self, app: FastAPI):
        pass

    def __init__(self, app: FastAPI):
        super().__init__()
        self.app = app
        self.router = APIRouter(tags=["Logs API"], prefix="/logs")
        self._register_routes()

    def _register_routes(self):
        self.router.add_api_route(
            "/catalogs/{catalog_id}/logs/{log_id}",
            self.get_log,
            methods=["GET"],
            dependencies=[Depends(require_admin)],
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}",
            self.list_catalog_logs,
            methods=["GET"],
            dependencies=[Depends(require_admin)],
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}",
            self.list_collection_logs,
            methods=["GET"],
            dependencies=[Depends(require_admin)],
        )
        self.router.add_api_route(
            "/system",
            self.list_system_logs,
            methods=["GET"],
            dependencies=[Depends(require_sysadmin)],
        )

    @staticmethod
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield

    async def get_log(self, catalog_id: str, log_id: int):
        """
        Retrieve a specific log entry by ID.
        """
        logs_service = get_protocol(LogsProtocol)
        if not logs_service:
            raise HTTPException(status_code=503, detail="LogsProtocol not active.")
        log_entry = await logs_service.get_log_by_id(log_id, catalog_id)
        
        if not log_entry:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Log entry {log_id} not found in catalog {catalog_id}"
            )
        
        return log_entry

    async def list_catalog_logs(
        self,
        catalog_id: str,
        level: Optional[str] = Query(None, description="Filter by log level (ERROR, WARNING, INFO)"),
        event_type: Optional[str] = Query(None, description="Filter by event type"),
        limit: int = Query(50, ge=1, le=1000, description="Maximum number of results"),
        offset: int = Query(0, ge=0, description="Pagination offset")
    ):
        """
        List log entries for a catalog with filtering and pagination.
        """
        logs_service = get_protocol(LogsProtocol)
        if not logs_service:
            raise HTTPException(status_code=503, detail="LogsProtocol not active.")
        logs = await logs_service.list_logs(
            catalog_id=catalog_id,
            level=level,
            event_type=event_type,
            limit=limit,
            offset=offset
        )
        
        return {
            "logs": logs,
            "count": len(logs),
            "limit": limit,
            "offset": offset
        }

    async def list_collection_logs(
        self,
        catalog_id: str,
        collection_id: str,
        level: Optional[str] = Query(None, description="Filter by log level (ERROR, WARNING, INFO)"),
        event_type: Optional[str] = Query(None, description="Filter by event type"),
        limit: int = Query(50, ge=1, le=1000, description="Maximum number of results"),
        offset: int = Query(0, ge=0, description="Pagination offset")
    ):
        """
        List log entries for a specific collection with filtering and pagination.
        """
        logs_service = get_protocol(LogsProtocol)
        if not logs_service:
            raise HTTPException(status_code=503, detail="LogsProtocol not active.")
        logs = await logs_service.list_logs(
            catalog_id=catalog_id,
            collection_id=collection_id,
            level=level,
            event_type=event_type,
            limit=limit,
            offset=offset
        )
        
        return {
            "logs": logs,
            "count": len(logs),
            "limit": limit,
            "offset": offset,
            "collection_id": collection_id
        }

    async def list_system_logs(
        self,
        level: Optional[str] = Query(None, description="Filter by log level (ERROR, WARNING, INFO)"),
        event_type: Optional[str] = Query(None, description="Filter by event type"),
        limit: int = Query(50, ge=1, le=1000, description="Maximum number of results"),
        offset: int = Query(0, ge=0, description="Pagination offset")
    ):
        """
        List system-level log entries with filtering and pagination.
        """
        logs_service = get_protocol(LogsProtocol)
        if not logs_service:
            raise HTTPException(status_code=503, detail="LogsProtocol not active.")
        logs = await logs_service.list_logs(
            catalog_id="_system_",
            level=level,
            event_type=event_type,
            limit=limit,
            offset=offset
        )
        
        return {
            "logs": logs,
            "count": len(logs),
            "limit": limit,
            "offset": offset
        }
