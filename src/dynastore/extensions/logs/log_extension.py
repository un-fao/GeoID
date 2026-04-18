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
import json
import os
from typing import Optional, List, Dict, Any
from datetime import datetime
from contextlib import asynccontextmanager
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    ResultHandler,
    managed_transaction,
    DbResource,
)
from dynastore.modules.catalog.catalog_module import register_event_listener
from dynastore.models.shared_models import SYSTEM_CATALOG_ID, SYSTEM_LOGS_TABLE
from dynastore.modules.catalog.log_manager import log_event
from fastapi import APIRouter, HTTPException, Query
from .models import LogEntryCreate, LogEntry, LogsListResponse
from dynastore.modules.catalog.event_service import CatalogEventType
from dynastore.models.protocols.catalogs import CatalogsProtocol
from dynastore.models.protocols.database import DatabaseProtocol
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)


def _kibana_url() -> Optional[str]:
    """Build Kibana dashboard URL if KIBANA_URL is set and ES client is active."""
    from dynastore.modules.elasticsearch.client import get_client, get_index_prefix
    from dynastore.modules.elasticsearch.mappings import get_log_index_name

    base = os.environ.get("KIBANA_URL", "").rstrip("/")
    if not base or get_client() is None:
        return None
    index = get_log_index_name(get_index_prefix())
    return f"{base}#/discover?_g=(filters:!(),refreshInterval:(pause:!t),time:(from:now-24h,to:now))&_a=(index:'{index}')"


from dynastore.models.protocols.logs import LogsProtocol
class LogExtension(ExtensionProtocol, LogsProtocol):
    priority: int = 100
    engine: Optional[DbResource] = None

    def __init__(self, app: Any = None):
        self.app = app
        self.router = APIRouter(prefix="/logs", tags=["Logs"])
        self._setup_routes()

    def _setup_routes(self):
        self.router.add_api_route(
            "/system",
            self.get_system_logs,
            methods=["GET"],
            response_model=LogsListResponse,
            summary="Retrieve global system-level logs",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}",
            self.get_catalog_logs,
            methods=["GET"],
            response_model=LogsListResponse,
            summary="Retrieve logs for a specific catalog",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/logs",
            self.get_catalog_logs,
            methods=["GET"],
            response_model=LogsListResponse,
            summary="Retrieve logs for a specific catalog",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/logs",
            self.get_collection_logs,
            methods=["GET"],
            response_model=LogsListResponse,
            summary="Retrieve logs for a specific collection",
        )

    @property
    def catalogs(self) -> CatalogsProtocol:
        return self.get_protocol(CatalogsProtocol)  # type: ignore[return-value]

    @property
    def database(self) -> DatabaseProtocol:
        return self.get_protocol(DatabaseProtocol)  # type: ignore[return-value]

    @asynccontextmanager
    async def lifespan(self, app: Any):
        db = self.database
        if db:
            self.engine = db.engine

        if not self.engine:
            logger.warning(
                "LogExtension: No DB engine found via DatabaseProtocol. Extension disabled."
            )
            yield
            return

        # We rely on CatalogModule for schema management.

        self._register_listeners()
        logger.info("LogExtension initialized.")
        yield

    def _register_listeners(self):

        register_event_listener(
            CatalogEventType.CATALOG_CREATION, self._on_catalog_created
        )
        register_event_listener(
            CatalogEventType.CATALOG_DELETION, self._on_catalog_deleted
        )
        register_event_listener(
            CatalogEventType.CATALOG_HARD_DELETION, self._on_catalog_hard_deleted
        )
        register_event_listener(
            CatalogEventType.CATALOG_HARD_DELETION_FAILURE, self._on_catalog_failure
        )

    async def _on_catalog_created(self, catalog_id: str, **kwargs):
        db_resource = kwargs.pop("db_resource", None)
        await self.append_log(
            LogEntryCreate(
                catalog_id=catalog_id,
                event_type=CatalogEventType.CATALOG_CREATION.value,
                message="Catalog created.",
                details=kwargs,
                is_system=True,
            ),
            db_resource=db_resource,
        )

    async def _on_catalog_deleted(self, catalog_id: str, **kwargs):
        db_resource = kwargs.pop("db_resource", None)
        await self.append_log(
            LogEntryCreate(
                catalog_id=catalog_id,
                event_type=CatalogEventType.CATALOG_DELETION.value,
                message="Catalog soft-deleted.",
                details=kwargs,
                is_system=True,
            ),
            db_resource=db_resource,
        )

    async def _on_catalog_hard_deleted(self, catalog_id: str, **kwargs):
        # This goes to System Logs as the schema might be dropping/dropped
        db_resource = kwargs.pop("db_resource", None)
        await self.append_log(
            LogEntryCreate(
                catalog_id=catalog_id,
                event_type=CatalogEventType.CATALOG_HARD_DELETION.value,
                message="Catalog hard-deleted (schema dropped).",
                details=kwargs,
                is_system=True,
            ),
            db_resource=db_resource,
        )

    async def _on_catalog_failure(self, catalog_id: str, error: Optional[str] = None, **kwargs):
        # We route this via system catalog for global logging,
        # but preserve the actual failing catalog ID for LogService to extract.
        db_resource = kwargs.pop("db_resource", None)
        await self.append_log(
            LogEntryCreate(
                catalog_id=catalog_id,
                event_type=CatalogEventType.CATALOG_HARD_DELETION_FAILURE.value,
                level="ERROR",
                message=f"Hard deletion failed: {error}",
                details=kwargs,
                is_system=True,
            ),
            db_resource=db_resource,
            immediate=True,
        )

    async def append_log(
        self,
        entry: LogEntryCreate,
        db_resource: Optional[DbResource] = None,
        immediate: bool = False,
    ):
        """Appends a log entry via the catalog module's log manager."""
        await log_event(
            catalog_id=entry.catalog_id,
            event_type=entry.event_type,
            level=entry.level,
            message=entry.message,
            collection_id=entry.collection_id,
            details=entry.details,
            db_resource=db_resource,
            immediate=immediate,
            is_system=entry.is_system,
        )

    async def search_logs(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        event_type: Optional[str] = None,
        level: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[LogEntry]:
        """
        Retrieves logs for a specific catalog (Tenant Logs).
        To view System Logs, pass catalog_id="_system_".

        Harmonization: If searching for a specific tenant, it includes records from
        both tenant logs and system logs associated with that tenant.
        """
        async with managed_transaction(self.engine) as conn:
            # 1. Prepare shared filters
            base_clauses = ["catalog_id = :catalog_id"]
            params = {"catalog_id": catalog_id, "limit": limit, "offset": offset}

            if collection_id:
                base_clauses.append("collection_id = :collection_id")
                params["collection_id"] = collection_id
            if event_type:
                base_clauses.append("event_type = :event_type")
                params["event_type"] = event_type
            if level:
                base_clauses.append("level = :level")
                params["level"] = level
            if start_date:
                base_clauses.append("timestamp >= :start_date")
                params["start_date"] = start_date
            if end_date:
                base_clauses.append("timestamp <= :end_date")
                params["end_date"] = end_date

            where_clause = " AND ".join(base_clauses)

            # 2. Determine tables to query
            queries = []

            # System Logs
            # If asking for _system_, we return ALL system logs.
            # If asking for a tenant, we return system logs matching that tenant.
            sys_where = where_clause
            if catalog_id == SYSTEM_CATALOG_ID:
                sys_where = where_clause.replace("catalog_id = :catalog_id", "TRUE")

            queries.append(
                f"SELECT * FROM catalog.{SYSTEM_LOGS_TABLE} WHERE {sys_where}"
            )

            # Tenant Logs
            if catalog_id != SYSTEM_CATALOG_ID:
                    # 1. Resolve Physical Names
                catalogs_provider = self.catalogs
                if not catalogs_provider:
                    raise HTTPException(
                        status_code=500, detail="CatalogsProtocol not available."
                    )

                physical_schema = await catalogs_provider.resolve_physical_schema(
                    catalog_id
                )
                if not physical_schema:
                    raise HTTPException(
                        status_code=404, detail=f"Catalog '{catalog_id}' not found."
                    )

                if physical_schema:
                    queries.append(
                        f'SELECT * FROM "{physical_schema}".logs WHERE {where_clause}'
                    )

            # 3. Combine queries with UNION ALL and sort
            final_sql = " UNION ALL ".join(queries)
            final_sql = f"SELECT * FROM ({final_sql}) AS combined_logs ORDER BY timestamp DESC LIMIT :limit OFFSET :offset"

            try:
                rows = await DQLQuery(
                    final_sql, result_handler=ResultHandler.ALL_DICTS
                ).execute(conn, **params)
                return [LogEntry.model_validate(r) for r in rows]
            except Exception as e:
                logger.debug(f"Log Search failed: {e}")
                return []


    async def get_system_logs(
        self,
        event_type: Optional[str] = Query(None),
        level: Optional[str] = Query(None),
        limit: int = 100,
        offset: int = 0,
    ) -> LogsListResponse:
        """Retrieve global system-level logs."""
        logs = await self.search_logs(
            catalog_id="_system_",
            event_type=event_type,
            level=level,
            limit=limit,
            offset=offset,
        )
        return LogsListResponse(
            logs=logs,
            kibana_dashboard_url=_kibana_url(),
        )

    async def get_catalog_logs(
        self,
        catalog_id: str,
        event_type: Optional[str] = Query(None),
        level: Optional[str] = Query(None),
        limit: int = 100,
        offset: int = 0,
    ) -> LogsListResponse:
        """
        Retrieve logs for a specific catalog.
        If the catalog has been hard-deleted, this may return final lifecycle events from the system log.
        """
        logs = await self.search_logs(
            catalog_id=catalog_id,
            event_type=event_type,
            level=level,
            limit=limit,
            offset=offset,
        )
        return LogsListResponse(
            logs=logs,
            kibana_dashboard_url=_kibana_url(),
        )

    async def get_collection_logs(
        self,
        catalog_id: str,
        collection_id: str,
        event_type: Optional[str] = Query(None),
        level: Optional[str] = Query(None),
        limit: int = 100,
        offset: int = 0,
    ) -> LogsListResponse:
        """Retrieve logs for a specific collection."""
        logs = await self.search_logs(
            catalog_id=catalog_id,
            collection_id=collection_id,
            event_type=event_type,
            level=level,
            limit=limit,
            offset=offset,
        )
        return LogsListResponse(
            logs=logs,
            kibana_dashboard_url=_kibana_url(),
        )
