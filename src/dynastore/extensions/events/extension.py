from dynastore.tools.discovery import get_protocol
from dynastore.modules.db_config.query_executor import DbResource
from contextlib import asynccontextmanager
from typing import Optional, Any
import logging
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.models.protocols import DatabaseProtocol
from fastapi import APIRouter, Query, HTTPException


logger = logging.getLogger(__name__)

# Try to import EventsProtocol, create dummy if it doesn't exist yet for registry check
try:
    from dynastore.models.protocols.events import EventsProtocol
except ImportError:
    class EventsProtocol:
        pass


class EventsExtension(ExtensionProtocol, EventsProtocol):
    priority: int = 100
    router = APIRouter(prefix="/events", tags=["Events"])
    
    def __init__(self, app: Optional[Any] = None):
        super().__init__()
        self.app = app
        self._engine: Optional[DbResource] = None
        self._register_routes()

    def _register_routes(self):
        self.router.add_api_route("/system", self.get_system_events, methods=["GET"])
        self.router.add_api_route(
            "/catalogs/{catalog_id}/events", self.get_catalog_events, methods=["GET"]
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/events",
            self.get_collection_events,
            methods=["GET"],
        )

    async def get_system_events(
        self,
        event_type: Optional[str] = Query(None),
        limit: int = 100,
        offset: int = 0,
    ):
        """Retrieve global system-level events."""
        from dynastore.modules.catalog.event_service import event_service
        db = get_protocol(DatabaseProtocol)
        return await event_service.search_events(
            engine=self.engine,
            catalog_id="_system_",
            event_type=event_type,
            limit=limit,
            offset=offset,
        )

    async def get_catalog_events(
        self,
        catalog_id: str,
        event_type: Optional[str] = Query(None),
        limit: int = 100,
        offset: int = 0,
    ):
        """Retrieve events for a specific catalog."""
        from dynastore.modules.catalog.event_service import event_service
        db = get_protocol(DatabaseProtocol)
        return await event_service.search_events(
            engine=self.engine,
            catalog_id=catalog_id,
            event_type=event_type,
            limit=limit,
            offset=offset,
        )

    async def get_collection_events(
        self,
        catalog_id: str,
        collection_id: str,
        event_type: Optional[str] = Query(None),
        limit: int = 100,
        offset: int = 0,
    ):
        """Retrieve events for a specific collection."""
        from dynastore.modules.catalog.event_service import event_service
        db = get_protocol(DatabaseProtocol)
        return await event_service.search_events(
            engine=self.engine,
            catalog_id=catalog_id,
            collection_id=collection_id,
            event_type=event_type,
            limit=limit,
            offset=offset,
        )

    @asynccontextmanager
    async def lifespan(self, app: Any):
        db = get_protocol(DatabaseProtocol)
        if db:
            self.engine = db.engine

        if not self._engine:
            logger.warning(
                "EventsExtension: No DB engine found via DatabaseProtocol. Extension disabled."
            )
            yield
            return

        logger.info("EventsExtension initialized.")
        yield

def get_events_extension():
    ext = get_protocol(EventsProtocol)
    if not ext:
        raise HTTPException(status_code=503, detail="EventsExtension not active.")
    return ext
