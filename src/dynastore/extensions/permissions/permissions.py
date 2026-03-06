
from typing import Optional
from dynastore.modules import get_protocol
from dynastore.modules.spanner.spanner_module import SpannerModule
from contextlib import asynccontextmanager
from dynastore.extensions.protocols import ExtensionProtocol

from fastapi import HTTPException, status, APIRouter, FastAPI, Depends, Header, Request, Security, BackgroundTasks, Query
import logging 
logger = logging.getLogger(__name__)


class PermissionsExtension(ExtensionProtocol):
    priority: int = 100
    router: APIRouter
    _spanner_module: Optional[SpannerModule]

    def __init__(self, app: FastAPI):
        logger.info("PermissionExtension: Initializing extension.")
        self.app = app
        self.router = APIRouter(prefix="/permission", tags=["Permissions"])

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        logger.info("PermissionExtension: Initializing lifecycle extension.")
        spanner_module = get_protocol(SpannerModule)
        if not spanner_module:
            # Fail fast if the core dependency is not available.
            # This prevents the application from starting in an invalid state.
            raise RuntimeError("SpannerModule not found. The PermissionExtension cannot operate and will not be loaded.")
        self.spanner_module = spanner_module
        # # # --- REGISTER AS SUBSCRIBER via API Call ---
        # try:
        #     # We run this as a background task so it doesn't block startup
        #     from dynastore.extensions.gcp.gcp_events import register_self_as_subscriber
        #     await register_self_as_subscriber(await spanner_module.get_self_url())
        # # except Exception as e:
        # except Exception as e:
        #     logger.error(f"GCP Module: Failed to create webhook registration task: {e}", exc_info=True)
        # # # --- END REGISTRATION ---
        yield

