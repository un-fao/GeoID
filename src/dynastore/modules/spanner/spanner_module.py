# dynastore/modules/spanner/service.py
import logging
import os
from uuid import uuid4
from typing import Optional
from contextlib import asynccontextmanager
from dynastore.modules import ModuleProtocol, get_protocol
from dynastore.modules.spanner.spanner_init import startup
import dynastore.modules.spanner.datamind as datamind
import dynastore.modules.spanner.permission as permission
import dynastore.modules.spanner.models.business.asset as _abm
import dynastore.modules.spanner.models.business.principal as _pbm
from dynastore.modules.spanner.constants import SYSTEM_PRINCIPAL_UUID, PrincipalTypes
#import dynastore.modules.spanner.spanner as spanner

from google.cloud import spanner
from google.cloud.spanner_v1.pool import BurstyPool
from google.api_core.exceptions import NotFound, InvalidArgument




# Assuming you have a Config tool similar to DBConfig, or we extract from generic config
# from dynastore.modules.config.tools import get_config 

from dynastore.modules.spanner.session_manager import SpannerSessionManager

logger = logging.getLogger(__name__)

class SpannerModule(ModuleProtocol):
    priority: int = 100
    app_state: object
    #router: APIRouter = APIRouter(prefix="/spanner", tags=["Spanner", "Permission"])

    def __init__(self, app_state: object):
        self.app_state = app_state

    def get_manager(self) -> Optional[SpannerSessionManager]:
        """Accessor to get the manager from app_state safely."""
        return getattr(self.app_state, 'spanner_manager', None)

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        """
        Manages the lifespan of the Spanner Session Manager.
        """
        logger.info("SpannerService: Startup initiated...")
        
        # 1. Retrieve Configuration
        # You might have a specific SpannerConfig object, or read from env/dict
        # For this example, I'll assume they are available in app_state or env
        
        # Example: Fetching from a hypothetical config object in app_state
        # config = app_state.config.spanner 
        
        # Fallback to Envs for demonstration
        project_id = os.getenv("SPANNER_PROJECT_ID")
        instance_id = os.getenv("SPANNER_INSTANCE_ID")
        database_id = os.getenv("SPANNER_DATABASE_ID")

        if not all([project_id, instance_id, database_id]):
            logger.warning("SpannerService: Missing configuration. Skipping initialization.")
            yield
            return

        try:
            # 2. Initialize the Session Manager
            logger.info(f"SpannerService: Connecting to {project_id}/{instance_id}/{database_id}")
            pool_kwargs = {}
            self.client = spanner.Client(project=project_id, disable_builtin_metrics=True)
            self.instance = self.client.instance(instance_id)
            self.database = self.instance.database(database_id)
            self._pool = BurstyPool(**pool_kwargs)
            # TO FIX THE VERSION ISSUE
            self._pool.bind(self.database)
            self.database.pool = self._pool
            manager = SpannerSessionManager(
                pool=self._pool
            )

            # 3. Attach to App State
            app_state.spanner_manager = manager
            logger.info("SpannerService: Manager attached to app_state.spanner_manager")
            

            await startup.initialize(self, manager)

            # DB is initialized, now ensure system principal exists
            system_principal = await datamind.get_principal_by_uuid(manager=manager, principal_uuid=SYSTEM_PRINCIPAL_UUID)
            if not system_principal:
                system_principal = _pbm.Principal(uuid=SYSTEM_PRINCIPAL_UUID, principal_id=f"system:{SYSTEM_PRINCIPAL_UUID}", type_code=PrincipalTypes.USER)
                await datamind.create_principal(manager, system_principal)
            app_state.system_principal = system_principal

            yield

        # except (StopAsyncIteration, InvalidArgument):
        #     await _repository.init_permission_registry(app_state=app_state)

        except Exception as e:
            logger.critical(f"SpannerService: FATAL: Failed to initialize Spanner: {e}", exc_info=True)
            raise

        finally:
            logger.info("SpannerService: Shutdown initiated...")
            if hasattr(app_state, 'spanner_manager') and app_state.spanner_manager:
                self.database.pool.clear()
                app_state.spanner_manager = None
                logger.info("SpannerService: Manager detached and closed.")
            logger.info("SpannerService: Shutdown completed.")

    #have an extension, in the extension do not use the name spanner and use "permission" 

