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
from contextlib import asynccontextmanager
from dynastore.modules import dynastore_module, ModuleProtocol
from .db_config import DBConfig

logger = logging.getLogger(__name__)

@dynastore_module
class DBConfigModule(ModuleProtocol):
    
    def __init__(self):
        logger.info("DBConfigModule: Configuration loaded and attached to app_state.db_config.")

    def get_config(self) -> DBConfig:
        """Returns the current database configuration."""
        return self.app_state.db_config

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        """Manages the configuration's presence in the app state during the app's lifecycle."""
        logger.info("DBConfigModule: Lifespan starting up.")
        self.app_state = app_state
        try:
            app_state.db_config = DBConfig()

            yield
        finally:
            if hasattr(self.app_state, 'db_config'):
                self.app_state.db_config = None
            logger.info("DBConfigModule: Lifespan shutting down, config removed from app_state.")
