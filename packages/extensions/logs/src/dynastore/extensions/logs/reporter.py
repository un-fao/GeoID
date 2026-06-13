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

from typing import Optional, Dict, Any
from dynastore.modules.catalog import log_manager

class LogReporter:
    """
    Helper class for tasks to report logs to the LogExtension using the global manager.
    """
    def __init__(self, catalog_id: str, collection_id: Optional[str] = None):
        self.catalog_id = catalog_id
        self.collection_id = collection_id

    async def log(self, event_type: str, level: str, message: str, details: Optional[Dict[str, Any]] = None):
        await log_manager.log_event(
            catalog_id=self.catalog_id,
            collection_id=self.collection_id,
            event_type=event_type,
            level=level,
            message=message,
            details=details
        )

    async def info(self, event_type: str, message: str, details: Optional[Dict[str, Any]] = None):
        await self.log(event_type, "INFO", message, details)

    async def warning(self, event_type: str, message: str, details: Optional[Dict[str, Any]] = None):
        await self.log(event_type, "WARNING", message, details)

    async def error(self, event_type: str, message: str, details: Optional[Dict[str, Any]] = None):
        await self.log(event_type, "ERROR", message, details)
