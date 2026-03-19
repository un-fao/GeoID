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
