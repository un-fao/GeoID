from typing import Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field, ConfigDict
from dynastore.tools.json import CustomJSONEncoder

# --- SQL Schema ---

# --- Pydantic Models ---

class LogEntry(BaseModel):
    id: Optional[int] = None
    catalog_id: str
    collection_id: Optional[str] = None
    event_type: str
    level: str = "INFO"
    message: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    timestamp: Optional[datetime] = Field(None, validation_alias="created_at")

    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True
    )

class LogEntryCreate(BaseModel):
    catalog_id: str
    collection_id: Optional[str] = None
    event_type: str
    level: str = "INFO"
    message: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    is_system: bool = False
