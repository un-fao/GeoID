from pydantic import BaseModel, Field
from typing import Dict, Optional, Any
from datetime import datetime

class NotebookBase(BaseModel):
    """Base model for notebook data"""
    notebook_id: str = Field(..., description="Logical ID/slug for the notebook (e.g., 'vegetation-analysis-v1')")
    title: str = Field(..., description="Human-readable title")
    description: Optional[str] = Field(None, description="Optional description")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Arbitrary tags, kernel specs, or requirements")

class NotebookCreate(NotebookBase):
    """Model for creating/saving a notebook"""
    content: Dict[str, Any] = Field(..., description="The full .ipynb JSON structure")

class Notebook(NotebookCreate):
    """Full notebook model including system fields"""
    catalog_id: str = Field(..., description="The catalog id this notebook belongs to")
    created_at: datetime = Field(..., description="Timestamp when created")
    updated_at: datetime = Field(..., description="Timestamp when last updated")
