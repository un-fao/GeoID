from enum import StrEnum
from pydantic import BaseModel, Field
from typing import Dict, Optional, Any
from datetime import datetime


class OwnerType(StrEnum):
    MODULE = "module"
    SYSADMIN = "sysadmin"


class NotebookBase(BaseModel):
    """Base model for notebook data"""
    notebook_id: str = Field(..., description="Logical ID/slug for the notebook")
    title: str = Field(..., description="Human-readable title")
    description: Optional[str] = Field(default=None, description="Optional description")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Arbitrary tags, kernel specs, or requirements")


class NotebookCreate(NotebookBase):
    """Model for creating/saving a tenant notebook"""
    content: Dict[str, Any] = Field(..., description="The full .ipynb JSON structure")


class Notebook(NotebookCreate):
    """Full tenant notebook model including system fields"""
    catalog_id: str = Field(..., description="The catalog this notebook belongs to")
    created_at: datetime = Field(..., description="Timestamp when created")
    updated_at: datetime = Field(..., description="Timestamp when last updated")
    deleted_at: Optional[datetime] = Field(default=None, description="Soft-delete timestamp")
    owner_id: Optional[str] = Field(default=None, description="Principal ID of the owner")
    copied_from: Optional[str] = Field(default=None, description="Platform notebook_id this was copied from")


class PlatformNotebookCreate(NotebookBase):
    """Model for creating/saving a platform-level notebook"""
    content: Dict[str, Any] = Field(..., description="The full .ipynb JSON structure")
    registered_by: str = Field(..., description="Module name or 'sysadmin'")
    owner_type: OwnerType = Field(..., description="'module' or 'sysadmin'")


class PlatformNotebook(PlatformNotebookCreate):
    """Full platform notebook model including system fields"""
    created_at: datetime = Field(..., description="Timestamp when created")
    updated_at: datetime = Field(..., description="Timestamp when last updated")
    deleted_at: Optional[datetime] = Field(default=None, description="Soft-delete timestamp")
