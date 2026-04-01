#    Copyright 2025 FAO
#    Licensed under the Apache License, Version 2.0 (the "License").

"""
Data models for the Schema Migration task.
"""

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class SchemaMigrationInputs(BaseModel):
    """
    Input payload for the schema_migration task.

    Attributes:
        catalog_id:      Target catalog.
        collection_id:   Target collection (must be a physical collection).
        target_config:   New ``PostgresCollectionDriverConfig`` as a plain dict.
                         When provided, the new physical tables are created from
                         this config instead of re-reading the stored config.
                         This is the primary mechanism for evolving a collection's
                         schema to a new configuration.
        dry_run:         When True, report what would happen without touching data.
    """

    catalog_id: str
    collection_id: str
    target_config: Optional[Dict[str, Any]] = Field(
        default=None,
        description="New PostgresCollectionDriverConfig as a dict (optional; defaults to current stored config).",
    )
    dry_run: bool = False


class TableExportReport(BaseModel):
    """Export report for a single table (hub or sidecar)."""

    table_name: str
    backup_name: str  # renamed-to name while migration is in progress
    row_count: int
    file_path: str
    columns: List[str]


class SchemaMigrationReport(BaseModel):
    """Complete report returned by the schema_migration task."""

    catalog_id: str
    collection_id: str
    physical_table: str
    schema: str
    timestamp: str
    tables: List[TableExportReport] = Field(default_factory=list)
    imported_rows: Dict[str, int] = Field(default_factory=dict)
    status: Literal["completed", "failed", "dry_run", "no_op"]
    error: Optional[str] = None
    dry_run: bool = False
