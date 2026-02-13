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

"""
Sidecar Base Protocol and Configuration.

This module defines the contract for implementing specialized storage sidecars
including methods for DDL generation, data transformation, and operation validation.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Set, Union, Tuple, Protocol, runtime_checkable
from pydantic import BaseModel, Field, field_validator
from enum import Enum
from dynastore.modules.db_config.query_executor import DbResource
from dynastore.models.localization import LocalizedText


class FieldCapability(str, Enum):
    """Capabilities a field can have."""
    FILTERABLE = "filterable"      # Can be used in WHERE
    SORTABLE = "sortable"          # Can be used in ORDER BY
    GROUPABLE = "groupable"        # Can be used in GROUP BY
    AGGREGATABLE = "aggregatable"  # Can be aggregated (SUM, COUNT, etc.)
    SPATIAL = "spatial"            # Spatial operations available
    INDEXED = "indexed"            # Has database index

class FieldDefinition(BaseModel):
    """Definition of a queryable field."""
    name: str
    title: Optional[Union[str, LocalizedText]] = None
    description: Optional[Union[str, LocalizedText]] = None
    sql_expression: str  # e.g., "sc_geom.geom", "sc_attr.external_id"
    capabilities: List[FieldCapability]
    data_type: str  # "geometry", "text", "integer", "jsonb", etc.
    aggregations: Optional[List[str]] = None  # None or ["*"] = all allowed, [] = none, ["count", "sum"] = specific
    transformations: Optional[List[str]] = None  # None or ["*"] = all allowed, [] = none, ["upper", "lower"] = specific
    
    def supports_aggregation(self, agg_func: str) -> bool:
        """Check if this field supports a specific aggregation."""
        if self.aggregations is None or "*" in (self.aggregations or []):
            return True  # All aggregations allowed
        return agg_func in (self.aggregations or [])
    
    def supports_transformation(self, transform_func: str) -> bool:
        """Check if this field supports a specific transformation."""
        if self.transformations is None or "*" in (self.transformations or []):
            return True  # All transformations allowed
        return transform_func in (self.transformations or [])

class ValidationResult(BaseModel):
    """Result of an operation validation."""
    valid: bool
    error: Optional[str] = None


class SidecarProtocol(ABC):
    """
    Abstract base class for sidecar storage implementations.
    
    Each sidecar manages a specific domain of data (geometry, attributes, etc.)
    and must implement methods for DDL generation, data transformation, 
    query resolution, and operation validation.
    """
    
    @property
    @abstractmethod
    def sidecar_id(self) -> str:
        """
        Unique identifier for this sidecar (e.g., 'geom', 'attrs').
        Used for table naming: {physical_table}_{sidecar_id}
        """
        pass
    
    @abstractmethod
    def get_ddl(
        self, 
        physical_table: str, 
        partition_keys: List[str] = [],
        partition_key_types: Dict[str, str] = {},
        has_validity: bool = False
    ) -> str:
        """
        Generates the CREATE TABLE DDL for this sidecar.
        
        Args:
            physical_table: Physical Hub table name (e.g. 't_abc123')
            partition_keys: List of columns forming the composite partition key
            partition_key_types: Dictionary mapping key names to their SQL types
            has_validity: Flag indicating if the sidecar should include a 'validity' column.
        
        Returns:
            SQL DDL string for creating the sidecar table
            
        Note:
            - MUST include FK to Hub: FOREIGN KEY (geoid) REFERENCES hub(geoid) ON DELETE CASCADE
            - MUST use same PARTITION BY clause as Hub if partitioning is enabled
            - MUST use (partition_key, geoid) as composite PK
        """
        pass
    
    @abstractmethod
    async def setup_lifecycle_hooks(
        self, 
        conn: DbResource, 
        schema: str,
        table_name: str
    ) -> None:
        """Register maintenance hooks on table creation."""
        pass
    
    @abstractmethod
    async def on_partition_create(
        self, 
        conn: DbResource,
        schema: str,
        parent_table: str,
        partition_table: str,
        partition_value: Any
    ) -> None:
        """Hook called after JIT partition creation."""
        pass
    
    @abstractmethod
    def resolve_query_path(
        self, 
        attr_name: str
    ) -> Optional[Tuple[str, str]]:
        """Resolves an attribute reference to SQL and JOIN requirements."""
        pass
    
    @abstractmethod
    def prepare_upsert_payload(
        self, 
        feature: Dict[str, Any],
        context: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Transforms a feature into sidecar-specific insert data."""
        pass
    
    # --- Query Generation Methods ---
    
    @abstractmethod
    def get_select_fields(
        self,
        hub_alias: str = "h",
        sidecar_alias: Optional[str] = None,
        include_all: bool = False
    ) -> List[str]:
        """
        Returns list of SELECT field expressions for this sidecar.
        
        Args:
            hub_alias: Alias used for hub table
            sidecar_alias: Alias for this sidecar table (auto-generated if None)
            include_all: If True, include all sidecar columns; if False, only essential ones
            
        Returns:
            List of SQL field expressions (e.g., ["sc.external_id", "ST_AsEWKB(sc.geom) as geom"])
        """
        pass
    
    @abstractmethod
    def get_join_clause(
        self,
        schema: str,
        hub_table: str,
        hub_alias: str = "h",
        sidecar_alias: Optional[str] = None,
        join_type: str = "LEFT"
    ) -> str:
        """
        Returns JOIN clause for this sidecar.
        
        **Important**: Sidecars should ONLY join on `geoid` (and partition keys if applicable).
        Validity filtering is a Hub-level concern and should NOT be included in sidecar JOINs.
        
        Args:
            schema: Physical schema name
            hub_table: Physical hub table name
            hub_alias: Alias for hub table
            sidecar_alias: Alias for sidecar (auto-generated if None)
            join_type: JOIN type (LEFT, INNER, etc.)
            
        Returns:
            Complete JOIN clause (e.g., 'LEFT JOIN "schema"."table_geom" sc_geom ON h.geoid = sc_geom.geoid')
        """
        pass
    
    def get_where_conditions(
        self,
        sidecar_alias: Optional[str] = None,
        **filters
    ) -> List[str]:
        """
        Returns WHERE clause conditions for filtering by this sidecar's columns.
        
        Args:
            sidecar_alias: Alias for sidecar table
            **filters: Filter criteria (e.g., external_id="abc")
            
        Returns:
            List of WHERE conditions (e.g., ["sc.external_id = :ext_id"])
        """
        return []
    
    def get_order_by_fields(
        self,
        sidecar_alias: Optional[str] = None
    ) -> List[str]:
        """
        Returns list of fields that can be used in ORDER BY clauses.
        
        Args:
            sidecar_alias: Alias for sidecar table
            
        Returns:
            List of field names that support ordering (e.g., ["external_id", "asset_id"])
        """
        return []
    
    def get_group_by_fields(
        self,
        sidecar_alias: Optional[str] = None
    ) -> List[str]:
        """
        Returns list of fields that can be used in GROUP BY clauses.
        
        Args:
            sidecar_alias: Alias for sidecar table
            
        Returns:
            List of field names that support grouping (e.g., ["asset_id", "h3_res8"])
        """
        return []

    async def resolve_existing_item(
        self,
        conn: DbResource,
        physical_schema: str,
        physical_table: str,
        processing_context: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Resolves an existing item based on the sidecar's identity logic (e.g. external_id).
        
        This method allows a sidecar to identify if an incoming item matches an existing one,
        enabling upsert/versioning logic.
        
        Args:
            conn: Database connection.
            physical_schema: Physical schema name.
            physical_table: Physical table name (Hub).
            processing_context: Context containing resolved IDs (e.g. external_id).
        
        Returns:
            Dictionary representing the existing item (must include 'geoid', and optionally 'validity'), 
            or None if not found or not applicable.
        """
        return None

    async def check_collision(
        self,
        conn: DbResource,
        physical_schema: str,
        physical_table: str,
        field_name: str,
        value: Any,
        exclude_geoid: Optional[Any] = None
    ) -> bool:
        """
        Checks if a specific value already exists in the sidecar's table.
        
        This is used for high-performance collision detection (e.g. REFUSE_ON_ASSET_ID_COLLISION).
        
        Args:
            conn: Database connection.
            physical_schema: Physical schema name.
            physical_table: Physical Hub table name.
            field_name: Name of the field to check.
            value: Value to check for.
            exclude_geoid: Optional geoid to exclude from collision check (e.g. during update).
            
        Returns:
            True if collision found, False otherwise.
        """
        return False

    def is_acceptable(
        self,
        feature: Dict[str, Any],
        context: Dict[str, Any]
    ) -> bool:
        """
        Checks if the sidecar can accept this feature for processing.
        
        Args:
            feature: The incoming GeoJSON feature.
            context: Processing context.
            
        Returns:
            True if feature is acceptable, False otherwise.
        """
        return True
    
    def get_field_definitions(
        self,
        sidecar_alias: Optional[str] = None
    ) -> Dict[str, FieldDefinition]:
        """
        Returns all queryable fields exposed by this sidecar.
        
        Returns:
            Dict mapping field name to FieldDefinition
        """
        return {}
    
    def get_default_sort(self) -> Optional[List[Tuple[str, str]]]:
        """
        Returns default sort order for this sidecar.
        
        Returns:
            List of (field_name, direction) tuples, e.g., [("external_id", "ASC")]
        """
        return None
    
    def supports_aggregation(self, field_name: str, agg_func: str) -> bool:
        """Check if field supports specific aggregation."""
        fields = self.get_field_definitions()
        field = fields.get(field_name)
        return field is not None and field.supports_aggregation(agg_func)
    
    def supports_transformation(self, field_name: str, transform_func: str) -> bool:
        """Check if field supports specific transformation."""
        fields = self.get_field_definitions()
        field = fields.get(field_name)
        return field is not None and field.supports_transformation(transform_func)

    # --- Operation Validation Hooks ---

    def validate_insert(
        self, 
        feature: Dict[str, Any], 
        context: Dict[str, Any]
    ) -> ValidationResult:
        """
        Validate if a feature can be inserted into this sidecar.
        Default implementation allows everything.
        """
        return ValidationResult(valid=True)

    def validate_update(
        self, 
        feature: Dict[str, Any], 
        existing: Dict[str, Any], 
        context: Dict[str, Any]
    ) -> ValidationResult:
        """
        Validate if a feature update is allowed.
        Default implementation allows everything.
        """
        return ValidationResult(valid=True)

    def validate_delete(
        self, 
        geoid: str, 
        context: Dict[str, Any]
    ) -> ValidationResult:
        """
        Validate if a feature deletion is allowed.
        Default implementation allows everything.
        """
        return ValidationResult(valid=True)
    



class SidecarConfig(BaseModel):
    """
    Base configuration model for sidecars.
    """
    sidecar_type: str  # Discriminator field
    enabled: bool = True
    
    # Per-sidecar indexing configuration
    # Note: IndexingConfig will be imported inside methods to avoid circular imports if needed
    indexing: Optional[Dict[str, Any]] = Field(
        default=None, 
        description="Sidecar-specific indexing configuration"
    )
    
    @property
    def partition_key_contributions(self) -> Dict[str, str]:
        """
        Returns a dictionary of {column_name: sql_type} for keys this sidecar 
        contributes to the global composite partition key.
        Override in subclasses.
        """
        return {}
    
    @property
    def sidecar_id(self) -> str:
        """
        Returns the standard sidecar_id for this config type.
        """
        mapping = {
            "geometry": "geometry",
            "attributes": "attributes"
        }
        return mapping.get(self.sidecar_type, self.sidecar_type)

    model_config = {"extra": "allow"}
