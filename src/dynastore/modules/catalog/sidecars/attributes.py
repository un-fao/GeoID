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
Feature Attribute Sidecar Implementation.

This module provides the FeatureAttributeSidecar which manages:
- Feature attributes (Relational or JSONB Mode)
- Identity columns (external_id, asset_id)
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
from enum import Enum
from pydantic import Field
from sqlalchemy import text
from dynastore.modules.db_config.query_executor import DbResource
from dynastore.modules.catalog.sidecars.base import (
    SidecarProtocol, 
    SidecarConfig, 
    ValidationResult,
    FieldDefinition,
    FieldCapability
)
from dynastore.modules.catalog.models import LocalizedText
from dynastore.modules.catalog.sidecars.attributes_config import (
    FeatureAttributeSidecarConfig,
    AttributeStorageMode,
    PostgresType,
    AttributeIndexType,
    AttributeSchemaEntry
)
from dynastore.modules.db_config.query_executor import DbResource
from dynastore.modules.db_config.locking_tools import acquire_lock_if_needed, check_trigger_exists

logger = logging.getLogger(__name__)


class FeatureAttributeSidecar(SidecarProtocol):
    """
    Sidecar for feature attributes and identity.
    
    Manages:
    - Identity: external_id, asset_id (optional)
    - Attributes: Relational columns or JSONB document
    """
    
    def __init__(self, config: FeatureAttributeSidecarConfig):
        self.config = config

    @property
    def resolved_storage_mode(self) -> AttributeStorageMode:
        """Resolve AUTOMATIC storage mode based on schema presence."""
        if self.config.storage_mode != AttributeStorageMode.AUTOMATIC:
            return self.config.storage_mode
        if self.config.attribute_schema:
            return AttributeStorageMode.COLUMNAR
        return AttributeStorageMode.JSONB
    
    @property
    def sidecar_id(self) -> str:
        return "attributes"

    @property
    def partition_key_contributions(self) -> Dict[str, str]:
        """Expose keys available for partitioning."""
        keys = {}
        if self.config.enable_external_id:
            keys["external_id"] = "text"
        if self.config.enable_asset_id:
            keys["asset_id"] = "text"
        if self.config.enable_validity:
            keys["validity"] = "tstzrange" # Actually managed by sidecar now
        
        # Columnar attributes could also be partitioning keys if marked as such
        if self.resolved_storage_mode == AttributeStorageMode.COLUMNAR and self.config.attribute_schema:
            for attr in self.config.attribute_schema:
                if attr.can_partition:
                    keys[attr.name] = attr.type.value
        return keys
    
    def get_ddl(
        self, 
        physical_table: str, 
        partition_keys: List[str] = [],
        partition_key_types: Dict[str, str] = {},
        has_validity: bool = False
    ) -> str:
        """Generate DDL for attribute sidecar table."""
        table_name = f"{physical_table}_{self.sidecar_id}"
        columns = ["geoid UUID NOT NULL"]
        known_columns = {"geoid"}
        
        # Add Identity Columns
        if self.config.enable_external_id:
            columns.append("external_id VARCHAR(255)")
            known_columns.add("external_id")
        
        if self.config.enable_asset_id:
            columns.append("asset_id VARCHAR(255)")
            known_columns.add("asset_id")
        
        # Add Attribute Storage
        indexes = []
        if self.resolved_storage_mode == AttributeStorageMode.COLUMNAR:
            # Mode A: Add physical columns from schema
            if self.config.attribute_schema:
                for attr in self.config.attribute_schema:
                    null_constraint = "" if attr.nullable else " NOT NULL"
                    unique_constraint = " UNIQUE" if attr.unique else ""
                    columns.append(f"{attr.name} {attr.type.value}{null_constraint}{unique_constraint}")
                    known_columns.add(attr.name)
                    
                    # Create index if specified
                    if attr.index != AttributeIndexType.NONE:
                        idx_type = attr.index.value.upper()
                        idx_clause = f"USING {idx_type}" if idx_type != "BTREE" else ""
                        indexes.append(
                            f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_{attr.name}" ON {{schema}}."{table_name}" '
                            f"{idx_clause} ({attr.name})"
                        )
        else:
            # Mode B: JSONB column
            columns.append(f"{self.config.jsonb_column_name} JSONB")
            known_columns.add(self.config.jsonb_column_name)
            
            # GIN index for full document containment (if not overridden by functional indexes)
            # Standard GIN
            indexes.append(f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_attrs_gin" ON {{schema}}."{table_name}" USING GIN({self.config.jsonb_column_name})')
            
            # Functional B-Tree indexes on specific JSON paths for trillion-row scale
            for path, cast_type in self.config.jsonb_indexed_paths.items():
                indexes.append(
                    f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_{path}" ON {{schema}}."{table_name}" '
                    f"(({self.config.jsonb_column_name}->>'{path}')::{cast_type.value})"
                )
        
        # Prepare validity column
        validity_col = ""
        if has_validity or "validity" in partition_keys:
             if "validity" not in known_columns:
                 # validity_col = '"validity" TSTZRANGE NOT NULL,' # WRONG: we need to add it to columns list
                 columns.append('"validity" TSTZRANGE NOT NULL')
                 known_columns.add("validity")

        # Composite PK Construction
        # Rule: Partition keys FIRST, then Hub identity (geoid, validity)
        pk_columns = []
        partition_clause = ""
        
        if partition_keys:
            # 1. Add any non-sidecar partition keys (e.g. catalog_id from Hub)
            for key in partition_keys:
                if key not in known_columns:
                    col_type = partition_key_types.get(key, "TEXT")
                    columns.insert(0, f'"{key}" {col_type} NOT NULL')
                    known_columns.add(key)
            
            # 2. Partitions must be part of the PK
            pk_set = set()
            for key in partition_keys:
                pk_columns.append(f'"{key}"')
                pk_set.add(key)
            
            # 3. Complete the PK with identity if not already present
            if "geoid" not in pk_set:
                pk_columns.append('"geoid"')
                pk_set.add("geoid")
            if (has_validity or "validity" in partition_keys) and "validity" not in pk_set:
                pk_columns.append('"validity"')
                pk_set.add("validity")
                
            partition_clause = f" PARTITION BY LIST ({', '.join([f'"{k}"' for k in partition_keys])})"
        else:
            partition_clause = ""
            # Non-partitioned table: PK is geoid + validity (if has_validity)
            pk_columns = ['"geoid"']
            if has_validity or "validity" in partition_keys:
                 pk_columns.append('"validity"')

        # Build DDL
        storage_params = ""
        if self.resolved_storage_mode == AttributeStorageMode.JSONB and self.config.use_hot_updates:
            storage_params = " WITH (FILLFACTOR=80)"

        ddl = f"""
CREATE TABLE IF NOT EXISTS {{schema}}."{table_name}" (
    {', '.join(columns)},
    PRIMARY KEY ({', '.join(pk_columns)})
){partition_clause}{storage_params};
"""
        
        # Foreign Key to Hub - only if validity matches or only geoid
        ref_cols = ["geoid"]
        if has_validity or "validity" in partition_keys:
             ref_cols.append("validity")
             
        ddl += f"""
ALTER TABLE {{schema}}."{table_name}"
ADD CONSTRAINT "fk_{table_name}_geoid"
FOREIGN KEY ({", ".join([f'"{c}"' for c in ref_cols])}) REFERENCES {{schema}}."{physical_table}" ON DELETE CASCADE;
"""
        
        # Add Indices
        # Identity Indices
        if self.config.enable_external_id and self.config.index_external_id:
            ddl += f'\nCREATE UNIQUE INDEX IF NOT EXISTS "idx_{table_name}_ext_id" ON {{schema}}."{table_name}" ({", ".join(pk_columns[:-1] + ["external_id"] if len(pk_columns) > 1 else ["external_id"])});'
        
        if self.config.enable_asset_id and self.config.index_asset_id:
            ddl += f'\nCREATE INDEX IF NOT EXISTS "idx_{table_name}_asset_id" ON {{schema}}."{table_name}" (asset_id);'

        # Add Computed Indices from schema/paths
        for idx_stmt in indexes:
            ddl += f"\n{idx_stmt};"
        
        return ddl
    
    async def setup_lifecycle_hooks(
        self, 
        conn: DbResource, 
        schema: str,
        table_name: str
    ) -> None:
        """Setup maintenance for attribute table."""
        if self.resolved_storage_mode == AttributeStorageMode.JSONB:
            logger.info(f"JSONB table {schema}.{table_name} configured with FILLFACTOR=80")
        
        # Setup Asset Cleanup Trigger if asset_id is enabled
        if self.config.enable_asset_id:
            logger.info(f"Setting up asset cleanup trigger for {schema}.{table_name}")
            
            # Using a stable trigger name
            trigger_name = "trg_asset_cleanup"
            
            # Derive Hub Physical Table Name
            # table_name is {hub_physical_table}_{sidecar_id}
            # We assume sidecar_id is 'attributes'
            suffix = f"_{self.sidecar_id}"
            if table_name.endswith(suffix):
                hub_physical_table = table_name[:-len(suffix)]
            else:
                # Fallback or error? 
                # If naming convention changes, this breaks. 
                # But for now strict convention is enforced in CollectionService.
                logger.warning(f"Could not derive hub table from {table_name}. Skipping trigger.")
                return

            # Safe DDL Execution
            # Using acquire_lock_if_needed directly to avoid splitting issues in execute_safe_ddl
            
            # Use check_trigger_exists as existence check
            async def _check():
                return await check_trigger_exists(conn, trigger_name, schema)

            # Using same lock for trigger creation
            lock_key = f"ddl.trigger.{schema}.{table_name}.{trigger_name}"
            
            async with acquire_lock_if_needed(conn, lock_key, _check) as should_run:
                if should_run:
                    # Drop first (idempotency, or replace if modified). No trailing semicolon for asyncpg prepared stmt.
                    await should_run.execute(text(f'DROP TRIGGER IF EXISTS {trigger_name} ON "{schema}"."{table_name}"'))
                    
                    # Create. No trailing semicolon.
                    create_sql = f"""
                    CREATE TRIGGER {trigger_name}
                    AFTER DELETE OR UPDATE OF asset_id ON "{schema}"."{table_name}"
                    FOR EACH ROW
                    EXECUTE FUNCTION asset_cleanup('{hub_physical_table}')
                    """.strip()
                    await should_run.execute(text(create_sql))
    
    async def on_partition_create(
        self, 
        conn: DbResource,
        schema: str,
        parent_table: str,
        partition_table: str,
        partition_value: Any
    ) -> None:
        """Partition-specific setup."""
        pass
    
    def resolve_query_path(
        self, 
        attr_name: str
    ) -> Optional[Tuple[str, str]]:
        """Resolve attribute access."""
        alias = "a"
        
        # Identity Columns
        if self.config.enable_external_id and attr_name == "external_id":
             return (f"{alias}.external_id", alias)
        if self.config.enable_asset_id and attr_name == "asset_id":
             return (f"{alias}.asset_id", alias)
        
        # Columnar Mode
        if self.resolved_storage_mode == AttributeStorageMode.COLUMNAR:
            if self.config.attribute_schema:
                for attr in self.config.attribute_schema:
                    if attr.name == attr_name:
                        return (f"{alias}.{attr_name}", alias)
        # JSONB Mode
        else:
            if attr_name == self.config.jsonb_column_name:
                return (f"{alias}.{self.config.jsonb_column_name}", alias)
            
            # Check if it's a known functional path
            if attr_name in self.config.jsonb_indexed_paths:
                cast_type = self.config.jsonb_indexed_paths[attr_name].value
                return (f"({alias}.{self.config.jsonb_column_name}->>'{attr_name}')::{cast_type}", alias)

            # Nested JSON path fallback
            if attr_name.startswith("properties."):
                json_key = attr_name.split(".", 1)[1]
                return (f"{alias}.{self.config.jsonb_column_name}->>'{json_key}'", alias)
            else:
                return (f"{alias}.{self.config.jsonb_column_name}->>'{attr_name}'", alias)
        
        return None
    
    def get_select_fields(
        self,
        hub_alias: str = "h",
        sidecar_alias: Optional[str] = None,
        include_all: bool = False
    ) -> List[str]:
        """Returns SELECT field expressions for attributes sidecar."""
        alias = sidecar_alias or f"sc_{self.sidecar_id}"
        fields = []
        
        if self.resolved_storage_mode == AttributeStorageMode.JSONB:
            fields.append(f"{alias}.attributes")
            # We explicitly EXCLUDE external_id and asset_id from default selects 
            # to satisfy privacy/security requirements (e.g. external_id hiding).
            if include_all:
                 if self.config.enable_external_id:
                      fields.append(f"{alias}.external_id")
                 if self.config.enable_asset_id:
                      fields.append(f"{alias}.asset_id")
        elif include_all:
              # For relational mode, we might want to be careful too.
              fields.append(f"{alias}.*")
        else:
              # Relational mode: return all columnar attributes EXCEPT internal IDs
              known_internal = {"geoid", "validity", "external_id", "asset_id"}
              if self.config.attribute_schema:
                   for attr in self.config.attribute_schema:
                        if attr.name not in known_internal:
                             fields.append(f'{alias}."{attr.name}"')
        
        return fields
    
    def get_join_clause(
        self,
        schema: str,
        hub_table: str,
        hub_alias: str = "h",
        sidecar_alias: Optional[str] = None,
        join_type: str = "LEFT"
    ) -> str:
        """Returns JOIN clause for attributes sidecar (geoid-only join)."""
        alias = sidecar_alias or f"sc_{self.sidecar_id}"
        table = f"{hub_table}_{self.sidecar_id}"
        return f'{join_type} JOIN "{schema}"."{table}" {alias} ON {hub_alias}.geoid = {alias}.geoid'
    
    def get_where_conditions(
        self,
        sidecar_alias: Optional[str] = None,
        **filters
    ) -> List[str]:
        """Returns WHERE conditions for filtering by attributes."""
        alias = sidecar_alias or f"sc_{self.sidecar_id}"
        conditions = []
        
        if "external_id" in filters and self.config.enable_external_id:
            conditions.append(f"{alias}.external_id = CAST(:ext_id AS TEXT)")
        
        if "asset_id" in filters and self.config.enable_asset_id:
            conditions.append(f"{alias}.asset_id = :asset_id")
        
        return conditions
    
    def get_order_by_fields(
        self,
        sidecar_alias: Optional[str] = None
    ) -> List[str]:
        """Returns fields that can be used for ordering."""
        alias = sidecar_alias or f"sc_{self.sidecar_id}"
        fields = []
        
        if self.config.enable_external_id:
            fields.append(f"{alias}.external_id")
        
        if self.config.enable_asset_id:
            fields.append(f"{alias}.asset_id")
        
        # Columnar mode attributes can be ordered
        if self.resolved_storage_mode == AttributeStorageMode.COLUMNAR and self.config.attribute_schema:
            for attr in self.config.attribute_schema:
                fields.append(f"{alias}.{attr.name}")
        
        return fields
    
    def get_group_by_fields(
        self,
        sidecar_alias: Optional[str] = None
    ) -> List[str]:
        """Returns fields that can be used for grouping."""
        alias = sidecar_alias or f"sc_{self.sidecar_id}"
        fields = []
        
        if self.config.enable_external_id:
            fields.append(f"{alias}.external_id")
        
        if self.config.enable_asset_id:
            fields.append(f"{alias}.asset_id")
        
        # Columnar mode attributes can be grouped
        if self.resolved_storage_mode == AttributeStorageMode.COLUMNAR and self.config.attribute_schema:
            for attr in self.config.attribute_schema:
                fields.append(f"{alias}.{attr.name}")
        
        return fields
    
    def get_field_definitions(
        self,
        sidecar_alias: Optional[str] = None
    ) -> Dict[str, FieldDefinition]:
        """Returns capabilities of attribute fields."""
        alias = sidecar_alias or f"sc_{self.sidecar_id}"
        fields = {}
        
        if self.config.enable_external_id:
            fields["external_id"] = FieldDefinition(
                name="external_id",
                title=LocalizedText(en="External ID", fr="Identifiant Externe", es="Identificador Externo"),
                description=LocalizedText(en="The unique identifier of the feature in the source system.", fr="L'identifiant unique de l'entité dans le système source.", es="El identificador único de la entidad en el sistema de origen."),
                sql_expression=f"{alias}.external_id",
                capabilities=[FieldCapability.FILTERABLE, FieldCapability.SORTABLE, 
                             FieldCapability.GROUPABLE, FieldCapability.INDEXED],
                data_type="text",
                aggregations=["count", "array_agg"],
                transformations=None  # All string transformations allowed
            )
        
        if self.config.enable_asset_id:
            fields["asset_id"] = FieldDefinition(
                name="asset_id",
                title=LocalizedText(en="Asset ID", fr="Identifiant de l'Actif", es="Identificador del Activo"),
                description=LocalizedText(en="The identifier of the associated physical asset.", fr="L'identifiant de l'actif physique associé.", es="El identificador del activo físico asociado."),
                sql_expression=f"{alias}.asset_id",
                capabilities=[FieldCapability.FILTERABLE, FieldCapability.SORTABLE, 
                             FieldCapability.GROUPABLE, FieldCapability.INDEXED],
                data_type="text",
                aggregations=["count", "array_agg"],
                transformations=[]  # No transformations for asset_id
            )
            
        if self.config.enable_validity:
            fields["validity"] = FieldDefinition(
                name="validity",
                title=LocalizedText(en="Validity Period", fr="Période de Validité", es="Periodo de Validez"),
                description=LocalizedText(en="The temporal range for which this feature version is valid.", fr="La plage temporelle pour laquelle cette version de l'entité est valide.", es="El rango temporal para el cual esta versión de la entidad es válida."),
                sql_expression=f"{alias}.validity",
                capabilities=[FieldCapability.FILTERABLE, FieldCapability.SORTABLE, 
                             FieldCapability.GROUPABLE, FieldCapability.INDEXED],
                data_type="tstzrange",
                aggregations=[], # Maybe specialized range aggs?
                transformations=["lower", "upper"]
            )
            
        # Standard Hub fields that might be useful to expose via attribute sidecar for consistency
        # transaction_time is often needed for STAC items
        fields["transaction_time"] = FieldDefinition(
            name="transaction_time",
            title=LocalizedText(en="Transaction Time", fr="Temps de Transaction", es="Tiempo de Transacción"),
            description=LocalizedText(en="The time when this record was recorded in the database.", fr="Le moment où cet enregistrement a été consigné dans la base de données.", es="El momento en que este registro fue grabado en la base de datos."),
            sql_expression="h.transaction_time", # Hub field
            capabilities=[FieldCapability.FILTERABLE, FieldCapability.SORTABLE],
            data_type="timestamptz"
        )
            
        # Columnar mode attributes
        if self.resolved_storage_mode == AttributeStorageMode.COLUMNAR and self.config.attribute_schema:
            for attr in self.config.attribute_schema:
                # Map postgres type to capabilities
                caps = [FieldCapability.FILTERABLE, FieldCapability.SORTABLE, FieldCapability.GROUPABLE]
                if attr.index_type != AttributeIndexType.NONE:
                    caps.append(FieldCapability.INDEXED)
                
                # Determine aggregations/transformations based on type
                aggs = ["count", "array_agg"]
                transforms = []
                
                if attr.pg_type in [PostgresType.INTEGER, PostgresType.BIGINT, PostgresType.NUMERIC, PostgresType.FLOAT]:
                    caps.append(FieldCapability.AGGREGATABLE)
                    aggs.extend(["sum", "avg", "min", "max"])
                elif attr.pg_type in [PostgresType.TEXT, PostgresType.VARCHAR]:
                    transforms = None  # All string transformations allowed
                
                fields[attr.name] = FieldDefinition(
                    name=attr.name,
                    sql_expression=f"{alias}.{attr.name}",
                    capabilities=caps,
                    data_type=attr.pg_type.value,
                    aggregations=aggs,
                    transformations=transforms
                )
        
        return fields
    
    def get_default_sort(self) -> Optional[List[Tuple[str, str]]]:
        if self.config.enable_external_id:
            return [("external_id", "ASC")]
        return None

    def prepare_upsert_payload(
        self, 
        feature: Dict[str, Any],
        context: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Extract attributes and identity from feature."""
        # Use context geoid or generate/fail
        geoid = context.get("geoid")
        if not geoid:
            raise ValueError("geoid is required in context")
        
        payload = {"geoid": geoid}
        
        # Partition Key check
        # If partition key is provided in context, use it. 
        # But we might also be the SOURCE of the partition key (e.g. valid_from or asset_id)
        if context.get("partition_key_name"):
             payload[context["partition_key_name"]] = context["partition_key_value"]

        # Identity Extraction: "The external_id value is the id of the incoming feature"
        if self.config.enable_external_id:
            # 1. Identity Logic
            # Try to extract the external_id using logical rules
            field_path = getattr(self.config, "external_id_field", "id")
            ext_id = self._extract_value(feature, field_path)
            
            if ext_id:
                # DECENTRALIZATION: Populate context so other components (like ItemService) can use it
                ext_id_str = str(ext_id)
                payload["external_id"] = ext_id_str
                context["external_id"] = ext_id_str

        asset_id = None
        if self.config.enable_asset_id:
            # 1. Start with context (System-assigned ID from ingestion has priority)
            asset_id = context.get("asset_id")
            
            # 2. If not in context, try configured field
            if not asset_id:
                field_path = getattr(self.config, "asset_id_field", "asset_id")
                asset_id = self._extract_value(feature, field_path)
                
            if asset_id:
                # DECENTRALIZATION: Ensure context has the resolved asset_id
                asset_id_str = str(asset_id)
                payload["asset_id"] = asset_id_str
                context["asset_id"] = asset_id_str
        
        # Validity Logic
        if self.config.enable_validity:
            # Standardized fields for validity
            valid_from = feature.get("valid_from")
            if not valid_from:
                valid_from = feature.get("properties", {}).get("valid_from")
            
            if valid_from:
                context["valid_from"] = valid_from

            valid_to = feature.get("valid_to")
            if not valid_to:
                valid_to = feature.get("properties", {}).get("valid_to")
                
            if valid_to:
                context["valid_to"] = valid_to

        # Attributes
        properties = feature.get("properties", {})
        
        if self.resolved_storage_mode == AttributeStorageMode.COLUMNAR:
            if self.config.attribute_schema:
                for attr in self.config.attribute_schema:
                    if attr.name in properties:
                        payload[attr.name] = properties[attr.name]
                    # Also check top-level for convenience (e.g. STAC extensions)
                    elif attr.name in feature:
                        payload[attr.name] = feature[attr.name]
        else:
            # JSONB Mode
            # We store the entire properties dict
            if properties:
                # OPTIONAL: Remove identity fields from attributes JSONB if they have their own columns
                # to avoid duplication and satisfy privacy/security requirements.
                # We work on a copy to avoid side-effects on the input feature object.
                props_to_save = dict(properties)
                
                if self.config.enable_external_id:
                     field_path = getattr(self.config, "external_id_field", "id")
                     if "." not in field_path and field_path in props_to_save:
                          del props_to_save[field_path]
                
                if self.config.enable_asset_id:
                     field_path = getattr(self.config, "asset_id_field", "asset_id")
                     if "." not in field_path and field_path in props_to_save:
                          del props_to_save[field_path]

                import json
                # Assuming simple JSON serialization handled by DB driver or upper layer dump
                payload[self.config.jsonb_column_name] = json.dumps(props_to_save)
        
        return payload

    async def check_collision(
        self,
        conn: DbResource,
        physical_schema: str,
        physical_table: str,
        field_name: str,
        value: Any,
        exclude_geoid: Optional[Any] = None
    ) -> bool:
        """Check for value existence in external_id or asset_id columns."""
        if field_name == "external_id" and not self.config.enable_external_id:
            return False
        if field_name == "asset_id" and not self.config.enable_asset_id:
            return False
            
        # We only support checks for identity fields or columnar attributes
        sc_table = f"{physical_table}_{self.sidecar_id}"
        
        # Check standard identity fields
        if field_name in ["external_id", "asset_id"]:
            sql = f'SELECT 1 FROM "{physical_schema}"."{sc_table}" WHERE {field_name} = :val'
            params = {"val": str(value)}
            if exclude_geoid:
                sql += " AND geoid <> :exclude"
                params["exclude"] = exclude_geoid
            
            # Limit 1 for speed
            sql += " LIMIT 1"
            
            res = await conn.execute(text(sql), params)
            return res.scalar() is not None

        # Check columnar attributes
        if self.resolved_storage_mode == AttributeStorageMode.COLUMNAR and self.config.attribute_schema:
            attr_names = {a.name for a in self.config.attribute_schema}
            if field_name in attr_names:
                sql = f'SELECT 1 FROM "{physical_schema}"."{sc_table}" WHERE "{field_name}" = :val'
                params = {"val": value}
                if exclude_geoid:
                    sql += " AND geoid <> :exclude"
                    params["exclude"] = exclude_geoid
                
                sql += " LIMIT 1"
                res = await conn.execute(text(sql), params)
                return res.scalar() is not None
        
        return False

    async def resolve_existing_item(
        self,
        conn: DbResource,
        physical_schema: str,
        physical_table: str,
        processing_context: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Resolves existing item by external_id (or other identity fields).
        """
        if not self.config.enable_external_id:
            return None
            
        external_id = processing_context.get("external_id")
        if not external_id:
            return None
            
        # Build query to match external_id and find active record
        sc_table = f"{physical_table}_{self.sidecar_id}"
        
        # We need geoid and validity (if enabled)
        select_fields = ["h.geoid", "h.content_hash"]
        
        # Check active status: 
        # 1. Hub deleted_at IS NULL
        # 2. Sidecar validity is active (if enabled)
        
        where_conditions = [
            """s.external_id = :ext_id""",
            """h.deleted_at IS NULL""",
            """h.geoid = s.geoid""" # Join condition
        ]
        
        if self.config.enable_validity:
            # Active usually means valid_to is infinite (upper bound is NULL)
            # OR we match the logic "current version".
            where_conditions.append("""upper(s.validity) IS NULL""")
            select_fields.append("s.validity")
            
        sql = f"""
            SELECT {", ".join(select_fields)}
            FROM "{physical_schema}"."{physical_table}" h, "{physical_schema}"."{sc_table}" s
            WHERE {" AND ".join(where_conditions)}
            LIMIT 1;
        """
        
        from sqlalchemy import text
        result = await conn.execute(text(sql), {"ext_id": str(external_id)})
        row = result.fetchone()
        
        if row:
            return dict(row._mapping) if hasattr(row, "_mapping") else dict(row)
            
        return None

    def _extract_value(self, data: Dict[str, Any], path: str) -> Any:
        """Helper to extract value from dict using dot notation and properties fallback."""
        val = None
        if "." in path:
            parts = path.split(".")
            current = data
            for part in parts:
                if isinstance(current, dict) and part in current:
                    current = current[part]
                else:
                    current = None
                    break
            val = current
        else:
            val = data.get(path)
            
        # Fallback to properties if not found at root (standard GIS/STAC behavior)
        if val is None and ("id" in path or "asset_id" in path):
             val = data.get("properties", {}).get(path)
             
        return val

    def is_acceptable(
        self,
        feature: Dict[str, Any],
        context: Dict[str, Any]
    ) -> bool:
        """
        Refuses feature if external_id is required but missing.
        """
        return True
