"""
STAC Items Sidecar - Stores external STAC metadata per item.

This sidecar stores user-provided STAC extensions, assets, and per-item
metadata (title, description, keywords) with internationalization support.
"""

import logging
from typing import Dict, Any, List, Optional, Set, Tuple, Union
from datetime import datetime
from pydantic import BaseModel, Field
from geojson_pydantic import Feature

from dynastore.modules.db_config.query_executor import (
    DbResource,
    DQLQuery,
    ResultHandler,
)
from dynastore.modules.catalog.sidecars.registry import SidecarRegistry
from dynastore.modules.catalog.sidecars.base import (
    SidecarProtocol,
    SidecarConfig,
    SidecarConfigRegistry,
    SidecarPipelineContext,
    ValidationResult,
    FieldDefinition,
    FieldCapability,
)

logger = logging.getLogger(__name__)


class StacItemsSidecarConfig(SidecarConfig):
    """Configuration for STAC metadata sidecar."""

    sidecar_type: str = "stac_metadata"


class StacItemsSidecar(SidecarProtocol):
    """
    Sidecar for external STAC extensions and per-item metadata.

    Stores:
    - Per-item title, description, keywords (internationalized)
    - External/user-provided STAC extensions
    - External/user-provided assets
    - Extension-specific fields

    Note: Validity is managed by attributes sidecar (single source of truth).
    """

    def __init__(self, config: StacItemsSidecarConfig):
        self.config = config

    @property
    def sidecar_id(self) -> str:
        return "stac_metadata"

    @property
    def sidecar_type_id(self) -> str:
        return "stac_metadata"

    @classmethod
    def get_default_config(
        cls, context: Dict[str, Any]
    ) -> Optional[StacItemsSidecarConfig]:
        """Auto-inject STAC sidecar if explicitly requested via context."""
        if context.get("stac_context"):
            return StacItemsSidecarConfig()
        return None

    @property
    def provides_feature_id(self) -> bool:
        return False

    @property
    def feature_id_field_name(self) -> Optional[str]:
        return None

    def get_ddl(
        self,
        physical_table: str,
        partition_keys: List[str] = [],
        partition_key_types: Dict[str, str] = {},
        has_validity: bool = False,
    ) -> str:
        """
        Generate DDL for STAC metadata sidecar.

        Follows GeometrySidecar pattern:
        - Composite PK from partition keys + geoid
        - No validity column (uses attributes sidecar)
        - Supports dynamic partitioning from other sidecars
        """

        # Base columns - all JSONB for flexibility and i18n
        columns = [
            "geoid UUID NOT NULL",
            "title JSONB",  # LocalizedText per item
            "description JSONB",  # LocalizedText per item
            "keywords JSONB",  # Array of LocalizedText per item
            "external_extensions JSONB",  # Array of extension URIs
            "external_assets JSONB",  # Asset dict with i18n
            "extra_fields JSONB",  # Extension-specific fields with i18n
        ]

        known_columns = {
            "geoid",
            "title",
            "description",
            "keywords",
            "external_extensions",
            "external_assets",
            "extra_fields",
        }

        # Add validity if versioned (has_validity) OR if it's a partition key
        if has_validity or "validity" in partition_keys:
            columns.append('"validity" TSTZRANGE NOT NULL')
            known_columns.add("validity")

        # Composite PK: partition keys FIRST, then geoid
        pk_columns = []

        if partition_keys:
            # Add partition columns from other sidecars (e.g., h3_res5, s2_res10)
            for key in partition_keys:
                if key not in known_columns:
                    col_type = partition_key_types.get(key, "TEXT")
                    columns.insert(0, f'"{key}" {col_type} NOT NULL')
                    known_columns.add(key)

            # Build PK: partitions + geoid
            for key in partition_keys:
                pk_columns.append(f'"{key}"')

            if "geoid" not in set(partition_keys):
                pk_columns.append('"geoid"')

            partition_clause = (
                f" PARTITION BY LIST ({', '.join([f'"{k}"' for k in partition_keys])})"
            )
        else:
            partition_clause = ""
            pk_columns = ['"geoid"']

        table_name = f"{physical_table}_{self.sidecar_id}"

        create_sql = (
            f'CREATE TABLE IF NOT EXISTS {{schema}}."{table_name}" '
            f"({', '.join(columns)}, PRIMARY KEY ({', '.join(pk_columns)}))"
            f"{partition_clause};"
        )

        # Foreign Key to Hub - only if validity matches or only geoid
        ref_cols = ["geoid"]

        # Determine if validity should be in FK
        # Only if validity is a partition key (shared with Hub)
        if "validity" in partition_keys:
            ref_cols.append("validity")

        fk_sql = (
            f'\nALTER TABLE {{schema}}."{table_name}" '
            f'ADD CONSTRAINT "fk_{table_name}_hub" '
            f"FOREIGN KEY ({', '.join([f'\"{c}\"' for c in ref_cols])}) "
            f'REFERENCES {{schema}}."{physical_table}" ({", ".join([f"\"{c}\"" for c in ref_cols])}) '
            f"ON DELETE CASCADE;"
        )

        ddl = create_sql + fk_sql

        # GIN Indexes for JSONB metadata optimization
        # title, description, keywords, external_extensions, external_assets, extra_fields
        for col in [
            "title",
            "description",
            "keywords",
            "external_extensions",
            "external_assets",
            "extra_fields",
        ]:
            ddl += (
                f'\nCREATE INDEX IF NOT EXISTS "idx_{table_name}_{col}" '
                f'ON {{schema}}."{table_name}" USING GIN({col});'
            )

        return ddl

    def get_select_fields(
        self,
        request: Optional[Any] = None,
        hub_alias: str = "h",
        sidecar_alias: Optional[str] = None,
        include_all: bool = False,
    ) -> List[str]:
        """Return SELECT fields for STAC metadata."""
        alias = sidecar_alias or f"sc_{self.sidecar_id}"

        # In O(1) streaming, we usually want all metadata for mapping
        # unless specifically excluded via request.select filters (not implemented yet for sidecars)
        return [
            f"{alias}.title AS stac_title",
            f"{alias}.description AS stac_description",
            f"{alias}.keywords AS stac_keywords",
            f"{alias}.external_extensions",
            f"{alias}.external_assets",
            f"{alias}.extra_fields AS stac_extra_fields",
        ]

    def get_join_clause(
        self,
        schema: str,
        hub_table: str,
        hub_alias: str = "h",
        sidecar_alias: Optional[str] = None,
        join_type: str = "LEFT",
        extra_condition: Optional[str] = None,
    ) -> str:
        """Generate JOIN clause for STAC metadata sidecar."""
        alias = sidecar_alias or f"sc_{self.sidecar_id}"
        table_name = f"{hub_table}_{self.sidecar_id}"

        on_clause = f"{hub_alias}.geoid = {alias}.geoid"
        if extra_condition:
            on_clause += f" AND {extra_condition}"

        return f'{join_type} JOIN "{schema}"."{table_name}" AS {alias} ON {on_clause}'

    async def validate_upsert(
        self,
        feature: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None,
    ) -> ValidationResult:
        """Validate STAC metadata before upsert."""
        errors = []
        warnings = []

        # Check if STAC-specific fields are provided correctly
        if "assets" in feature and not isinstance(feature["assets"], dict):
            errors.append("STAC assets must be an object")

        if "stac_extensions" in feature and not isinstance(
            feature["stac_extensions"], list
        ):
            errors.append("stac_extensions must be an array")

        return ValidationResult(
            valid=len(errors) == 0,
            error="; ".join(errors) if errors else None,
        )

    async def setup_lifecycle_hooks(
        self, conn: Any, schema: str, table_name: str
    ) -> None:
        """No specific lifecycle hooks needed for this sidecar."""
        pass

    async def on_partition_create(
        self,
        conn: Any,
        schema: str,
        parent_table: str,
        partition_table: str,
        partition_value: Any,
    ) -> None:
        """No specific action needed on partition create."""
        pass

    def resolve_query_path(self, attr_name: str) -> Optional[Tuple[str, str]]:
        """Resolves an attribute reference to SQL and JOIN requirements."""
        alias = f"sc_{self.sidecar_id}"
        if attr_name in ["stac_title", "title"]:
            return (f"{alias}.title", alias)
        if attr_name in ["stac_description", "description"]:
            return (f"{alias}.description", alias)
        if attr_name == "external_extensions":
            return (f"{alias}.external_extensions", alias)
        return None

    def apply_query_context(
        self,
        request: Any,
        context: Dict[str, Any],
    ) -> None:
        """
        Populates query context with STAC-specific selections and joins.
        """
        # If any stac-specific field is requested, or if select * is used
        is_select_all = not request.select or any(s.field == "*" for s in request.select)
        
        # We also check if any "stac_*" field is explicitly requested
        has_stac_request = any(s.field.startswith("stac_") or s.field in ["assets", "links"] for s in request.select)
        
        if is_select_all or has_stac_request:
            sc_alias = f"sc_{self.sidecar_id}"
            
            # 1. Add SELECT fields
            context["select_fields"].extend(self.get_select_fields(request, hub_alias="h", sidecar_alias=sc_alias))
            
            # 2. Ensure JOIN is added (handled by QueryOptimizer default logic, 
            # but we can explicitly add it here if we want specialized join conditions)
            pass

    def get_queryable_fields(self) -> Dict[str, FieldDefinition]:
        """Return queryable field definitions."""
        return {
            "stac_title": FieldDefinition(
                name="stac_title",
                data_type="jsonb",
                sql_expression="sc_stac_metadata.title",
                capabilities={FieldCapability.FILTERABLE},
                description="Item title (internationalized)",
            ),
            "stac_description": FieldDefinition(
                name="stac_description",
                data_type="jsonb",
                sql_expression="sc_stac_metadata.description",
                capabilities={FieldCapability.FILTERABLE},
                description="Item description (internationalized)",
            ),
            "external_extensions": FieldDefinition(
                name="external_extensions",
                data_type="jsonb",
                sql_expression="sc_stac_metadata.external_extensions",
                capabilities={FieldCapability.FILTERABLE},
                description="External STAC extension URIs",
            ),
        }

    def get_feature_type_schema(self) -> Dict[str, Any]:
        """Return JSON Schema fragment for Feature output."""
        return {
            "stac_title": {
                "type": "object",
                "additionalProperties": {"type": "string"},
            },
            "stac_description": {
                "type": "object",
                "additionalProperties": {"type": "string"},
            },
            "stac_keywords": {
                "type": "array",
                "items": {"type": "object", "additionalProperties": {"type": "string"}},
            },
            "external_extensions": {"type": "array", "items": {"type": "string"}},
            "external_assets": {"type": "object"},
            "stac_extra_fields": {"type": "object"},
        }

    def get_identity_columns(self) -> List[str]:
        """Returns the list of columns that form the identity (PK)."""
        # Note: In a real implementation we should get partition keys from config
        # but for now we follow the general pattern.
        return ["geoid"]

    def prepare_upsert_payload(
        self, feature: Any, context: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Extract STAC metadata and prune managed assets.
        Called automatically by ItemService.upsert if registered.
        """
        geoid = context.get("geoid")
        if not geoid:
            raise ValueError("geoid is required in context for STAC sidecar")

        # Convert feature to dict if it's a model
        if hasattr(feature, "dict"):
            data = feature.dict(by_alias=True)
        elif hasattr(feature, "model_dump"):
            data = feature.model_dump(by_alias=True)
        else:
            data = dict(feature)

        # 1. Get STAC extension providers
        from dynastore.tools.discovery import get_protocols
        from .stac_extension_protocol import StacExtensionProtocol

        providers = get_protocols(StacExtensionProtocol)

        # 2. Prune managed content
        from .metadata_helpers import (
            prune_managed_content_sync,
            prune_stac_managed_properties,
        )

        pruned = prune_managed_content_sync(data, providers)

        # 3. In-place pruning of properties to avoid duplicate storage in generic sidecars
        if isinstance(feature, dict) and "properties" in feature:
            prune_stac_managed_properties(feature["properties"], providers)
        elif hasattr(feature, "properties") and isinstance(feature.properties, dict):
            prune_stac_managed_properties(feature.properties, providers)

        # 3. Build payload
        payload = {
            "geoid": geoid,
            "title": pruned["title"],
            "description": pruned["description"],
            "keywords": pruned["keywords"],
            "external_extensions": pruned["external_extensions"],
            "external_assets": pruned["external_assets"],
            "extra_fields": pruned["extra_fields"],
        }

        # Add partition keys if present in context
        if context.get("partition_key_name"):
            payload[context["partition_key_name"]] = context["partition_key_value"]

        # Clean None values (except JSONB fields where we might want null)
        # Actually in PG JSONB can be NULL, which is fine.

        import json
        from dynastore.extensions.tools.fast_api import CustomJSONEncoder

        # Serialize JSONB fields for DB driver
        for field in [
            "title",
            "description",
            "keywords",
            "external_extensions",
            "external_assets",
            "extra_fields",
        ]:
            if payload[field] is not None:
                payload[field] = json.dumps(payload[field], cls=CustomJSONEncoder)

        return payload

    def finalize_upsert_payload(
        self,
        sc_payload: Dict[str, Any],
        hub_row: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Finalizes STAC payload by injecting validity from Hub or context."""
        payload = sc_payload.copy()

        # Inject validity if managed by this sidecar
        # Note: We check if validity column was added by get_ddl logic
        if "validity" in context or "validity" in hub_row:
            validity = hub_row.get("validity") or context.get("validity")
            if validity:
                payload["validity"] = validity

        return payload

    async def expire_version(
        self,
        conn: DbResource,
        physical_schema: str,
        physical_table: str,
        geoid: str,
        expire_at: datetime,
    ) -> int:
        """Marks the current active version as expired in the STAC sidecar table."""
        # Note: we assume STAC sidecar follows versioning if validity is present
        sc_table = f"{physical_table}_{self.sidecar_id}"

        # Check if validity column exists in this table (approximated by checking if it's in partition keys
        # or if we want to be safe, just try the update and catch error, or check config)
        # Actually, if we follow AttributesSidecar pattern:
        sql = f'UPDATE "{physical_schema}"."{sc_table}" SET validity = tstzrange(lower(validity), :expire_at, \'[)\') WHERE geoid = :geoid AND upper(validity) IS NULL'

        from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler

        try:
            return await DQLQuery(sql, result_handler=ResultHandler.ROWCOUNT).execute(
                conn, geoid=geoid, expire_at=expire_at
            )
        except Exception as e:
            # If validity column doesn't exist, it's not versioned
            if 'column "validity" does not exist' in str(e).lower():
                return 0
            raise

    def get_internal_columns(self) -> set:
        """Columns owned by this sidecar that are never part of Feature properties."""
        return {
            "stac_title",
            "stac_description",
            "stac_keywords",
            "external_extensions",
            "external_assets",
            "stac_extra_fields",
        }

    def map_row_to_feature(
        self,
        row: Dict[str, Any],
        feature: Feature,
        context: SidecarPipelineContext,
    ) -> None:
        """Merge STAC metadata from database row back into GeoJSON feature."""
        # Publish all raw row values under this sidecar's key in context.
        context.publish(self.sidecar_id, dict(row))

        import json

        def _maybe_parse(v):
            """Parse JSON string → Python object if needed (SQLAlchemy streaming doesn't auto-parse JSONB)."""
            if isinstance(v, str):
                try:
                    return json.loads(v)
                except (ValueError, TypeError):
                    return v
            return v

        if feature.properties is None:
            feature.properties = {}

        from dynastore.models.localization import localize_dict

        from dynastore.tools.language_utils import resolve_localized_field

        # 1. Metadata Merging (title, description, keywords)
        available_langs = set()
        for field in ["title", "description", "keywords"]:
            val = row.get(f"stac_{field}") or row.get(field)
            if val is not None:
                parsed = _maybe_parse(val)
                # Localize if it's a dict
                if isinstance(parsed, dict):
                    # Collect available languages before resolving
                    # Filter for 2-char keys to avoid custom metadata fields that might look like dicts
                    available_langs.update([k for k in parsed.keys() if isinstance(k, str) and len(k) == 2])
                    
                    # Use resolve_localized_field which returns a single value or dict, NOT a tuple
                    resolved = resolve_localized_field(parsed, context.lang)
                    if resolved is not None:
                        feature.properties[field] = resolved
                else:
                    feature.properties[field] = parsed
        
        # Publish available languages to context for extension generators
        if available_langs:
            context.publish("stac_available_langs", list(available_langs))

        # 2. Extensions Merging
        raw_exts = row.get("external_extensions")
        exts = _maybe_parse(raw_exts) if raw_exts else []
        if exts:
            existing = getattr(feature, "stac_extensions", []) or []
            if not existing:
                existing = feature.properties.get("stac_extensions", []) or []

            # Merge and deduplicate
            merged_exts = list(set(existing + exts))
            
            try:
                feature.stac_extensions = merged_exts
            except Exception:
                feature.properties["stac_extensions"] = merged_exts

        # 3. Assets Merging
        assets = _maybe_parse(row.get("external_assets"))
        if assets:
            try:
                # Localize asset titles/descriptions if they are dicts
                for asset_key, asset_val in assets.items():
                    if isinstance(asset_val, dict):
                        for sub_field in ["title", "description"]:
                            if sub_field in asset_val and isinstance(asset_val[sub_field], dict):
                                asset_val[sub_field] = resolve_localized_field(asset_val[sub_field], context.lang)

                curr_assets = getattr(feature, "assets", None)
                if curr_assets is None:
                    if "assets" in feature.properties and isinstance(feature.properties["assets"], dict):
                        curr_assets = feature.properties["assets"]
                        try:
                            setattr(feature, "assets", curr_assets)
                            del feature.properties["assets"]
                        except Exception:
                            pass
                    else:
                        try:
                            setattr(feature, "assets", {})
                            curr_assets = getattr(feature, "assets")
                        except Exception:
                            pass

                if curr_assets is not None and isinstance(curr_assets, dict):
                    curr_assets.update(assets)
                elif curr_assets is not None:
                    for k, v in assets.items():
                        try:
                            setattr(curr_assets, k, v)
                        except Exception:
                            pass
                else:
                    if "assets" not in feature.properties:
                        feature.properties["assets"] = {}
                    feature.properties["assets"].update(assets)
            except Exception as e:
                logger.warning(f"Failed to merge STAC assets: {e}")
                if "assets" not in feature.properties:
                    feature.properties["assets"] = {}
                feature.properties["assets"].update(assets)

        # 4. Extra Fields
        extra = _maybe_parse(row.get("stac_extra_fields") or row.get("extra_fields"))
        if extra:
            top_level_stac = {"stac_version", "stac_extensions", "collection", "bbox", "links"}
            for k, v in extra.items():
                # Localize extra fields if they are dicts
                if isinstance(v, dict):
                    # Only resolve if it looks like a language dict (heuristic: all keys are 2 chars)
                    if all(isinstance(k2, str) and len(k2) == 2 for k2 in v.keys() if k2):
                        v = resolve_localized_field(v, context.lang)

                if k in top_level_stac:
                    try:
                        setattr(feature, k, v)
                    except Exception:
                        feature.properties[k] = v
                else:
                    feature.properties[k] = v


# Register for polymorphic resolution
SidecarConfigRegistry.register("stac_metadata", StacItemsSidecarConfig)
SidecarRegistry.register("stac_metadata", StacItemsSidecar)
