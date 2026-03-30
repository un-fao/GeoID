"""
STAC Items Sidecar — lightweight overlay for STAC-specific item fields.

This sidecar does **not** own a database table.  It reads raw metadata
published by the core ``ItemMetadataSidecar`` via the pipeline context
and injects STAC-specific fields (extensions, assets, extra_fields) into
the Feature.  Generic multilanguage resolution (title, description,
keywords) is handled entirely by ``ItemMetadataSidecar``.
"""

import json
import logging
from typing import Dict, Any, List, Optional, Set, Tuple, Union
from datetime import datetime

from geojson_pydantic import Feature

from dynastore.modules.catalog.sidecars.registry import SidecarRegistry
from dynastore.modules.catalog.sidecars.base import (
    SidecarProtocol,
    SidecarConfig,
    SidecarConfigRegistry,
    SidecarPipelineContext,
    ConsumerType,
    ValidationResult,
    FieldDefinition,
    FieldCapability,
)
from dynastore.modules.db_config.query_executor import DbResource

logger = logging.getLogger(__name__)

# STAC-specific output fields that must NOT appear in OGC Features responses.
# Kept for defense-in-depth stripping at the Features extension layer.
STAC_FEATURES_STRIP: Set[str] = {
    "stac_extensions",
    "stac_version",
    "assets",
}


class StacItemsSidecarConfig(SidecarConfig):
    """Configuration for the STAC overlay sidecar."""

    sidecar_type: str = "stac_metadata"


class StacItemsSidecar(SidecarProtocol):
    """
    Lightweight STAC overlay sidecar.

    Reads from ``context.get_sidecar("item_metadata")`` and injects:
    - ``stac_extensions`` from ``external_extensions``
    - ``assets`` from ``external_assets`` (with multilang resolution)
    - Extra fields (``stac_version``, extension-specific properties)

    This sidecar has **no DDL, no SELECT, no JOIN** of its own.
    It only runs during ``map_row_to_feature`` and is gated on
    ``context.consumer`` to avoid injecting STAC fields into OGC
    Features responses.
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
        """Auto-inject STAC overlay if explicitly requested via context."""
        if context.get("stac_context"):
            return StacItemsSidecarConfig()
        return None

    def is_mandatory(self) -> bool:
        """Not mandatory — has no table of its own."""
        return False

    @property
    def provides_feature_id(self) -> bool:
        return False

    @property
    def feature_id_field_name(self) -> Optional[str]:
        return None

    # ── No table of its own ──────────────────────────────────────────────

    def get_ddl(
        self,
        physical_table: str,
        partition_keys: List[str] = [],
        partition_key_types: Dict[str, str] = {},
        has_validity: bool = False,
    ) -> str:
        """No DDL — table is owned by ItemMetadataSidecar."""
        return ""

    def get_select_fields(
        self,
        request: Optional[Any] = None,
        hub_alias: str = "h",
        sidecar_alias: Optional[str] = None,
        include_all: bool = False,
    ) -> List[str]:
        """No SELECT — reads from pipeline context."""
        return []

    def get_join_clause(
        self,
        schema: str,
        hub_table: str,
        hub_alias: str = "h",
        sidecar_alias: Optional[str] = None,
        join_type: str = "LEFT",
        extra_condition: Optional[str] = None,
    ) -> str:
        """No JOIN — reads from pipeline context."""
        return ""

    # ── Validation ───────────────────────────────────────────────────────

    async def validate_upsert(
        self,
        feature: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None,
    ) -> ValidationResult:
        errors = []
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
        pass

    async def on_partition_create(
        self,
        conn: Any,
        schema: str,
        parent_table: str,
        partition_table: str,
        partition_value: Any,
    ) -> None:
        pass

    # ── Query support (reads from ItemMetadataSidecar context) ───────────

    def resolve_query_path(self, attr_name: str) -> Optional[Tuple[str, str]]:
        return None

    def apply_query_context(
        self,
        request: Any,
        context: Dict[str, Any],
    ) -> None:
        pass

    def get_queryable_fields(self) -> Dict[str, FieldDefinition]:
        return {}

    def get_feature_type_schema(self) -> Dict[str, Any]:
        return {
            "stac_extensions": {"type": "array", "items": {"type": "string"}},
            "assets": {"type": "object"},
        }

    def get_identity_columns(self) -> List[str]:
        return ["geoid"]

    # ── Upsert delegates to ItemMetadataSidecar ──────────────────────────

    def prepare_upsert_payload(
        self, feature: Any, context: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """No-op — writes are handled by ItemMetadataSidecar."""
        return None

    def finalize_upsert_payload(
        self,
        sc_payload: Dict[str, Any],
        hub_row: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        return sc_payload

    async def expire_version(
        self,
        conn: DbResource,
        physical_schema: str,
        physical_table: str,
        geoid: str,
        expire_at: datetime,
    ) -> int:
        """No-op — versioning handled by ItemMetadataSidecar."""
        return 0

    def get_internal_columns(self) -> set:
        return set()

    # ── The core method: inject STAC fields from context ─────────────────

    def map_row_to_feature(
        self,
        row: Dict[str, Any],
        feature: Feature,
        context: SidecarPipelineContext,
    ) -> None:
        """Inject STAC-specific fields into the Feature.

        Reads raw metadata from the ``item_metadata`` sidecar's context
        publication.  Skips injection entirely for OGC Features consumers.
        """
        # For OGC Features consumers, do nothing — generic metadata
        # (title/desc/keywords) was already resolved by ItemMetadataSidecar.
        if context.consumer == ConsumerType.OGC_FEATURES:
            return

        # Read raw data published by ItemMetadataSidecar
        metadata = context.get_sidecar("item_metadata")

        # Also check the row directly for backward compat (legacy column aliases)
        def _get(key: str):
            if metadata:
                v = metadata.get(key)
                if v is not None:
                    return v
            return row.get(key)

        def _maybe_parse(v):
            if isinstance(v, str):
                try:
                    return json.loads(v)
                except (ValueError, TypeError):
                    return v
            return v

        if feature.properties is None:
            feature.properties = {}

        from dynastore.tools.language_utils import resolve_localized_field
        from dynastore.models.localization import is_valid_language_key

        # 1. Extensions Merging
        raw_exts = _get("external_extensions")
        exts = _maybe_parse(raw_exts) if raw_exts else []
        if exts:
            existing: List[str] = getattr(feature, "stac_extensions", []) or []
            if not existing:
                existing = feature.properties.get("stac_extensions", []) or []
            exts_list: List[str] = exts if isinstance(exts, list) else ([exts] if isinstance(exts, str) else [])
            merged_exts = list(set([*existing, *exts_list]))
            try:
                feature.stac_extensions = merged_exts
            except Exception:
                feature.properties["stac_extensions"] = merged_exts

        # 2. Assets Merging
        assets = _maybe_parse(_get("external_assets"))
        if isinstance(assets, dict):
            try:
                for asset_key, asset_val in assets.items():
                    if isinstance(asset_val, dict):
                        for sub_field in ["title", "description"]:
                            if sub_field in asset_val and isinstance(
                                asset_val[sub_field], dict
                            ):
                                asset_val[sub_field] = resolve_localized_field(
                                    asset_val[sub_field], context.lang
                                )

                curr_assets = getattr(feature, "assets", None)
                if curr_assets is None:
                    if "assets" in feature.properties and isinstance(
                        feature.properties["assets"], dict
                    ):
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

        # 3. Extra Fields
        extra = _maybe_parse(
            _get("item_extra_fields")
            or _get("stac_extra_fields")
            or _get("extra_fields")
        )
        if isinstance(extra, dict):
            top_level_stac = {
                "stac_version",
                "stac_extensions",
                "collection",
                "bbox",
                "links",
            }
            for k, v in extra.items():
                if isinstance(v, dict):
                    if all(
                        isinstance(k2, str) and is_valid_language_key(k2)
                        for k2 in v.keys()
                        if k2
                    ):
                        v = resolve_localized_field(v, context.lang)

                if k in top_level_stac:
                    try:
                        setattr(feature, k, v)
                    except Exception:
                        feature.properties[k] = v
                else:
                    feature.properties[k] = v

        # Publish to context for downstream consumers (e.g. STAC generator)
        context.publish(self.sidecar_id, {
            "stac_extensions": getattr(feature, "stac_extensions", None)
            or feature.properties.get("stac_extensions"),
            "assets": getattr(feature, "assets", None)
            or feature.properties.get("assets"),
        })


# Register for polymorphic resolution
SidecarConfigRegistry.register("stac_metadata", StacItemsSidecarConfig)
SidecarRegistry.register("stac_metadata", StacItemsSidecar)
