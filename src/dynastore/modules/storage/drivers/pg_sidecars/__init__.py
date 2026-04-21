from .base import (
    SidecarProtocol,
    SidecarConfig,
    FeaturePipelineContext,
    ConsumerType,
    ValidationResult,
    FieldDefinition,
    FieldCapability,
)
from .geometries import GeometriesSidecar
from .attributes import FeatureAttributeSidecar
from .item_metadata import ItemMetadataSidecar
from .geometries_config import GeometriesSidecarConfig
from .attributes_config import FeatureAttributeSidecarConfig
from .item_metadata_config import ItemMetadataSidecarConfig
from .registry import SidecarRegistry
from .resolver import _effective_sidecars

# Deprecated alias — remove in next major version
SidecarPipelineContext = FeaturePipelineContext

__all__ = [
    "SidecarProtocol",
    "SidecarConfig",
    "FeaturePipelineContext",
    "SidecarPipelineContext",  # deprecated alias
    "ConsumerType",
    "ValidationResult",
    "FieldDefinition",
    "FieldCapability",
    "GeometriesSidecar",
    "FeatureAttributeSidecar",
    "ItemMetadataSidecar",
    "GeometriesSidecarConfig",
    "FeatureAttributeSidecarConfig",
    "ItemMetadataSidecarConfig",
    "SidecarRegistry",
    "_effective_sidecars",
]
