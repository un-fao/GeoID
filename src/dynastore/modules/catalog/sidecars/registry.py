#    Copyright 2026 FAO
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

from typing import Dict, Type, Any

from dynastore.modules.catalog.sidecars.base import SidecarProtocol, SidecarConfig
from dynastore.modules.catalog.sidecars.geometry import GeometrySidecar
from dynastore.modules.catalog.sidecars.attributes import FeatureAttributeSidecar
from dynastore.modules.catalog.sidecars.geometry_config import GeometrySidecarConfig
from dynastore.modules.catalog.sidecars.attributes import FeatureAttributeSidecarConfig

class SidecarRegistry:
    """
    Centralized registry for resolving Sidecar Configurations to their
    Implementation classes.
    """
    
    # Map Config Type -> Implementation Class
    _registry: Dict[Type[SidecarConfig], Type[SidecarProtocol]] = {
        GeometrySidecarConfig: GeometrySidecar,
        FeatureAttributeSidecarConfig: FeatureAttributeSidecar
    }
    
    @classmethod
    def get_sidecar(cls, config: SidecarConfig) -> SidecarProtocol:
        """
        Factory method to instantiate the correct SidecarProtocol implementation
        for a given configuration object.
        """
        config_type = type(config)
        sidecar_cls = cls._registry.get(config_type)
        
        if not sidecar_cls:
            # Fallback: try matching by sidecar_type string if needed (for dynamic loading?)
            # For now, explicit type mapping is safer and cleaner with Pydantic.
            raise ValueError(f"No sidecar implementation registered for config type: {config_type}")
            
        return sidecar_cls(config)

    @classmethod
    def register(cls, config_cls: Type[SidecarConfig], impl_cls: Type[SidecarProtocol]):
        """Register a new sidecar type dynamically."""
        cls._registry[config_cls] = impl_cls
