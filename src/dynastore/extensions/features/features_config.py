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

from typing import List, Optional
from pydantic import Field
from dynastore.modules.db_config.platform_config_manager import PluginConfig, register_config

FEATURES_PLUGIN_CONFIG_ID = "features"

@register_config(FEATURES_PLUGIN_CONFIG_ID)
class FeaturesPluginConfig(PluginConfig):
    """
    Runtime configuration for the OGC Features extension.
    Controls caching and visibility.
    """
    enabled: bool = Field(True, description="If False, Features requests will be rejected.")
    
    # Caching
    cache_on_demand: bool = Field(
        False,
        description="If True, generated Features responses are saved to the bucket storage."
    )
    
    storage_priority: List[str] = Field(
        default=["bucket"], 
        description="Priority list of storage providers to use for saving cached responses."
    )
