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

from typing import List, Optional, Any, Literal, Dict, Type
from pydantic import BaseModel, Field, model_validator
from dynastore.modules.db_config.platform_config_manager import PluginConfig, register_config
from dynastore.modules.catalog.catalog_config import VersioningBehaviorEnum

# --- Configuration Identifier ---
INGESTION_CONFIG_ID = "ingestion"

# --- Main Ingestion Config ---

@register_config(INGESTION_CONFIG_ID)
class IngestionPluginConfig(PluginConfig):
    """
    Configuration for the Ingestion Module.
    This can be updated at any time via API.
    """
    # 1. Behavior Rules
    versioning_behavior: VersioningBehaviorEnum = Field(
        default=VersioningBehaviorEnum.ALWAYS_ADD_NEW,
        description="Determines how new features with an existing external_id are handled."
    )