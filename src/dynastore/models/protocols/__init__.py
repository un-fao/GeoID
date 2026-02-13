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
Centralized protocol definitions for DynaStore.

This package contains protocol definitions organized by domain:
- catalogs.py: Catalog-related protocols
- assets.py: Asset management protocols
- configs.py: Configuration management protocols
- logs.py: Logging protocols
- storage.py: Storage protocols
- job_execution.py: Serverless job execution protocols (optional, enterprise)
- cloud_storage_client.py: Cloud storage client access protocols (optional, enterprise)
- cloud_identity.py: Cloud identity and context protocols (optional, enterprise)
- eventing.py: Cloud eventing and notification protocols (optional, enterprise)
"""

from dynastore.models.protocols.catalogs import CatalogsProtocol
from dynastore.models.protocols.items import ItemsProtocol
from dynastore.models.protocols.collections import CollectionsProtocol
from dynastore.models.protocols.assets import AssetsProtocol
from dynastore.models.protocols.configs import ConfigsProtocol
from dynastore.models.protocols.logs import LogsProtocol
from dynastore.models.protocols.storage import StorageProtocol
from dynastore.models.protocols.job_execution import JobExecutionProtocol
from dynastore.models.protocols.cloud_storage_client import CloudStorageClientProtocol
from dynastore.models.protocols.cloud_identity import CloudIdentityProtocol
from dynastore.models.protocols.eventing import EventingProtocol
from dynastore.models.protocols.apikey import ApiKeyProtocol
from dynastore.models.protocols.proxy import ProxyProtocol
from dynastore.models.protocols.database import DatabaseProtocol
from dynastore.models.protocols.properties import PropertiesProtocol
from dynastore.models.protocols.localization import LocalizationProtocol

# Export auth_models for convenience
from dynastore.models.auth_models import (
    SYSTEM_USER_ID,
    ApiKeyPolicy,
    Role,
    IdentityAuthorization,
    IdentityLink,
    TokenExchangeRequest,
    TokenResponse,
    ApiKey,
    ApiKeyCreate,
    ApiKeyStatus,
    ApiKeyValidationRequest,
    ApiKeyStatusFilter,
    RefreshToken,
)

__all__ = [
    "CatalogsProtocol",
    "ItemsProtocol",
    "CollectionsProtocol",
    "AssetsProtocol",
    "ConfigsProtocol",
    "LogsProtocol",
    "StorageProtocol",
    "JobExecutionProtocol",
    "CloudStorageClientProtocol",
    "CloudIdentityProtocol",
    "EventingProtocol",
    "ApiKeyProtocol",
    "ProxyProtocol",
    "DatabaseProtocol",
    "PropertiesProtocol",
    "LocalizationProtocol",
    # Auth models
    "SYSTEM_USER_ID",
    "ApiKeyPolicy",
    "Role",
    "IdentityAuthorization",
    "IdentityLink",
    "TokenExchangeRequest",
    "TokenResponse",
    "ApiKey",
    "ApiKeyCreate",
    "ApiKeyStatus",
    "ApiKeyValidationRequest",
    "ApiKeyStatusFilter",
    "RefreshToken",
]
