import dynastore.modules.spanner.constants as _c
from enum import Enum
import os

default_lang_code = _c.LanguageCodes.EN
legacy_static_file_path = os.path.join(os.path.dirname(__file__), 'static')
CKAN_TEAM_PROFILE_CODE = "CKAN-organization"
CKAN_REGISTRY_CODE = "CKAN"
CKAN_ACTIVE_CODE = "active"
CKAN_DELETED_CODE = "deleted"
JSONSCHEMA_BODY_CODE = "jsonschema_body"
JSONSCHEMA_TYPE_CODE = "jsonschema_type"
JSONSCHEMA_OPT_CODE = "jsonschema_opt"

CKAN_PRIVATE_FLAG_CODE = "private"
CKAN_USER_ID_PREFIX = "ckan_user"
CKAN_METADATA_TYPE_CODE = "type"
CKAN_RESOURCE_TYPE_CODE = "jsonschema_type"
CKAN_DEFAULT_RESOURCE_TYPE = "dataset-resource"
CKAN_LICENSE_CONFIG_CODE = "ckan_license_config"
CKAN_REGISTRY_PROPERTY_VALUE = "CKAN"
ETL_DEFAULT_WORKFLOW_NAME = "simple_workflow"
class DatasetType(Enum):
    DATASET="dataset"
    ISO="iso"
    