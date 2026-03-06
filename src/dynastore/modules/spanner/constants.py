from enum import Enum
import os
import json
from uuid import UUID


# TODO To be able to dynamically manage these soon most of enums might become dicts
SPANNER_NULL_STR = 'NULL'
# Table names
ASSET_TABLE_NAME="asset"
ASSET_TYPE_TABLE_NAME = "asset_type"
ASSET_CLASS_TABLE_NAME = "asset_class"
ASSET_WORKFLOW_EDGE_TABLE_NAME = "asset_workflow_edge"
ASSET_WORKFLOW_EDGE_ATTRIBUTE_TABLE_NAME = "asset_workflow_edge_attribute"
ASSET_WORKFLOW_STATE_TABLE_NAME = "asset_workflow_state"
ASSET_WORKFLOW_TYPE_TABLE_NAME = "asset_workflow_type"
ASSET_WORKFLOW_STATE_HISTORY_TABLE_NAME = "asset_workflow_state_history"
ASSET_WORKFLOW_CONFIG_TABLE_NAME = "asset_workflow_config"
ASSET_PROPERTY_TABLE_NAME="asset_property"
ASSET_EDGE_ATTRIBUTE_TABLE_NAME="asset_edge_attribute"
ASSET_EDGE_TABLE_NAME="asset_edge"
ASSET_RELATION_TYPE_TABLE_NAME="asset_relation_type"
ASSET_WORKFLOW_STATE_TYPE_TABLE_NAME="asset_workflow_state_type"
LANGUAGE_TABLE_NAME = "language"
VOCABULARY_TABLE_NAME = "vocabulary"
TERM_TABLE_NAME = "term"
TERM_TRANSLATION_TABLE_NAME = "term_translation"
CONTEXT_TABLE_NAME = "context"
CONDITION_TABLE_NAME = "condition"
PERMISSION_TABLE_NAME = "permission"
PERMISSION_EDGE_TABLE_NAME = "permission_edge"
PERMISSION_BIT_MAP_TABLE_NAME = "permission_bit_map"
ASSET_PRINCIPAL_LINK_TABLE_NAME = "asset_principal_link"
ASSET_PRINCIPAL_PERMISSION_TABLE_NAME = "asset_principal_permission"
PRINCIPAL_TABLE_NAME = "principal"
PRINCIPAL_TYPE_TYPE_TABLE_NAME = "principal_type"
PRINCIPAL_EDGE_TABLE_NAME = "principal_edge"
GENERIC_PERMISSION_BIT_TABLE_NAME = "generic_permission_bit"

# Graphs
WORKFLOW_GRAPH_NAME = "workflow_graph"
ASSET_RELATION_GRAPH_NAME = "asset_graph"

# Graph visualization keys
GRAPH_NODE_UUID_KEY = "uuid"
GRAPH_NODE_COLOR_KEY = "color"
GRAPH_NODE_LABEL_KEY = "label"
GRAPH_EDGE_SOURCE_KEY = "source"
GRAPH_EDGE_TARGET_KEY = "target"
GRAPH_EDGE_LABEL_KEY = "label"
GRAPH_EDGE_TITLE_KEY = "title"

# Codes

WORKFLOW_EDGE_ATTRIBUTE_INITIAL_CODE = "initial"

# Well known and common properties
NAME_PROPERTY_CODE="name"
DATA_PROPERTY_CODE = "data"
OPT_PROPERTY_CODE = "opt"
TAGS_PROPERTY_CODE = "tags"
DEF_PROPERTY_CODE = "def"
REGISTRY_PROPERTY_CODE = "registry"

# TODO If we need to have a second version of  a profile, we can use a second property lavel as below to match them
#CONFIG-1:profile=iso,label=jsonschema-iso
#CONFIG-2:profile=iso2,label=jsonschema-iso
PROFILE_PROPERTY_CODE = "profile"
MIMETYPE_PROPERTY_CODE = "mimetype"
LICENSE_ID_PROPERTY_CODE = "license_id"

# Vocabulary Codes 
NAME_TERM_CODE="name"
DESCRIPTION_TERM_CODE = "description"

CANONICAL_LANGUAGE_CODE_KEY = "canonical_language"
DEFAULT_CANONICAL_LANGUAGE_CODE = "en"

# Keywords
SPANNER_GRAPH_PROPS_KW="properties"

# TO BE DELETED
SYSTEM_PRINCIPAL_UUID = UUID("550e8400-e29b-41d4-a716-000000000000")

class AssetClassCodes(Enum):
    CONFIG = 1
    DOCUMENT = 2
    SCHEMA = 3
    TEMPLATE = 4
    VOCABULARY = 5
    FILE = 6
    WORKSPACE = 7
    GCP = 8
    
class AssetTypeCodes(Enum):
    TEAM = 1
    METADATA = 2
    RESOURCE = 3
    VIEW = 4
    LICENSE = 5
    SHAPEFILE = 6
    CATALOG = 7
    BUCKET = 8
    MULTILINGUAL = 9
    REGISTRY = 10
class StateCodes(Enum):
    ACTIVE = 1
    DELETED = 2
    ARCHIVED = 3

    @staticmethod
    def get_enum_code():
        return "state"

class AssetRelationCodes(Enum):
    HAS = 0
    HAS_METADATA = 1          # matches 'has_metadata'
    HAS_RESOURCE = 2         # matches 'has_resource'
    HAS_SCHEMA = 3           # matches 'has_schema'
    HAS_GENERATOR = 4        # matches 'has_generator'
    HAS_RENDERER = 5         # matches 'has_renderer'
    HAS_CONFIG = 6           # matches 'has_config'
    HAS_LICENSE = 7          # matches 'has_license'
    DEFINES_VOCABULARY = 8   # matches 'defines_vocabulary'
    HAS_SHAPEFILE = 9           
    HAS_BUCKET = 10          # matches 'has_bucket'
    HAS_FILE = 11
    HAS_REGISTRY = 12
    POINTS_TO = 13
    HAS_CATALOG = 14
    HAS_VIEW = 15

    def to_dict(self):
        return {
            "name": self.name,
            "value": self.value
        }

class PropertyDataTypes(Enum):
    STRING = "string"
    OBJECT = "object"
    ARRAY = "array"
    XML = "xml"
    NUMBER = "number"
    BOOLEAN = "boolean"

class PrincipalTypes(Enum):
    USER = 0
    SERVICE_ACCOUNT = 1
    GROUP = 2

class PermissionActionTypes(Enum):
    ALLOW = "ALLOW"
    DENY = "DENY"

class PermissionTypeCodes(Enum):
    PERMISSION = "PERMISSION"
    ROLE = "ROLE"

class LanguageCodes(Enum):
    EN = {"id": 1, "name": "ENGLISH"}
    ES = {"id": 2, "name": "SPANISH"}
    FR = {"id": 3, "name": "FRENCH"}
    CH = {"id": 4, "name": "CHINESE"}

    def get_id(self):
        # returns the "id" part of the enum member dictionary
        return self.value["id"]

    def get_name(self):
        # returns the "name" part of the enum member dictionary
        return self.value["name"]

    @classmethod
    def get_language_by_id(cls, id_to_find: int):
        for lang in cls:
            if lang.get_id() == id_to_find:
                return lang
        raise ValueError(f"No language with id {id_to_find} found")

class AssetWorkflowStateCodes(Enum):
    UPLOADING = 0
    CREATED = 1
    ARCHIVE = 2
    WITHDRAWN = 3
    DRAFT = 4
    WIP = 5
    QA = 6
    RELEASED = 7
    PUBLISHED = 8
    ABORTED = 9
    INGESTING = 10
    INGESTED = 11

class TypeAttributeCodes(Enum):
    TREE = "tree" # has a single parent (not always) puts the rule that the existence of the child require the existence of the parent or parents
    GRAPH = "graph" # can have multiple parents defines mostly many-to-many,  the existence of the child does not require the existence of any parent

    @staticmethod
    def get_enum_code():
        return "type"
