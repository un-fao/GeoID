import base64
import json
from uuid import UUID
import datetime as datetime

import dynastore.modules.spanner.constants as _c
import dynastore.modules.spanner.models.business.principal as _pbm
import dynastore.modules.spanner.models.business.asset as _abm

def convert_json_object(json_obj):
    value = json.loads(json_obj.serialize())
    try:
        dict_value = json.loads(value)
        return dict_value
    except:
        return value

def convert_into_null_if_none(var):
    if var == None:
        return _c.SPANNER_NULL_STR


def asset_from_spanner_dict(spanner_dict):
    return _abm.Asset(
        uuid=convert_from_spanner_uuid(spanner_dict["asset_uuid"]),
        class_code=_c.AssetClassCodes(spanner_dict["asset_class_id"]),
        type_code=_c.AssetTypeCodes(spanner_dict["asset_type_id"]),
        state_code=_c.StateCodes(spanner_dict["state"])
    )

def asset_from_spanner_row(row):
    spanner_dict = {
        "asset_uuid": row[0],
        "asset_class_id": row[1],
        "asset_type_id": row[2],
        "state": row[3],
    }
    return asset_from_spanner_dict(spanner_dict)

def asset_to_spanner_dict(asset):
    return {
        # UPDATED: uuid -> asset_uuid
        "asset_uuid": convert_into_spanner_uuid(asset.get_uuid()),
        # getattr(_c.AssetClassCodes, asset.get_class_code().upper()).value,
        "asset_class_id": asset.get_class_code().value,
        # getattr(_c.AssetTypeCodes, asset.get_type_code().upper()).value,
        "asset_type_id": asset.get_type_code().value,
        # getattr(_c.StateCodes, asset.get_state_code().upper()).value,
        "state": asset.get_state_code().value,
        "change_timestamp": get_current_timestamp_with_safety()
    }

def principal_from_spanner_dict(spanner_dict):
    """Converts a Spanner dict principal object."""
    return _pbm.Principal(
        # UPDATED: uuid -> principal_uuid
        uuid=convert_from_spanner_uuid(spanner_dict.get('principal_uuid')),
        type_code=_c.PrincipalTypes(spanner_dict.get('type_id')) if spanner_dict.get('type_id') is not None else None,
        principal_id=spanner_dict.get('principal_id'),
        display_name=spanner_dict.get('display_name'),
        description=spanner_dict.get('description'),
        email=spanner_dict.get('email'),
        state_code=_c.StateCodes(spanner_dict.get('state')),
        creation_time=spanner_dict.get('creation_time'),
        metadata=json.loads(spanner_dict.get('metadata')) if spanner_dict.get('metadata') else None,
    )

def principal_to_spanner_dict(principal: _pbm.Principal):
    return {
        # UPDATED: uuid -> principal_uuid
        "principal_uuid": convert_into_spanner_uuid(principal.get_uuid()),

        # SECURITY FIX: Inline check. If get_type_code() is None, return None immediately.
        # This prevents the "AttributeError: 'NoneType' has no attribute 'value'" crash.
        "type_id": principal.get_type_code().value,
        
        "principal_id": principal.get_principal_id(),
        "display_name": principal.get_display_name(),
        "description": principal.get_description(),
        "email": principal.get_email(),
        "metadata": json.dumps(principal.get_metadata()) if principal.get_metadata() else None,
        # State is safe because your class getter handles the None case internally.
        "state": principal.get_state_code().value,
        
        "creation_time": principal.get_creation_time() or get_current_timestamp_with_safety(),
        "update_time": get_current_timestamp_with_safety()
    }

def asset_relation_to_spanner_dict(relation_code: str, asset_relation: _abm.AssetRelation):
    return {
        "source_asset_uuid": convert_into_spanner_uuid(asset_relation.get_source().get_uuid()),
        "asset_relation_type_id": relation_code.value,
        "target_asset_uuid": convert_into_spanner_uuid(asset_relation.get_target().get_uuid()),
        "state": asset_relation.get_state_code().value
    }

def asset_relation_attribute_from_spanner_row(row):
    return asset_relation_attribute_from_spanner_dict({
        "attribute_code": row[2],
        "attribute": row[3]
    })

def asset_relation_attribute_from_spanner_dict(spanner_dict):
    return _abm.Attribute(
        code=spanner_dict.get('attribute_code'),
        attribute=spanner_dict.get('attribute')
    )

def asset_relation_attribute_to_spanner_dict(relation_id: int, attribute: _abm.Attribute):
    return {
        "asset_edge_id": relation_id,
        "attribute_code": attribute.get_code(),
        "attribute": attribute.get_attribute()
    }

def build_relation_from_spanner_dict(source_asset: _abm.Asset, target_asset: _abm.Asset, spanner_dict: dict) -> _abm.AssetRelation:
    """Converts a Spanner dict relation object."""
    return _abm.AssetRelation(
        source=source_asset,
        target=target_asset,
        code=_c.AssetRelationCodes(spanner_dict.get('asset_relation_type_id')),
        state_code=_c.StateCodes(spanner_dict.get('state')),
        attributes=build_attributes_from_attributes_array(spanner_dict.get('attributes', []))
    )

def asset_workflow_config_to_spanner_dict(asset:_abm.AssetConfig):
    return {
        "asset_workflow_type_id": asset.get_workflow().get_id(),
        "asset_uuid": convert_into_spanner_uuid(asset.get_uuid()),
        # If attached to asset config it is active, maybe could be refined later
        "state": _c.StateCodes.ACTIVE.value
    }

def asset_workflow_type_from_spanner_dict(workflow_type_spanner_dict: dict, next_states_spanner_list: list = []) -> _abm.AssetWorkflowType:
    """Converts a Spanner dict workflow type object."""
    return _abm.AssetWorkflowType(
        id=workflow_type_spanner_dict.get('id'),
        name=workflow_type_spanner_dict.get('name'),
        description=workflow_type_spanner_dict.get('description'),
        principal=_pbm.Principal(uuid=convert_from_spanner_uuid(workflow_type_spanner_dict.get('principal_uuid'))),
        creation_timestamp=workflow_type_spanner_dict.get('creation_timestamp'),
        state_code=_c.StateCodes(workflow_type_spanner_dict.get('state')),
        next_state_edges=[build_workflow_edge_from_spanner_dict(edge) for edge in next_states_spanner_list]
    )

def build_workflow_edge_from_spanner_dict(spanner_dict: dict) -> _abm.Relation:
    return _abm.Relation(
        source_state_type_id=spanner_dict.get('source_asset_workflow_state_type_id'),
        target_state_type_id=spanner_dict.get('target_asset_workflow_state_type_id'),
        attribute_code=spanner_dict.get('attribute_code')
    )

def convert_from_spanner_uuid(spanner_uuid):
    if not spanner_uuid:
        return None
    # Decode base64 bytes to raw bytes
    uuid_bytes = base64.b64decode(spanner_uuid)
    # Create UUID from bytes
    return UUID(bytes=uuid_bytes)

def convert_into_spanner_uuid(uuid: UUID):
    if not uuid:
        return None
    return base64.b64encode(uuid.bytes)

def get_current_timestamp_with_safety():
    #return "PENDING_COMMIT_TIMESTAMP"
    
    #from google.cloud import spanner
    #return spanner.COMMIT_TIMESTAMP

    return datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=3)  # 3s safety margin


def asset_type_from_spanner_row(spanner_row: list):
    return {
        "type_code":spanner_row[1],
        "description":spanner_row[2]
    }

def asset_class_from_spanner_row(spanner_row: list):
    return {
        "class_code":spanner_row[1],
        "description":spanner_row[2]
    }

# TODO should be fixed, mark: attribute_list_fix
def create_tree_attribute():
    return {
        _c.TypeAttributeCodes.get_enum_code(): [get_tree_attribute()]
    }

# TODO should be fixed, mark: attribute_list_fix
def get_tree_attribute():
    return _abm.Attribute(
        code=_c.TypeAttributeCodes.get_enum_code(),
        attribute=_c.TypeAttributeCodes.TREE.value.lower()
    )

def state_from_spanner_dict(spanner_dict: dict) -> _abm.State:
    """Converts a Spanner dict status object."""
    return _abm.State(
        id=spanner_dict.get('id'),
        name=spanner_dict.get('name')
    )

def build_attributes_from_attributes_array(attributes_array):
    attributes = {}
    for attribute_row in attributes_array:
        attribute_id, attribute_code, attribute = attribute_row
        if attribute_code not in attributes:
            attributes[attribute_code] = []
        attributes[attribute_code].append(
            _abm.Attribute(
                code=attribute_code,
                attribute=attribute
            )
        )
    return attributes

# TODO MAKE THE SYNC FUNCTIONS ASYNC IF UNNECESSARY

#########PROPERTY#########
def asset_property_to_spanner_dict(asset, property_code, property):
    return {
        "asset_uuid": convert_into_spanner_uuid(asset.get_uuid()),
        "name": property_code,
        "body": json.dumps(property.get_body()),
        # TODO decide a default type
        "type_code": property.get_type().value,
        "state": property.get_state().value
    }

def asset_property_from_spanner_row(row):
    spanner_dict = {
        "id": row[1],
        "name": row[2],
        "body": row[6],
        "type_code": row[7],
        "state": row[8],
    }
    return asset_property_from_spanner_dict(spanner_dict)

def asset_property_from_spanner_dict(spanner_dict):
    return _abm.Property(
        id=spanner_dict.get('id'),
        name=spanner_dict.get('name'),
        body=convert_json_object(spanner_dict.get('body')),
        type=getattr(_c.PropertyDataTypes, spanner_dict.get('type_code').upper()),
        state=_c.StateCodes(spanner_dict.get('state'))
    )

#########VOCABULARY#########
def term_to_spanner_dict(asset: _abm.Asset, term_code:str, term: _abm.Term):
    return {
        "asset_uuid": convert_into_spanner_uuid(asset.get_uuid()),
        "term_code": term_code,
        "state": term.get_state_code().value
    }

def term_translation_to_spanner_dict(asset_uuid: UUID, term_id: int, term_translation: _abm.TermTranslation):
    return {
        "asset_uuid": convert_into_spanner_uuid(asset_uuid), # required because of interleaving
        "term_id": term_id,
        "language_id": term_translation.get_language_code().get_id(),
        "value": term_translation.get_value()
    }

#########WORKFLOW#########
def get_workflow_initial_state_attribute():
    return {
            _c.WORKFLOW_EDGE_ATTRIBUTE_INITIAL_CODE: [_abm.Attribute(
                code=_c.WORKFLOW_EDGE_ATTRIBUTE_INITIAL_CODE,
                attribute="")]
            }

def asset_workflow_type_to_spanner_dict(workflow: _abm.AssetWorkflowType) -> dict:
    asset_workflow_type_spanner_dict =  {
        "id": workflow.get_id() or None,
        "name": workflow.get_name(),
        "description": workflow.get_description(),
        "principal_uuid": convert_into_spanner_uuid(workflow.get_principal().get_uuid()),
        "creation_timestamp": workflow.get_creation_timestamp(),
        # TODO add back once it is added to db
        "state": workflow.get_state_code().value
    }
    if asset_workflow_type_spanner_dict['id'] == None:
        del asset_workflow_type_spanner_dict['id']
    return asset_workflow_type_spanner_dict

def asset_workflow_state_from_spanner_row(row):
    spanner_dict = {
        "asset_uuid": row[0],
        "id": row[1],
        "current_asset_workflow_state_type_id": row[3],
        "principal_uuid": row[4],
        "reason": row[5],
        "change_timestamp": row[6],
    }
    return asset_workflow_state_from_spanner_dict(spanner_dict)

def asset_workflow_state_from_spanner_dict(spanner_dict):
    workflow_state = _c.AssetWorkflowStateCodes(
        spanner_dict.get('current_asset_workflow_state_type_id')
    )

    return _abm.AssetState(
        current_state=workflow_state,
        reason=spanner_dict.get('reason'),
        #workflow_type_id=spanner_dict.get('asset_workflow_type_id')
        # TODO add principal info
        principal=_pbm.Principal(
            uuid=convert_from_spanner_uuid(spanner_dict.get('principal_uuid'))
        ),
        id=spanner_dict.get('id'),
        change_timestamp=spanner_dict.get('change_timestamp')
    )

def asset_workflow_state_to_spanner_dict(asset_workflow_state, asset_uuid, asset_workflow_type_id):
    asset_workflow_state_spanner_dict = {
        # Optional, since Spanner can auto-generate
        "id": getattr(asset_workflow_state, "id", None),
        "asset_uuid": convert_into_spanner_uuid(asset_uuid),
        "asset_workflow_type_id": asset_workflow_type_id,
        "current_asset_workflow_state_type_id": asset_workflow_state.get_current_state().value,
        "principal_uuid": convert_into_spanner_uuid(asset_workflow_state.get_principal().get_uuid()),
        "reason": asset_workflow_state.get_reason(),
        "change_timestamp": asset_workflow_state.get_change_timestamp() or get_current_timestamp_with_safety()
    }

    if asset_workflow_state_spanner_dict['id'] == None:
        del asset_workflow_state_spanner_dict['id']
    return asset_workflow_state_spanner_dict


def asset_workflow_edge_to_spanner_dict(workflow_type_id: int, edge: _abm.Relation) -> dict:

    return {
        "source_asset_workflow_state_type_id": edge.get_source().value,
        "target_asset_workflow_state_type_id": edge.get_target().value,
        "asset_workflow_type_id": workflow_type_id,
        "state": edge.get_state_code().value
    }

def asset_workflow_edge_attribute_to_spanner_dict(edge_id: int, attribute: _abm.Attribute) -> dict:

    return {
        "attribute_code": attribute.get_code(),
        "attribute": attribute.get_attribute(),
        "asset_workflow_edge_id": edge_id,
    }

############ SPANNER STATIC TYPE DEFINITIONS ############
from google.cloud.spanner_v1 import TypeCode
from google.cloud.spanner_v1.types import Type
ASSET_PROPERTY_TYPES = {
    "id": Type(code=TypeCode.INT64),
    "asset_uuid": Type(code=TypeCode.BYTES),
    "name":  Type(code=TypeCode.STRING),
    "body":  Type(code=TypeCode.JSON),
    "type_code":  Type(code=TypeCode.STRING)
}

EDGE_TYPES = {
    "source_asset_uuid": Type(code=TypeCode.BYTES),
    "asset_relation_type_id": Type(code=TypeCode.INT64),
    "target_asset_uuid":  Type(code=TypeCode.BYTES)
}

EDGE_ATTRIBUTE_TYPES = {
    "id": Type(code=TypeCode.INT64),
    "asset_edge_id": Type(code=TypeCode.INT64),
    "attribute_code": Type(code=TypeCode.STRING),
    "attribute":  Type(code=TypeCode.STRING)
}

PRINCIPAL_PARAM_TYPES = {
    "email": Type(code=TypeCode.STRING),
    "creation_time": Type(code=TypeCode.TIMESTAMP),
    "update_time": Type(code=TypeCode.TIMESTAMP),
    "display_name": Type(code=TypeCode.STRING),
    "description": Type(code=TypeCode.STRING),
    "principal_id": Type(code=TypeCode.STRING),
    "metadata": Type(code=TypeCode.JSON),
}