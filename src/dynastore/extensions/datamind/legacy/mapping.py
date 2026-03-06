import json
import typing
from uuid import UUID, uuid4
from dynastore.extensions.datamind import datamind
from dynastore.extensions.datamind.legacy import constants as _lc
from dynastore.modules.spanner.models.business import asset as _abm
from dynastore.modules.spanner.models.business import principal as _pbm

from dynastore.modules.spanner import constants as _c
from dynastore.modules.spanner import utils as _su

# import internal_module as _abm  # Assuming this exists in your environment


def _set_if_present(target_dict: dict, key: str, value: typing.Any, data_type: _c.PropertyDataTypes):
    """
    Helper to apply the General Rule: 
    If value is None or Empty String, do not add to property.
    """
    if value is None:
        return
    if isinstance(value, str) and value.strip() == "":
        return
    target_dict[key] = _abm.Property(body=value, name=key, type=data_type)

def map_organization(organization_data: dict, config_uuid:UUID) -> _abm.Asset:
    """
    Maps legacy CKAN Organization data into Datamind.
    """
    # 1. State Check: Do not fetch if not active
    if organization_data.get("state") != _lc.CKAN_ACTIVE_CODE:
        return None

    properties = {}

    # --- Property Mapping ---
    # Based on organization_result structure provided
    # 'id' is excluded from properties as it is usually the asset UUID or used for linking logic externally
    
    direct_mappings = [
        ("image_display_url", "image_display_url", _c.PropertyDataTypes.STRING),
        ("created", "created", _c.PropertyDataTypes.STRING),
        ("image_url", "image_url", _c.PropertyDataTypes.STRING),
        ("approval_status", "approval_status", _c.PropertyDataTypes.STRING),
        ("package_count", "package_count", _c.PropertyDataTypes.NUMBER),
        ("num_followers", "num_followers", _c.PropertyDataTypes.NUMBER),
        ("revision_id", "revision_id", _c.PropertyDataTypes.STRING),
        ("is_organization", "is_organization", _c.PropertyDataTypes.BOOLEAN),
        ("extras", "extras", _c.PropertyDataTypes.ARRAY)
    ]

    for src_key, dest_key, data_type in direct_mappings:
        _set_if_present(properties, dest_key, organization_data.get(src_key), data_type)

    # Construct the Asset
    # Note: Assuming _abm.Asset takes properties and terms. 
    # Adjust arguments based on actual constructor.
    asset = _abm.Asset(
        properties=properties,
        uuid=organization_data.get("id"),
        type_code=_c.AssetTypeCodes.TEAM # Assuming type needs to be passed
    )

    asset.add_relation_to_another_asset(relation_code=_c.AssetRelationCodes.HAS_CONFIG, target_asset=_abm.Asset(uuid=config_uuid))
    asset.add_term_translation(
        term_code="title", 
        value=organization_data.get("title"), 
        language_code=_lc.default_lang_code
    )

    asset.add_term_translation(
        term_code="name", 
        value=organization_data.get("name"), 
        language_code=_lc.default_lang_code
    )

    asset.add_term_translation(
        term_code="description", 
        value=organization_data.get("description"), 
        language_code=_lc.default_lang_code
    )

    return asset

def map_dataset(package_body:dict, config_uuid: UUID, organization_registry_asset: _abm.Asset) -> _abm.Asset:
    """
    Maps legacy CKAN Package (Dataset) data to the new Asset structure.
    """
    # 1. State Check
    if package_body.get("state") != _lc.CKAN_ACTIVE_CODE:
        return None

    properties = {}
    # --- Property Mapping ---
    
    # Mappings defined in your notes:
    mapping_config = [
        ("private", "private", _c.PropertyDataTypes.BOOLEAN),
        ("maintainer_email", "maintainer_email", _c.PropertyDataTypes.STRING),
        ("num_tags", "num_tags", _c.PropertyDataTypes.NUMBER),
        ("author", "author", _c.PropertyDataTypes.STRING),
        ("author_email", "author_email", _c.PropertyDataTypes.STRING),
        ("version", "version", _c.PropertyDataTypes.STRING),
        ("num_resources", "num_resources", _c.PropertyDataTypes.NUMBER),
        ("tags", "tags", _c.PropertyDataTypes.ARRAY),
        ("groups", "groups", _c.PropertyDataTypes.ARRAY),
        ("extras", "extras", _c.PropertyDataTypes.ARRAY),
        ("isopen", "isopen", _c.PropertyDataTypes.BOOLEAN),
        ("url", "source_url", _c.PropertyDataTypes.STRING), # Renamed
        ("revision_id", "revision_id", _c.PropertyDataTypes.STRING),
        ("metadata_created", "metadata_created", _c.PropertyDataTypes.STRING),
        ("metadata_modified", "metadata_modified", _c.PropertyDataTypes.STRING),
        # IDs for linking (stored in props to be accessed by ETL logic)
        ("creator_user_id", "creator_user_id", _c.PropertyDataTypes.STRING), # TODO later it will be used to link to user asset
        (_lc.JSONSCHEMA_TYPE_CODE, _lc.JSONSCHEMA_TYPE_CODE, _c.PropertyDataTypes.STRING),
        (_lc.JSONSCHEMA_OPT_CODE, _lc.JSONSCHEMA_OPT_CODE, _c.PropertyDataTypes.OBJECT),
    ]

    for src_key, dest_key, data_type in mapping_config:
        _set_if_present(properties, dest_key, package_body.get(src_key), data_type)
    

    asset = _abm.Asset(
        properties=properties,
        uuid=package_body.get("id")
    )
    json_body = package_body.get(_lc.JSONSCHEMA_BODY_CODE)
    if json_body:
        asset.add_data(json_body, _c.PropertyDataTypes.OBJECT)

    #legacy_organization_uuid = package_body.get("organization").get("id")    
    #asset.add_relation_from_another_asset(relation_code=_c.AssetRelationCodes.HAS_METADATA, source_asset=_abm.Asset(uuid=legacy_organization_uuid), attributes=_su.create_tree_attribute())
    
    
    # MOVE THESE 2 LINES OUTSIDE AND GET RID OF THE DEPENDENCIES
    asset.add_relation_from_another_asset(relation_code=_c.AssetRelationCodes.HAS_METADATA, source_asset=organization_registry_asset, attributes=_su.create_tree_attribute())
    asset.add_relation_to_another_asset(relation_code=_c.AssetRelationCodes.HAS_CONFIG, target_asset=_abm.Asset(uuid=config_uuid))

    # --- Terms Mapping ---
    
    # 'title' -> Term Title
    asset.add_term_translation(
        term_code="title", 
        value=package_body.get("title"), 
        language_code=_lc.default_lang_code
    )

    # 'name' -> Term Name
    asset.add_term_translation(
        term_code="name", 
        value=package_body.get("name"), 
        language_code=_lc.default_lang_code
    )

    # 'notes' -> Term Description
    asset.add_term_translation(
        term_code="description", 
        value=package_body.get("notes"), 
        language_code=_lc.default_lang_code
    )
    return asset

def map_resource(resource_data: dict, config_uuid: UUID) -> _abm.Asset:
    """
    Maps legacy CKAN Resource to the new Asset structure.
    """
    # 1. State Check
    if resource_data.get("state") != _lc.CKAN_ACTIVE_CODE:
        # TODO, if we need to map inactive resources, we need to handle this
        return None

    properties = {}

    # --- Property Mapping ---
    
    mapping_config = [
        ("mimetype", "mimetype", _c.PropertyDataTypes.STRING),
        ("hash", "hash", _c.PropertyDataTypes.STRING),
        ("format", "format", _c.PropertyDataTypes.STRING),
        ("url", "url", _c.PropertyDataTypes.STRING),
        ("created", "first_creation", _c.PropertyDataTypes.STRING), # Renamed
        ("mimetype_inner", "mimetype_inner", _c.PropertyDataTypes.STRING),
        ("last_modified", "last_update", _c.PropertyDataTypes.STRING), # Renamed
        ("position", "position", _c.PropertyDataTypes.NUMBER),
        ("revision_id", "revision_id", _c.PropertyDataTypes.STRING),
        ("url_type", "url_type", _c.PropertyDataTypes.STRING),
        ("size", "size", _c.PropertyDataTypes.NUMBER)
    ]

    for src_key, dest_key, data_type in mapping_config:
        _set_if_present(properties, dest_key, resource_data.get(src_key), data_type)

    # --- Data Property (JSONSchema Body) ---
    # "All the yes'es will be seperate properties and jsonschema_body will be the data property"
    # This implies the raw resource dictionary is stored in 'data'.

    asset = _abm.Asset(
        properties=properties,
        uuid=resource_data.get("id"),
        type_code=_c.AssetTypeCodes.RESOURCE # Or specific resource type based on input
    )

    json_body = resource_data.get(_lc.JSONSCHEMA_BODY_CODE)
    if json_body:
        asset.add_data(json_body, _c.PropertyDataTypes.OBJECT)

    metadata_uuid = resource_data.get("package_id")
    asset.add_relation_from_another_asset(relation_code=_c.AssetRelationCodes.HAS_RESOURCE, source_asset=_abm.Asset(uuid=metadata_uuid), attributes=_su.create_tree_attribute())
    asset.add_relation_to_another_asset(relation_code=_c.AssetRelationCodes.HAS_CONFIG, target_asset=_abm.Asset(uuid=config_uuid))


    # 'name' -> Term Name
    asset.add_term_translation(
        term_code="name", 
        value=resource_data.get("name"), 
        language_code=_lc.default_lang_code
    )

    # 'description' -> Term Description
    asset.add_term_translation(
        term_code="description", 
        value=resource_data.get("description"), 
        language_code=_lc.default_lang_code
    )

    return asset

def reverse_map_organization(asset) -> dict:
    if not asset: return {}
    
    org_dict = {
        "id": str(asset.get_uuid()),
        "title": asset.get_term_translation("title", _lc.default_lang_code),
        "name": asset.get_term_translation("name", _lc.default_lang_code),
        "description": asset.get_term_translation("description", _lc.default_lang_code),
        "state": _lc.CKAN_ACTIVE_CODE
    }

    # Direct Property Mappings
    direct_mappings = [
        ("image_display_url", "image_display_url"),
        ("created", "created"),
        ("image_url", "image_url"),
        ("approval_status", "approval_status"),
        ("package_count", "package_count"),
        ("num_followers", "num_followers"),
        ("revision_id", "revision_id"),
        ("is_organization", "is_organization"),
        ("extras", "extras")
    ]
    
    for ckan_key, prop_key in direct_mappings:
        prop = asset.get_property(prop_key)
        if prop and prop.body is not None:
            org_dict[ckan_key] = prop.body
            
    return org_dict

def reverse_map_resource(asset: _abm.Asset, package_id: UUID) -> dict:
    res_dict = {
        "id": str(asset.uuid),
        "name": asset.get_term_translation(_c.NAME_TERM_CODE, _lc.default_lang_code),
        "description": asset.get_term_translation(_c.DESCRIPTION_TERM_CODE, _lc.default_lang_code),
        "state": _lc.CKAN_ACTIVE_CODE
    }

    direct_mappings = [
        ("mimetype", "mimetype", None),
        ("hash", "hash", ""),
        ("format", "format", None),
        ("url", "url", ""),
        ("created", "first_creation", None),
        ("mimetype_inner", "mimetype_inner",""),
        ("last_modified", "last_update", None),
        ("position", "position", None),
        ("revision_id", "revision_id", None),
        ("url_type", "url_type", None),
        ("size", "size", None)
    ]

    for ckan_key, prop_key, default_value in direct_mappings:
        prop = asset.get_property(prop_key)
        if prop and prop.body is not None:
            res_dict[ckan_key] = prop.body
        else:
            res_dict[ckan_key] = default_value


    # Attach package id
    res_dict["package_id"] = package_id

    # Handle JSON Body
    data_content = asset.get_property(_c.DATA_PROPERTY_CODE)
    if data_content and data_content.body is not None:
        res_dict[_lc.JSONSCHEMA_BODY_CODE] = data_content.body

    return res_dict

def reverse_map_dataset(asset) -> dict:
    pkg_dict = {
        "id": str(asset.get_uuid()),
        "title": asset.get_term_translation("title", _lc.default_lang_code),
        "name": asset.get_term_translation("name", _lc.default_lang_code),
        "notes": asset.get_term_translation("description", _lc.default_lang_code),
        # TODO for actives only, if we fetch non actives we need to handle this
        "state": _lc.CKAN_ACTIVE_CODE
    }

    direct_mappings = [
        ("private", "private", True),
        ("maintainer_email", "maintainer_email", ""),
        ("num_tags", "num_tags", 0),
        ("author", "author", ""),
        ("author_email", "author_email", ""),
        ("version", "version", ""),
        ("tags", "tags", None),
        ("groups", "groups", None),
        ("extras", "extras", None),
        ("isopen", "isopen", False),
        ("url", "source_url", ""),
        ("revision_id", "revision_id", ""),
        ("metadata_created", "metadata_created", None),
        ("metadata_modified", "metadata_modified",None),
        ("creator_user_id", "creator_user_id", None),
        (_lc.JSONSCHEMA_TYPE_CODE, _lc.JSONSCHEMA_TYPE_CODE, None),
        (_lc.JSONSCHEMA_OPT_CODE, _lc.JSONSCHEMA_OPT_CODE, None),
    ]

    for ckan_key, prop_key, default_value in direct_mappings:
        prop = asset.get_property(prop_key)
        if prop and prop.body is not None:
            pkg_dict[ckan_key] = prop.body
        else:
            pkg_dict[ckan_key] = default_value

    # Handle JSON Body
    data_content = asset.get_property(_c.DATA_PROPERTY_CODE)
    if data_content and data_content.body is not None:
        pkg_dict[_lc.JSONSCHEMA_BODY_CODE] = data_content.body

    return pkg_dict


def map_user(user_data: dict) -> _pbm.Principal:
    # TODO Temporary security check
    if user_data.get("state") != _lc.CKAN_ACTIVE_CODE:
        raise ValueError("Only active users can be mapped")

    METADATA_FIELDS_TO_KEEP = [
        'email_hash',
        'name', 
        'created', 
        'sysadmin',  
        'activity_streams_email_notifications',
        'number_of_edits',
        'fullname',
        'number_created_packages'
    ]
    # Prepare Metadata (Whitelist Approach)
    # Only grab the specific fields defined above
    meta_payload = {
        key: user_data.get(key) 
        for key in METADATA_FIELDS_TO_KEEP 
        if key in user_data
    }

    #  Construct the Principal Object
    principal = _pbm.Principal(
        uuid=UUID(user_data.get("id")),
        principal_id=f"{_lc.CKAN_USER_ID_PREFIX}:{user_data.get('id')}", 
        type_code=_c.PrincipalTypes.USER, 
        # Mapped columns (excluded from metadata to avoid redundancy)
        display_name=user_data.get('display_name'),
        description=user_data.get('about'),
        email=user_data.get('email'),
        state_code=_c.StateCodes.ACTIVE,
        metadata=json.dumps(meta_payload) 
    )
    
    return principal

def reverse_map_user(principal: _pbm.Principal) -> dict:
    metadata = principal.get_metadata()

    user_dict = {
        "id": str(principal.get_uuid()),
        "display_name": principal.get_display_name(),
        "about": principal.get_description(),
        "email": principal.get_email(),
        "state": _lc.CKAN_ACTIVE_CODE if principal.get_state_code() == _c.StateCodes.ACTIVE else _lc.CKAN_DELETED_CODE,
        **(metadata if metadata else {})
    }
    return user_dict

def map_license(license_body: dict, license_config_uuid: UUID) -> _abm.Asset:
    properties = {}

    # --- Property Mapping ---
    mapping_config = [
        ("id",_c.LICENSE_ID_PROPERTY_CODE, _c.PropertyDataTypes.STRING),
        ("description", "description", _c.PropertyDataTypes.STRING),
        ("domain_content", "domain_content", _c.PropertyDataTypes.BOOLEAN),
        ("domain_data", "domain_data", _c.PropertyDataTypes.BOOLEAN),
        ("domain_software", "domain_software", _c.PropertyDataTypes.BOOLEAN),
        ("family", "family", _c.PropertyDataTypes.STRING),
        ("od_conformance", "od_conformance", _c.PropertyDataTypes.STRING),
        ("osd_conformance", "osd_conformance", _c.PropertyDataTypes.STRING),
        ("is_generic", "is_generic", _c.PropertyDataTypes.BOOLEAN)
    ]

    for src_key, dest_key, data_type in mapping_config:
        _set_if_present(properties, dest_key, license_body.get(src_key), data_type)

    asset = _abm.Asset(
        properties=properties,
        uuid=uuid4(),
        type_code=_c.AssetTypeCodes.LICENSE,
        class_code=_c.AssetClassCodes.DOCUMENT,
        state_code=_c.StateCodes.ACTIVE
    )

    asset.add_term_translation(
        term_code="title", 
        value=license_body.get("title"), 
        language_code=_lc.default_lang_code
    )
    
    asset.add_term_translation(
        term_code="url",
        value=license_body.get("url"),
        language_code=_lc.default_lang_code
    )

    asset.add_relation_to_another_asset(
        relation_code=_c.AssetRelationCodes.HAS_CONFIG,
        target_asset=_abm.Asset(uuid=license_config_uuid)
    )
    
    # Add Data Property (Full License Body)
    #asset.add_data(license_body, _c.PropertyDataTypes.OBJECT)

    return asset

def reverse_map_license(license_asset: _abm.Asset) -> dict:
    license_dict = {
        "title": license_asset.get_term_translation("title", _lc.default_lang_code),
        "url": license_asset.get_term_translation("url", _lc.default_lang_code),
    }

    mapping_config = [
        (_c.LICENSE_ID_PROPERTY_CODE, "id"),
        ("description", "description"),
        ("domain_content", "domain_content"),
        ("domain_data", "domain_data"),
        ("domain_software", "domain_software"),
        ("family", "family"),
        ("od_conformance", "od_conformance"),
        ("osd_conformance", "osd_conformance"),
        ("is_generic", "is_generic")
    ]

    for src_key, prop_key in mapping_config:
        prop = license_asset.get_property(prop_key)
        if prop and prop.body is not None:
            license_dict[src_key] = prop.body

    return license_dict

def inject_license_fields(license_asset:_abm.Asset, package_dict:dict):
    package_dict["license_id"] = license_asset.get_property(_c.LICENSE_ID_PROPERTY_CODE).get_body()
    package_dict["license_title"] = license_asset.get_term_translation("title", _lc.default_lang_code)
    package_dict["license_url"] = license_asset.get_term_translation("url", _lc.default_lang_code)