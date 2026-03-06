from dynastore.modules.spanner import datamind
from dynastore.modules.spanner import constants as _c
from dynastore.modules.spanner.models.business import asset as _abm
from dynastore.modules.spanner import utils as _u 
import logging
logger = logging.getLogger(__name__)

#######PARSERS#######
async def parse_registry_config_old(config_data: dict, registry_assets=[], workflow_code_map=None):
    # Create AssetConfig for each config JSON entry
    class_code = _c.AssetClassCodes.CONFIG
    if 'class' in config_data:
        class_code = getattr(_c.AssetClassCodes,
                                config_data['class'].upper())
    if class_code == _c.AssetClassCodes.CONFIG:
        asset = _abm.AssetConfig()
    else:
        asset = _abm.Asset()

    asset_type = getattr(_c.AssetTypeCodes, config_data['type'].upper())
    asset.set_type_code(asset_type)

    asset.set_class_code(class_code)

    # Add properties from `property` key
    if "property" in config_data:
        for prop_key, prop_data in config_data['property'].items():
            property = _abm.Property(body=prop_data['value'], type=getattr(
                _c.PropertyDataTypes, prop_data['type'].upper()))
            asset.add_property(property_name=prop_key, property=property)

    if "terms" in config_data:
        for term_code, translations_data in config_data['terms'].items():
            translations = {}
            for language_key, translation in translations_data.items():
                language_code = getattr(
                    _c.LanguageCodes, language_key.upper())
                translations.update({language_code: _abm.TermTranslation(
                    language_code=language_code, value=translation)})
            asset.add_term(term_code=term_code, term=_abm.Term(
                translations=translations))

    if "relations" in config_data:
        for relation in config_data["relations"]:
            for relation_code, related_assets in relation.items():
                for related_asset_data in related_assets:
                    parsed_assets = []
                    await parse_registry_config(config_data=related_asset_data, registry_assets=parsed_assets, workflow_code_map=workflow_code_map)
                    for parsed_asset in parsed_assets:
                        asset.add_relation_to_another_asset(relation_code=getattr(
                            _c.AssetRelationCodes, relation_code.upper()), target_asset=parsed_asset, attributes=_u.create_tree_attribute())
                    registry_assets.extend(parsed_assets)

    if "schema" in config_data:
        schema_asset = _abm.Asset(
            type_code=asset_type, class_code=_c.AssetClassCodes.SCHEMA)
        asset.add_relation_to_another_asset(
            relation_code=_c.AssetRelationCodes.HAS_SCHEMA, target_asset=schema_asset)
        registry_assets.append(schema_asset)

    if "template" in config_data:
        for template_data in config_data['template']:
            template_asset = _abm.Asset(
                type_code=asset_type, class_code=_c.AssetClassCodes.TEMPLATE)
            asset.add_relation_to_another_asset(relation_code=getattr(
                _c.AssetRelationCodes, template_data['relation'].upper()), target_asset=template_asset)
            registry_assets.append(template_asset)

    if "workflow_code" in config_data:
        workflow = workflow_code_map.get(config_data['workflow_code'])
        asset.set_workflow(workflow)
    registry_assets.append(asset)
    return registry_assets

async def parse_registry_config(manager, config_data: dict, registry_assets=[], workflow_code_map=None, reference_map=None):
    # Ensure reference_map exists
    current_reference_map = reference_map.copy() if reference_map else {}

    # -------------------------------------------------------------------------
    # STEP 1: Process 'configurations' (Flat List Pre-creation)
    # -------------------------------------------------------------------------
    if "configurations" in config_data:
        for config_key, config_item_data in config_data["configurations"].items():
            # We simply parse this item as a single config unit (no deep recursion into sub-configs needed)
            temp_assets = []
            
            # Use the same function but we know these items won't have their own 'configurations' block
            await parse_registry_config(
                manager, 
                config_data=config_item_data, 
                registry_assets=temp_assets, 
                workflow_code_map=workflow_code_map, 
                reference_map=current_reference_map # Pass map so they can ref higher levels if needed
            )
            
            registry_assets.extend(temp_assets)
            
            # Register in map
            if temp_assets:
                # Find the main asset (ignoring schema/template sidecars)
                main_asset = next((a for a in reversed(temp_assets) 
                                   if a.get_class_code() not in [_c.AssetClassCodes.SCHEMA, _c.AssetClassCodes.TEMPLATE]), 
                                   temp_assets[-1])
                
                # Determine key: explicit 'profile' property OR the dict key
                key_to_store = config_key
                if "property" in config_item_data and "profile" in config_item_data["property"]:
                     key_to_store = config_item_data["property"]["profile"]["value"]
                
                current_reference_map[key_to_store] = main_asset

    # -------------------------------------------------------------------------
    # STEP 2: Create Current Asset
    # -------------------------------------------------------------------------
    class_code = _c.AssetClassCodes.CONFIG
    if 'class' in config_data:
        class_code = getattr(_c.AssetClassCodes, config_data['class'].upper())
    
    if class_code == _c.AssetClassCodes.CONFIG:
        asset = _abm.AssetConfig()
    else:
        asset = _abm.Asset()

    if 'type' in config_data:
        asset.set_type_code(getattr(_c.AssetTypeCodes, config_data['type'].upper()))
    
    asset.set_class_code(class_code)

    # Properties
    if "property" in config_data:
        for prop_key, prop_data in config_data['property'].items():
            property_obj = _abm.Property(body=prop_data['value'], type=getattr(
                _c.PropertyDataTypes, prop_data['type'].upper()))
            asset.add_property(property_name=prop_key, property=property_obj)

    # Terms
    if "terms" in config_data:
        for term_code, translations_data in config_data['terms'].items():
            translations = {}
            for language_key, translation in translations_data.items():
                language_code = getattr(_c.LanguageCodes, language_key.upper())
                translations.update({language_code: _abm.TermTranslation(
                    language_code=language_code, value=translation)})
            asset.add_term(term_code=term_code, term=_abm.Term(translations=translations))

    # -------------------------------------------------------------------------
    # STEP 3: Process Standard Relations (Recursion)
    # -------------------------------------------------------------------------
    if "relations" in config_data:
        for relation_group in config_data["relations"]:
            for relation_code, related_items_list in relation_group.items():
                for related_item_data in related_items_list:
                    parsed_children = []
                    
                    # Pass the populated map down
                    child_asset = await parse_registry_config(
                        manager, 
                        config_data=related_item_data, 
                        registry_assets=parsed_children, 
                        workflow_code_map=workflow_code_map, 
                        reference_map=current_reference_map 
                    )
                    
                    registry_assets.extend(parsed_children)

                    asset.add_relation_to_another_asset(
                        relation_code=getattr(_c.AssetRelationCodes, relation_code.upper()), 
                        target_asset=child_asset, 
                        attributes=_u.create_tree_attribute()
                    )
    # -------------------------------------------------------------------------
    # STEP 4: Process Reference Relations (Resolution)
    # -------------------------------------------------------------------------
    if "relation_by_reference" in config_data:
        async def resolve_references_recursive(source_asset, ref_config_block):
            for ref_key, ref_info in ref_config_block.items():
                target_asset = current_reference_map.get(ref_key)

                if not target_asset:
                    # Licenses are created beforehand
                    license_asset = await datamind.get_asset_by_property(manager=manager, property_name=_c.LICENSE_ID_PROPERTY_CODE, value=ref_key, asset_type=_c.AssetTypeCodes.LICENSE)
                    if not license_asset:
                        # TODO if we want to relate to some existing configs apart from licenses, that logic should be added
                        logger.warning(f"Asset reference '{ref_key}' not found in configuration map.")
                    target_asset = license_asset 


                attributes = {}
                if "attributes" in ref_info:
                    # TODO should be fixed, mark: attribute_list_fix
                    for attr_item in ref_info["attributes"]:
                        if not attributes.get(attr_item["code"], None):
                            attributes[attr_item["code"]] = [_abm.Attribute(**attr_item)]
                        else:
                            attributes[attr_item["code"]].append(_abm.Attribute(**attr_item))
                    ###
                relation_enum = getattr(_c.AssetRelationCodes, ref_info["relation"].upper())
                source_asset.add_relation_to_another_asset(
                    relation_code=relation_enum, 
                    target_asset=target_asset, 
                    attributes=attributes
                )

                # Recursion for nested references (referencing children of the target)
                if "relation_by_reference" in ref_info:
                    await resolve_references_recursive(target_asset, ref_info["relation_by_reference"])


        await resolve_references_recursive(asset, config_data["relation_by_reference"])

    # Schema/Template/Workflow
    if "schema" in config_data:
        schema_asset = _abm.Asset(type_code=asset.get_type_code(), class_code=_c.AssetClassCodes.SCHEMA)
        asset.add_relation_to_another_asset(_c.AssetRelationCodes.HAS_SCHEMA, schema_asset)
        registry_assets.append(schema_asset)

    if "template" in config_data:
        for template_data in config_data['template']:
            template_asset = _abm.Asset(type_code=asset.get_type_code(), class_code=_c.AssetClassCodes.TEMPLATE)
            asset.add_relation_to_another_asset(getattr(_c.AssetRelationCodes, template_data['relation'].upper()), template_asset)
            registry_assets.append(template_asset)

    if "workflow_code" in config_data and workflow_code_map:
        asset.set_workflow(workflow_code_map.get(config_data['workflow_code']))

    registry_assets.append(asset)
    return asset