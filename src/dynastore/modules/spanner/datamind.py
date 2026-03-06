import asyncio
from uuid import UUID
import datetime as datetime
from contextlib import asynccontextmanager

from typing import Optional, List, Dict, Iterable, Any, OrderedDict
from typing import AsyncGenerator # important, due to version (3.12+) it is from typing instead of collection
from google.cloud import spanner
from dynastore.modules.spanner.session_manager import get_or_create_session, get_or_create_transaction
import dynastore.modules.spanner.utils as _u
import dynastore.modules.spanner.tools as _t
import dynastore.modules.spanner.constants as _c
import dynastore.modules.spanner.models.business.principal as _pbm
import dynastore.modules.spanner.models.business.asset as _abm

from google.api_core.exceptions import NotFound, AlreadyExists
from dynastore.modules.spanner.dml_tools import delete_and_return, insert_and_return, update_and_return

import logging

logger = logging.getLogger(__name__)
# TODO, this will be refined
system_principal = _pbm.Principal(uuid=_c.SYSTEM_PRINCIPAL_UUID)

async def get_assets(manager) -> AsyncGenerator[_abm.Asset, None]:
    sql = f"SELECT * FROM asset"

    async with manager.get_session() as session:
        # Use the session object to execute the query
        results = session.execute_sql(sql)
        # Process results (results is an iterator, may need to consume it)
        for row in results:
            yield _u.asset_from_spanner_row(row)

# TODO This could be deleted, only there for demo and test purposes
async def create_asset(manager, asset: _abm.Asset, principal: Optional[_pbm.Principal] = None, tx=None):
    async with get_or_create_transaction(manager, tx) as active_tx:
        config_rel = asset.get_config()
        if not config_rel:
            raise Exception("Config asset should be specified")
        
        # UPDATED: uuid -> asset_uuid
        UUID_PARAM_NAME = 'asset_uuid'
        asset_query = f"SELECT * FROM asset WHERE asset_uuid = @{UUID_PARAM_NAME}"

        results = active_tx.execute_sql(
            asset_query, params={UUID_PARAM_NAME: _u.convert_into_spanner_uuid(asset.get_uuid())})
        try:
            results.one()
            raise Exception("Asset already exists")
        except NotFound as e:
            pass

        # UPDATED: uuid -> asset_uuid
        get_class_type_config_query = f"""
        SELECT p.body as class, a.asset_type_id as type_id, awc.id, awc.asset_workflow_type_id, awc.asset_uuid 
        FROM {_c.ASSET_TABLE_NAME} a
        JOIN @{{JOIN_METHOD=APPLY_JOIN}} {_c.ASSET_PROPERTY_TABLE_NAME} p 
        ON a.asset_uuid = p.asset_uuid
        JOIN @{{JOIN_METHOD=APPLY_JOIN}} {_c.ASSET_WORKFLOW_CONFIG_TABLE_NAME} awc 
        ON a.asset_uuid = awc.asset_uuid
        WHERE a.asset_uuid = @{UUID_PARAM_NAME} 
        AND p.name = '{_c.DEF_PROPERTY_CODE}'
        """
        results = active_tx.execute_sql(get_class_type_config_query, params={
                                    UUID_PARAM_NAME: _u.convert_into_spanner_uuid(config_rel.get_uuid())})
        try:
            class_def, type_id, workflow_config_id, asset_workflow_type_id, asset_uuid= results.one()
        except NotFound as e:
            raise Exception(
                "Asset class and type could not be retrieved from config")
        
        # UPDATED: id -> asset_workflow_edge_id
        get_initial_state_query = f"""
        select awe.source_asset_workflow_state_type_id, awe.target_asset_workflow_state_type_id 
            from {_c.ASSET_WORKFLOW_EDGE_ATTRIBUTE_TABLE_NAME} awea, {_c.ASSET_WORKFLOW_EDGE_TABLE_NAME} awe 
            where awea.asset_workflow_edge_id = awe.asset_workflow_edge_id 
                and awe.asset_workflow_type_id = {asset_workflow_type_id}
                and awea.attribute_code = '{_c.WORKFLOW_EDGE_ATTRIBUTE_INITIAL_CODE}'
        """
        results = active_tx.execute_sql(get_initial_state_query)
        initial_source_state_type_id, initial_target_state_type_id = results.one()
        # TODO reason could be passed
        initial_state = _abm.AssetState(current_state=_c.AssetWorkflowStateCodes(
                                                                          # TODO default system principal might be gotten rid of later
            initial_source_state_type_id), reason="asset create", principal=principal or system_principal)
        asset.add_state(initial_state)
        #Spanner json to string value
        class_type_code = class_def._simple_value
        if not class_type_code:
            raise Exception(
                "Class definition is not present for config instance")
        asset.set_class_code(class_code=getattr(
            _c.AssetClassCodes, class_type_code.upper()))
        asset.set_type_code(type_code=_c.AssetTypeCodes(type_id))
        asset.set_state_code(_c.StateCodes.ACTIVE)
        return await upsert_asset(manager=manager, asset=asset, tx=active_tx, new_asset_workflow_type_id=asset_workflow_type_id)

async def create_assets_in_single_transaction(manager, assets: list[_abm.Asset], principal: Optional[_pbm.Principal] = None):
    async with manager.transaction() as tx:
        for asset in assets:
            await create_asset(manager=manager, asset=asset, principal=principal, tx=tx)

async def update_asset(manager, asset: _abm.Asset):
    asset_uuid = asset.get_uuid()
    asset_state = asset.get_current_state()
    # if not asset_state:
    #     raise Exception("Asset state not found")
    async with manager.transaction() as tx:
        # UPDATED: uuid -> asset_uuid
        UUID_PARAM_NAME = 'asset_uuid'
        asset_query = f"SELECT asset_type_id, asset_class_id FROM asset WHERE asset_uuid = @{UUID_PARAM_NAME}"

        results = tx.execute_sql(
            asset_query, params={UUID_PARAM_NAME: _u.convert_into_spanner_uuid(asset_uuid)})
        try:
            asset_class_id, asset_type_id = results.one()
            asset.set_class_code(_c.AssetClassCodes(asset_class_id))
            asset.set_type_code(_c.AssetTypeCodes(asset_type_id))
        except NotFound as e:
            raise Exception("Asset does not exist")
        ASSET_UUID_PARAM_NAME = 'asset_uuid'

        # TODO actual principal should be set
        if asset_state:
            # TODO it will be necessary to set the actual principal getting from the middleware etc. ###
            asset_state.set_principal(system_principal)
            passed_asset_state_type_id = asset_state.get_current_state().value
            
            # UPDATED: Graph queries now use asset_uuid on nodes
            sql = f"""
                SELECT 
                aws.asset_workflow_type_id,
                aws.current_asset_workflow_state_type_id,
                EXISTS (
                    SELECT 1
                    FROM GRAPH_TABLE({_c.WORKFLOW_GRAPH_NAME}
                    MATCH (source_state: asset_workflow_state_type)-[next_asset_workflow_step]->(target_state: asset_workflow_state_type)
                    WHERE source_state.id = aws.current_asset_workflow_state_type_id 
                        AND target_state.id = {passed_asset_state_type_id}
                        AND next_asset_workflow_step.asset_workflow_type_id = aws.asset_workflow_type_id
                    )
                ) AS is_workflow_step_available,
                a.asset_class_id,
                a.asset_type_id
                FROM asset a
                JOIN @{{JOIN_METHOD=APPLY_JOIN}} asset_workflow_state aws
                ON aws.asset_uuid = a.asset_uuid
                AND aws.change_timestamp = (
                    SELECT MAX(change_timestamp)
                    FROM asset_workflow_state aws2
                    WHERE aws2.asset_uuid = a.asset_uuid
                )
                WHERE a.asset_uuid = @{ASSET_UUID_PARAM_NAME}
            """

            results = tx.execute_sql(
                sql, params={ASSET_UUID_PARAM_NAME: _u.convert_into_spanner_uuid(asset_uuid)})
            asset_workflow_type_id, current_asset_workflow_state_type_id, is_next_state_valid, asset_class_id, asset_type_id = results.one()

            if passed_asset_state_type_id == current_asset_workflow_state_type_id:
                # State is not being changed
                return await upsert_asset(manager=manager,asset=asset, tx=tx, new_asset_workflow_type_id=None)
            elif is_next_state_valid:
                # State is being changed and it is valid
                return await upsert_asset(manager=manager, asset=asset, tx=tx, new_asset_workflow_type_id=asset_workflow_type_id)
            else:
                # Provided next state is not valid
                raise Exception(f"The next state is not valid")
        else:
            return await upsert_asset(manager=manager, asset=asset, tx=tx, new_asset_workflow_type_id=None)

async def create_asset_workflow_config_attachment(manager, config_asset: _abm.AssetConfig, tx):

    asset_workflow_config_dict = _u.asset_workflow_config_to_spanner_dict(
        asset=config_asset)
    insert_and_return(
        transaction=tx,
        table_name=_c.ASSET_WORKFLOW_CONFIG_TABLE_NAME,
        data_to_insert=asset_workflow_config_dict
    )
    
from typing import Dict, Any, Iterable

def yield_row_dicts(spanner_result):
    """Helper generator to safely map Spanner results to dictionaries."""
    columns = None
    for row in spanner_result:
        if columns is None:
            columns = [field.name for field in spanner_result.fields]
        yield dict(zip(columns, row))

async def upsert_asset(manager, asset: _abm.Asset, tx, new_asset_workflow_type_id=None) -> UUID:
    asset_updateable_columns = ['state', 'change_timestamp']
    asset_property_updateable_columns = ['body', 'type_code', 'state']

    def get_update_values(spanner_dict: Dict[str, Any], updateable_columns: Iterable[str]) -> Dict[str, Any]:
        updateable_columns_set = set(updateable_columns)
        return {col: spanner_dict.get(col) for col in updateable_columns_set} # Used .get() for safer comparison

    UUID_PARAM_KEY = "asset_uuid"
    asset_spanner_dict = _u.asset_to_spanner_dict(asset)
    asset_uuid = asset.get_uuid()
    spanner_asset_uuid = _u.convert_into_spanner_uuid(asset_uuid)
    
    # --- 1. ASSET UPSERT (Diffing Applied) ---
    asset_query = f"SELECT * FROM {_c.ASSET_TABLE_NAME} WHERE asset_uuid = @{UUID_PARAM_KEY}"
    asset_result = tx.execute_sql(asset_query, params={UUID_PARAM_KEY: spanner_asset_uuid})
    try:
        asset_row = asset_result.one()
        columns = [field.name for field in asset_result.fields]
        existing_asset_dict = dict(zip(columns, asset_row))
        
        # IN-MEMORY DIFF: Only update if the values actually changed
        new_asset_vals = get_update_values(asset_spanner_dict, asset_updateable_columns)
        existing_asset_vals = get_update_values(existing_asset_dict, asset_updateable_columns)
        
        if new_asset_vals != existing_asset_vals:
            update_and_return(
                transaction=tx, table_name=_c.ASSET_TABLE_NAME,
                update_values=new_asset_vals,
                where_conditions={"asset_uuid": spanner_asset_uuid}
            )
    except NotFound as e:
        insert_and_return(
            transaction=tx, table_name=_c.ASSET_TABLE_NAME,
            data_to_insert=asset_spanner_dict, returning_columns=['*']
        )

    # --- 2. ASSET PROPERTIES (Diffing Applied) ---
    # Fetch the updatable columns too, not just the name
    prop_cols = ", ".join(asset_property_updateable_columns)
    asset_property_query = f"SELECT name, {prop_cols} FROM {_c.ASSET_PROPERTY_TABLE_NAME} WHERE asset_uuid = @{UUID_PARAM_KEY}"
    property_result = tx.execute_sql(asset_property_query, params={UUID_PARAM_KEY: spanner_asset_uuid})
    
    existing_properties = {row['name']: row for row in yield_row_dicts(property_result)}

    for asset_property_code, asset_property in asset.get_properties().items():
        asset_property_spanner_dict = _u.asset_property_to_spanner_dict(asset, asset_property_code, asset_property)
        
        if asset_property_code in existing_properties:
            new_prop_vals = get_update_values(asset_property_spanner_dict, asset_property_updateable_columns)
            existing_prop_vals = get_update_values(existing_properties[asset_property_code], asset_property_updateable_columns)
            
            # IN-MEMORY DIFF: Skip the database write if the property is identical
            if new_prop_vals != existing_prop_vals:
                update_and_return(
                    transaction=tx, table_name=_c.ASSET_PROPERTY_TABLE_NAME,
                    update_values=new_prop_vals,
                    where_conditions={"asset_uuid": spanner_asset_uuid, "name": asset_property_code},
                    explicit_param_types=_u.ASSET_PROPERTY_TYPES
                )
        else:
            insert_and_return(
                transaction=tx, table_name=_c.ASSET_PROPERTY_TABLE_NAME,
                data_to_insert=asset_property_spanner_dict,
                explicit_param_types=_u.ASSET_PROPERTY_TYPES
            )

    # --- 3. ASSET RELATIONS (EDGES) ---
    existing_rels_query = f"""
    graph {_c.ASSET_RELATION_GRAPH_NAME}
        match (source_asset:asset)-[rel:related_to]->(target_asset: asset) 
        where source_asset.asset_uuid =  @{UUID_PARAM_KEY} 
            or target_asset.asset_uuid = @{UUID_PARAM_KEY}  
            return 
                source_asset.asset_uuid as source_uuid,
                rel.asset_relation_type_id as relation_type_id, 
                target_asset.asset_uuid as target_uuid,
                rel.state as state,
                rel.asset_edge_id
    """
    existing_relations = {
        row['asset_edge_id']: {
            "source_asset_uuid": row['source_uuid'],
            "asset_relation_type_id": row['relation_type_id'],
            "target_asset_uuid": row['target_uuid'],
            "state": row['state']
        }
        for row in yield_row_dicts(tx.execute_sql(existing_rels_query, params={UUID_PARAM_KEY: spanner_asset_uuid}))
    }

    existing_edge_ids = list(existing_relations.keys())
    batched_edge_attributes = {}
    
    if existing_edge_ids:
        batched_attr_query = f"SELECT * FROM {_c.ASSET_EDGE_ATTRIBUTE_TABLE_NAME} WHERE asset_edge_id IN UNNEST(@EDGE_IDS)"
        for row in yield_row_dicts(tx.execute_sql(batched_attr_query, params={"EDGE_IDS": existing_edge_ids})):
            edge_id = row['asset_edge_id'] 
            if edge_id not in batched_edge_attributes:
                batched_edge_attributes[edge_id] = {}
            batched_edge_attributes[edge_id][row['id']] = _u.asset_relation_attribute_from_spanner_dict(row)

    for relation_code, asset_relations in asset.get_relations().items():
        for asset_relation in asset_relations:
            relation_spanner_dict = _u.asset_relation_to_spanner_dict(relation_code=relation_code, asset_relation=asset_relation)
            relation_id = None
            
            for existing_relation_id, existing_relation in existing_relations.items():
                if relation_spanner_dict == existing_relation:
                    relation_id = existing_relation_id
                    break
            
            if relation_id is not None:
                existing_attributes = batched_edge_attributes.get(relation_id, {})
                for attribute_code, attributes in asset_relation.get_attributes().items():
                    for attribute in attributes:
                        if attribute in existing_attributes.values():
                            continue
                        relation_attribute_spanner_dict = _u.asset_relation_attribute_to_spanner_dict(relation_id=relation_id, attribute=attribute)
                        insert_and_return(
                            transaction=tx, table_name=_c.ASSET_EDGE_ATTRIBUTE_TABLE_NAME,
                            data_to_insert=relation_attribute_spanner_dict,
                            explicit_param_types=_u.EDGE_ATTRIBUTE_TYPES
                        )
                continue
            
            relation_id = insert_and_return(
                transaction=tx, table_name=_c.ASSET_EDGE_TABLE_NAME,
                data_to_insert=relation_spanner_dict, returning_columns=['asset_edge_id']
            ).pop()

            for attribute_code, attributes in asset_relation.get_attributes().items():
                for attribute in attributes:
                    relation_attribute_spanner_dict = _u.asset_relation_attribute_to_spanner_dict(relation_id=relation_id, attribute=attribute)
                    insert_and_return(
                        transaction=tx, table_name=_c.ASSET_EDGE_ATTRIBUTE_TABLE_NAME,
                        data_to_insert=relation_attribute_spanner_dict,
                        explicit_param_types=_u.EDGE_ATTRIBUTE_TYPES
                    )
    
    # --- 4. TERMS & TRANSLATIONS (Diffing already handled cleanly) ---
    existing_terms_query = f"select term_id, term_code from {_c.TERM_TABLE_NAME} where asset_uuid = @{UUID_PARAM_KEY}"
    existing_terms = {row['term_code']: row['term_id'] for row in yield_row_dicts(tx.execute_sql(existing_terms_query, params={UUID_PARAM_KEY: spanner_asset_uuid}))}

    existing_term_ids = list(existing_terms.values())
    batched_term_translations = {} 
    
    if existing_term_ids:
        batched_translations_query = f"""
            SELECT term_id, language_id, value 
            FROM {_c.TERM_TRANSLATION_TABLE_NAME} 
            WHERE asset_uuid = @{UUID_PARAM_KEY} 
            AND term_id IN UNNEST(@TERM_IDS)
        """
        # Note: You must also pass the UUID_PARAM_KEY in the tx.execute_sql params!
        for row in yield_row_dicts(tx.execute_sql(batched_translations_query, params={"TERM_IDS": existing_term_ids, UUID_PARAM_KEY: spanner_asset_uuid})):
            t_id = row['term_id']
            if t_id not in batched_term_translations:
                batched_term_translations[t_id] = {}
            batched_term_translations[t_id][row['language_id']] = row['value']

    for term_code, term in asset.get_terms().items():
        term_spanner_dict = _u.term_to_spanner_dict(asset=asset, term_code=term_code, term=term)
        
        if term_code not in existing_terms:
            term_id = insert_and_return(
                transaction=tx, table_name=_c.TERM_TABLE_NAME,
                data_to_insert=term_spanner_dict, returning_columns=["term_id"]
            ).pop()
        else:
            term_id = existing_terms[term_code]

        existing_term_translations = batched_term_translations.get(term_id, {})

        for language, translation in term.get_translations().items():
            language_id = translation.get_language_code().get_id()
            
            if language_id in existing_term_translations:
                # IN-MEMORY DIFF: Only updating if translation value changed
                if existing_term_translations[language_id] != translation.value:
                    update_and_return(
                        transaction=tx, table_name=_c.TERM_TRANSLATION_TABLE_NAME,
                        update_values={"value": translation.value},
                        where_conditions={
                            "asset_uuid": spanner_asset_uuid,
                            "term_id": term_id, "language_id": language_id
                        }
                    )
                continue
            
            term_translation_spanner_dict = _u.term_translation_to_spanner_dict(asset_uuid=asset_uuid, term_id=term_id, term_translation=translation)
            insert_and_return(
                transaction=tx, table_name=_c.TERM_TRANSLATION_TABLE_NAME,
                data_to_insert=term_translation_spanner_dict
            )
            
    # --- 5. WORKFLOW STATE ---
    if new_asset_workflow_type_id:
        asset_workflow_state_spanner_dict = _u.asset_workflow_state_to_spanner_dict(asset.get_current_state(), asset.get_uuid(), new_asset_workflow_type_id)
        insert_and_return(
            transaction=tx, table_name=_c.ASSET_WORKFLOW_STATE_TABLE_NAME,
            data_to_insert=asset_workflow_state_spanner_dict
        )
        
    return asset.get_uuid()

# Kept for reference, the new upsert_asset has diffing and is more efficient, but this is the original version without diffing for comparison and fallback if needed
async def upsert_asset_old(manager, asset: _abm.Asset, tx, new_asset_workflow_type_id=None) -> UUID:
    # TODO will be defined somewhere else -----------------------------------
    asset_updateable_columns = ['state', 'change_timestamp']
    asset_property_updateable_columns = ['body', 'type_code', 'state']

    # def get_intersection(a,b):
    #    return list(set(a) & set(b))

    # def get_columns_to_update(spanner_dict, updateable_columns):
    #    return get_intersection(
    #        spanner_dict.keys(), updateable_columns)

    # def get_values_to_update(spanner_dict, columns_to_update) -> tuple:
    #    return tuple(spanner_dict[col] for col in columns_to_update)

    def get_update_values(
        spanner_dict: Dict[str, Any],
        updateable_columns: Iterable[str]
    ) -> Dict[str, Any]:

        updateable_columns_set = set(updateable_columns)  # for fast lookup
        return {col: spanner_dict[col] for col in updateable_columns_set if col in spanner_dict}
    # --------------------------------------------------------------------------------------------

    # UPDATED: uuid -> asset_uuid
    UUID_PARAM_KEY = "asset_uuid"
    asset_spanner_dict = _u.asset_to_spanner_dict(asset)
    asset_uuid = asset.get_uuid()
    spanner_asset_uuid = _u.convert_into_spanner_uuid(asset_uuid)
    asset_query = f"SELECT * FROM {_c.ASSET_TABLE_NAME} WHERE asset_uuid = @{UUID_PARAM_KEY}"
    asset_result = tx.execute_sql(
        asset_query, params={UUID_PARAM_KEY:  spanner_asset_uuid})
    try:
        asset_exists = asset_result.one()
        # columns_to_update = get_columns_to_update(asset_spanner_dict, asset_updateable_columns)
        update_and_return(transaction=tx, table_name=_c.ASSET_TABLE_NAME,
                            update_values=get_update_values(
                                asset_spanner_dict, asset_updateable_columns),
                            where_conditions={"asset_uuid": spanner_asset_uuid}
                            )
        # tx.update(
        #     table=_asset.ASSET_TABLE_NAME,
        #     columns=columns_to_update,
        #     values=get_values_to_update(asset_spanner_dict, columns_to_update)
        # )
    except NotFound as e:
        inserted_asset = insert_and_return(
            transaction=tx,
            table_name=_c.ASSET_TABLE_NAME,
            data_to_insert=asset_spanner_dict,
            returning_columns=['*']
        )
    asset_property_names_query = f"SELECT name FROM {_c.ASSET_PROPERTY_TABLE_NAME} WHERE asset_uuid = @{UUID_PARAM_KEY}"
    property_names_result = tx.execute_sql(
        asset_property_names_query, params={UUID_PARAM_KEY:  spanner_asset_uuid})
    existing_property_names = []
    for result in property_names_result:
        existing_property_names.append(result[0])  # only name is selected
    for asset_property_code, asset_property in asset.get_properties().items():
        asset_property_spanner_dict = _u.asset_property_to_spanner_dict(asset, asset_property_code, asset_property)
        if asset_property_code in existing_property_names:
            update_and_return(transaction=tx,
                                table_name=_c.ASSET_PROPERTY_TABLE_NAME,
                                update_values=get_update_values(
                                    asset_property_spanner_dict, asset_property_updateable_columns),
                                where_conditions={
                                    "asset_uuid": spanner_asset_uuid, "name": asset_property_code},
                                explicit_param_types=_u.ASSET_PROPERTY_TYPES
                                )
        else:
            insert_and_return(
                transaction=tx,
                table_name=_c.ASSET_PROPERTY_TABLE_NAME,
                data_to_insert=asset_property_spanner_dict,
                explicit_param_types=_u.ASSET_PROPERTY_TYPES
            )
    # does not make much sense to update an existing relation
    
    # UPDATED: uuid -> asset_uuid, id -> asset_edge_id
    existing_rels_query = f"""
    graph {_c.ASSET_RELATION_GRAPH_NAME}
        match (source_asset:asset)-[rel:related_to]->(target_asset: asset) 
        where source_asset.asset_uuid =  @{UUID_PARAM_KEY} 
            or target_asset.asset_uuid = @{UUID_PARAM_KEY}  
            return 
                source_asset.asset_uuid as source_uuid,
                rel.asset_relation_type_id as relation_type_id, 
                target_asset.asset_uuid as target_uuid,
                rel.state as state,
                rel.asset_edge_id
    """
    existing_rels_result = tx.execute_sql(
        existing_rels_query, params={UUID_PARAM_KEY:  spanner_asset_uuid})
    existing_relations = {}
    for result in existing_rels_result:
        existing_relations.update(
            {
                result[4]:{ 
                    "source_asset_uuid": result[0],
                    "asset_relation_type_id": result[1],
                    "target_asset_uuid": result[2],
                    "state": result[3]
                }
            }
        )
    for relation_code, asset_relations in asset.get_relations().items():
        # TODO maybe a bit better check is required later
        for asset_relation in asset_relations:
            relation_spanner_dict = _u.asset_relation_to_spanner_dict(relation_code=relation_code, asset_relation=asset_relation)
            relation_id = None
            for existing_relation_id, existing_relation in existing_relations.items():
                if relation_spanner_dict == existing_relation:
                    relation_id = existing_relation_id
                    break
            if relation_id is not None: # This is an existing relation
                SOURCE_UUID_PARAM_NAME = "SOURCE_UUID"
                TARGET_UUID_PARAM_NAME = "TARGET_UUID"
                RELATION_TYPE_ID_PARAM_NAME = "RELATION_TYPE_ID"
                
                # UPDATED: id -> asset_edge_id
                existing_attributes_query=f"""
                select * from {_c.ASSET_EDGE_ATTRIBUTE_TABLE_NAME}
                where asset_edge_id in (
                    select asset_edge_id from {_c.ASSET_EDGE_TABLE_NAME}
                    where
                    source_asset_uuid = @{SOURCE_UUID_PARAM_NAME} and
                    target_asset_uuid = @{TARGET_UUID_PARAM_NAME} and
                    asset_relation_type_id = @{RELATION_TYPE_ID_PARAM_NAME}
                    )
                """
                #TODO attribute update may require test + refactoring
                existing_attributes_result = tx.execute_sql(
                    existing_attributes_query, params={
                        SOURCE_UUID_PARAM_NAME: relation_spanner_dict.get("source_asset_uuid"),
                        TARGET_UUID_PARAM_NAME: relation_spanner_dict.get("target_asset_uuid"),
                        RELATION_TYPE_ID_PARAM_NAME: relation_spanner_dict.get("asset_relation_type_id")
                    })
                existing_attributes = {}
                # TODO seems to be the best approach vs returning TO_JSON or converting from row order, could be made a utility and used more
                columns = None
                for row in existing_attributes_result:
                    # A. Fetching column names dynamically (only needed once)
                    if columns is None:
                        # .fields is a list of metadata objects containing the column name and type
                        columns = [field.name for field in existing_attributes_result.fields]
                    
                    # B. Zipping the names with the values to create a dict
                    row_dict = dict(zip(columns, row))
                    
                    # C. Now we have safe, named dictionary
                    # { 'id': 123, 'attribute_code': 'color', ... }
                    edge_attribute_id = row_dict['id']
                    
                    existing_attributes.update({
                        edge_attribute_id: _u.asset_relation_attribute_from_spanner_dict(row_dict)
                    })
                all_attributes = asset_relation.get_attributes()
                for attribute_code, attributes in all_attributes.items():
                    # TODO should be fixed, mark: attribute_list_fix
                    for attribute in attributes:
                        if attribute in existing_attributes.values():
                            continue
                        relation_attribute_spanner_dict = _u.asset_relation_attribute_to_spanner_dict(relation_id=relation_id, attribute=attribute)
                        insert_and_return(
                            transaction=tx,
                            table_name=_c.ASSET_EDGE_ATTRIBUTE_TABLE_NAME,
                            data_to_insert=relation_attribute_spanner_dict,
                            explicit_param_types=_u.EDGE_ATTRIBUTE_TYPES
                        )
                continue
            
            # UPDATED: returning asset_edge_id
            relation_id = insert_and_return(
                transaction=tx,
                table_name=_c.ASSET_EDGE_TABLE_NAME,
                data_to_insert=relation_spanner_dict,
                returning_columns=['asset_edge_id']
            ).pop()

            all_attributes = asset_relation.get_attributes()
            for attribute_code, attributes in all_attributes.items():
                for attribute in attributes:
                    relation_attribute_spanner_dict = _u.asset_relation_attribute_to_spanner_dict(relation_id=relation_id, attribute=attribute)
                    insert_and_return(
                        transaction=tx,
                        table_name=_c.ASSET_EDGE_ATTRIBUTE_TABLE_NAME,
                        data_to_insert=relation_attribute_spanner_dict,
                        explicit_param_types=_u.EDGE_ATTRIBUTE_TYPES
                    )
    
    # UPDATED: id -> term_id
    existing_terms_query = f"""
    select term_id, term_code from {_c.TERM_TABLE_NAME}
    where asset_uuid =  @{UUID_PARAM_KEY}
    """
    existing_terms_result = tx.execute_sql(
        existing_terms_query, params={UUID_PARAM_KEY:  spanner_asset_uuid})
    existing_terms = {}
    for result in existing_terms_result:
        existing_terms.update(
            {result[1]: result[0]}
        )

    for term_code, term in asset.get_terms().items():
        term_spanner_dict = _u.term_to_spanner_dict(asset=asset, term_code=term_code, term=term)
        term_id = None
        if term_code not in existing_terms.keys():
            # UPDATED: returning term_id
            term_id = insert_and_return(
                transaction=tx,
                table_name=_c.TERM_TABLE_NAME,
                data_to_insert=term_spanner_dict,
                returning_columns=["term_id"]
            ).pop()
        else:
            term_id = existing_terms[term_code]

        existing_term_translations_query = f"""
        select language_id, value from {_c.TERM_TRANSLATION_TABLE_NAME}
        where term_id = {term_id}
        """
        existing_term_translations_result = tx.execute_sql(
            existing_term_translations_query)
        existing_term_translations = {}
        for result in existing_term_translations_result:
            existing_term_translations.update({result[0]: result[1]})

        # ... (Looping through translations) ...
        for language, translation in term.get_translations().items():
            language_id = translation.get_language_code().get_id()
            if language_id in existing_term_translations.keys():
                if existing_term_translations.get(language_id) != translation.value:
                    update_and_return(
                        transaction=tx,
                        table_name=_c.TERM_TRANSLATION_TABLE_NAME,
                        update_values={"value": translation.value},
                        where_conditions={
                            "asset_uuid": spanner_asset_uuid, # <--- For the interleaving to be used
                            "term_id": term_id, 
                            "language_id": language_id
                        }
                    )
                continue
            
            term_translation_spanner_dict = _u.term_translation_to_spanner_dict(asset_uuid=asset_uuid, term_id=term_id, term_translation=translation)
            insert_and_return(
                transaction=tx,
                table_name=_c.TERM_TRANSLATION_TABLE_NAME,
                data_to_insert=term_translation_spanner_dict
            )
    # a state should not be updated in theory
    if new_asset_workflow_type_id:
        asset_workflow_state_spanner_dict = _u.asset_workflow_state_to_spanner_dict(asset.get_current_state(), asset.get_uuid(), new_asset_workflow_type_id)
        insert_and_return(
            transaction=tx,
            table_name=_c.ASSET_WORKFLOW_STATE_TABLE_NAME,
            data_to_insert=asset_workflow_state_spanner_dict
        )
    return asset.get_uuid()

async def create_principal_hierarchy(manager, principal: _pbm.Principal, tx) -> UUID:
    """
    Ensure `principal` exists and recursively ensure all descendant principals exist.
    For each child:
        - create the child principal if missing
        - ensure a principal_edge parent -> child exists
        - recurse into the child's children
    Returns the principal.uuid.
    """
    # ensure the parent principal row exists
    # UPDATED: uuid -> principal_uuid
    principal_uuid_spanner = _u.convert_into_spanner_uuid(principal.get_uuid())
    try:
        q = f"SELECT principal_uuid FROM {_c.PRINCIPAL_TABLE_NAME} WHERE principal_uuid = @uuid"
        res = tx.execute_sql(q, params={"uuid": principal_uuid_spanner})
        res.one()  # exists
    except NotFound:
        await create_principal(manager, principal, tx)

    # retrieve children via model API
    try:
        children = principal.get_children()
    except AttributeError:
        children = None

    if not children:
        return principal.get_uuid()

    for child in children.values():
        child_uuid_spanner = _u.convert_into_spanner_uuid(child.get_uuid())

        # check if child principal exists, create if not
        await create_principal_hierarchy(manager, child, tx)

        # ensure principal_edge parent -> child exists
        try:
            q = f"SELECT id FROM {_c.PRINCIPAL_EDGE_TABLE_NAME} WHERE parent_principal_uuid = @p AND child_principal_uuid = @c"
            tx.execute_sql(q, params={"p": principal_uuid_spanner, "c": child_uuid_spanner}).one()
        except NotFound:
            insert_and_return(
                transaction=tx,
                table_name=_c.PRINCIPAL_EDGE_TABLE_NAME,
                data_to_insert={
                    "parent_principal_uuid": principal_uuid_spanner,
                    "child_principal_uuid": child_uuid_spanner,
                    "state": _c.StateCodes.ACTIVE.value
                },
                returning_columns=["id"]
            )

    return principal.get_uuid()

async def create_principal(manager, principal: _pbm.Principal, tx=None) -> UUID:
    # 2. Run the logic inside the unified context
    async with get_or_create_transaction(manager, tx) as active_tx:

        principal_spanner_dict = _u.principal_to_spanner_dict(principal)

        insert_and_return(
            transaction=active_tx,
            table_name=_c.PRINCIPAL_TABLE_NAME,
            data_to_insert=principal_spanner_dict,
            explicit_param_types=_u.PRINCIPAL_PARAM_TYPES
        )

    return principal.uuid

async def get_principal_by_uuid(manager, principal_uuid: UUID) -> Optional[_pbm.Principal]:
    # UPDATED: uuid -> principal_uuid
    UUID_PARAM_KEY = "principal_uuid"
    principal_query = f"SELECT TO_JSON(p) FROM {_c.PRINCIPAL_TABLE_NAME} p WHERE p.principal_uuid = @{UUID_PARAM_KEY}"

    async with manager.get_session() as session:
        # Use the session object to execute the query
        principal_result = session.execute_sql(
            principal_query, params={UUID_PARAM_KEY:  _u.convert_into_spanner_uuid(principal_uuid)})

        try:
            principal_dict = principal_result.one().pop()
            return _u.principal_from_spanner_dict(principal_dict)
        except NotFound:
            return None

async def delete_principal(manager, principal: _pbm.Principal, hard_delete: Optional[bool] = False):
    principal_uuid_spanner = _u.convert_into_spanner_uuid(principal.get_uuid())

    async with manager.transaction() as tx:
        # TODO not safe, there might be lots of references in other tables
        if hard_delete:
            delete_and_return(
                transaction=tx,
                table_name=_c.PRINCIPAL_EDGE_TABLE_NAME,
                where_conditions={
                    "parent_principal_uuid": principal_uuid_spanner
                }
            )

            delete_and_return(
                transaction=tx,
                table_name=_c.PRINCIPAL_EDGE_TABLE_NAME,
                where_conditions={
                    "child_principal_uuid": principal_uuid_spanner
                }
            )

            # UPDATED: uuid -> principal_uuid
            delete_and_return(
                transaction=tx,
                table_name=_c.PRINCIPAL_TABLE_NAME,
                where_conditions={"principal_uuid": principal_uuid_spanner}
            )

        else:
            # UPDATED: uuid -> principal_uuid
            update_and_return(
                transaction=tx,
                table_name=_c.PRINCIPAL_TABLE_NAME,
                update_values={"state": _c.StateCodes.DELETED.value},
                where_conditions={"principal_uuid": principal_uuid_spanner}
            )

async def _fetch_nodes(manager):
    async with manager.get_session() as session:
        # UPDATED: a.uuid -> a.asset_uuid
        assets_sql = f""" 
            SELECT 
                a.asset_uuid, 
                a.state, 
                tt.value
            FROM {_c.ASSET_TABLE_NAME} a
            LEFT OUTER JOIN {_c.TERM_TABLE_NAME} t ON t.asset_uuid = a.asset_uuid AND t.term_code = '{_c.NAME_PROPERTY_CODE}'
            LEFT OUTER JOIN {_c.TERM_TRANSLATION_TABLE_NAME} tt ON tt.asset_uuid = t.asset_uuid AND tt.term_id = t.term_id AND tt.language_id = {_c.LanguageCodes.EN.get_id()}
            GROUP BY a.asset_uuid, a.state, tt.value
        """
        asset_results = session.execute_sql(assets_sql)
        nodes = []
        for asset_row in asset_results:
            try:
                state = _c.StateCodes(asset_row[1])
                color = None
                if state == _c.StateCodes.ACTIVE:
                    color = "green"
                elif state == _c.StateCodes.DELETED:
                    color = "red"
                elif state == _c.StateCodes.ARCHIVED:
                    color = "yellow"
                nodes.append({_c.GRAPH_NODE_UUID_KEY: str(_u.convert_from_spanner_uuid(asset_row[0])),
                              _c.GRAPH_NODE_COLOR_KEY: color,
                              _c.GRAPH_NODE_LABEL_KEY: asset_row[2]})
            except Exception as e:
                logger.error(str(e))

    return nodes

async def _fetch_edges(manager):
    async with manager.get_session() as session:
        # UPDATED: edg.id -> edg.asset_edge_id
        edges_sql = f"""
            SELECT 
                ARRAY(
                    SELECT attr.attribute 
                    FROM {_c.ASSET_EDGE_ATTRIBUTE_TABLE_NAME} attr 
                    WHERE attr.asset_edge_id = edg.asset_edge_id
                ) AS attributes,
                edg.asset_edge_id AS asset_edge_id,
                edg.source_asset_uuid, 
                edg.target_asset_uuid, 
                edg.asset_relation_type_id
            FROM {_c.ASSET_EDGE_TABLE_NAME} edg
        """
        edges_results = session.execute_sql(edges_sql)
        edges = []
        for edges_row in edges_results:
            try:
                attributes = []
                for attribute_data in edges_row[0]:
                    attributes.append(attribute_data)
                attribute = ', '.join(attributes)

                edge_type = _c.AssetRelationCodes(edges_row[4])
                # TODO could be an object rather than a dict
                edges.append({_c.GRAPH_EDGE_SOURCE_KEY: str(_u.convert_from_spanner_uuid(edges_row[2])),
                              _c.GRAPH_EDGE_TARGET_KEY: str(_u.convert_from_spanner_uuid(edges_row[3])),
                              _c.GRAPH_EDGE_LABEL_KEY: edge_type.name,
                              _c.GRAPH_EDGE_TITLE_KEY: attribute}
                              )
            except Exception as e:
                logger.error(str(e))
    return edges

async def get_graph_for_visualization(manager):
    # This is where the magic happens.
    # We offload the blocking sync functions to separate threads.
    # The Event Loop stays free, and the OS handles the parallel I/O.
    nodes, edges = await asyncio.gather(
        await asyncio.to_thread(_fetch_nodes, manager),
        await asyncio.to_thread(_fetch_edges, manager)
    )
    
    return nodes, edges

async def get_asset_by_uuid(manager, asset_uuid: UUID, allowed_states: Optional[List[str]] = None, deep=False, session=None) -> Optional[_abm.Asset]:
    """
    Fetches a single asset by wrapping the UUID in a list and calling the batch fetcher.
    """
    
    if allowed_states is None:
        allowed_states = [state.value for state in _c.StateCodes]  # default to all states

    # 1. Call the batch function with a single-item list
    assets = await get_assets_by_uuids(
        manager=manager,
        asset_uuids=[asset_uuid],
        allowed_states=allowed_states,
        deep=deep,
        session=session
    )
    
    # 2. Return the first asset if it exists, otherwise return None
    return assets[0] if assets else None

async def get_assets_by_uuids(manager, asset_uuids: List[UUID], allowed_states: Optional[List[str]] = None, deep=False, session=None) -> List[_abm.Asset]:
    if not asset_uuids:
        return []
    
    if allowed_states is None:
        allowed_states = [state.value for state in _c.StateCodes]  # default to all states

    UUIDS_PARAM_KEY = "asset_uuids"
    ALLOWED_STATES_PARAM = "allowed_states"
    # Convert all standard UUIDs to Spanner's string format
    spanner_asset_uuids = [_u.convert_into_spanner_uuid(uuid) for uuid in asset_uuids]

    # --- SQL Queries using UNNEST ---
    
    asset_query = f"""
        SELECT * FROM {_c.ASSET_TABLE_NAME} 
        WHERE asset_uuid IN UNNEST(@{UUIDS_PARAM_KEY})
        AND state IN UNNEST(@{ALLOWED_STATES_PARAM})
    """

    relations_query = f"""
        SELECT
        ae.asset_edge_id AS asset_edge_id,
        ae.source_asset_uuid,
        ae.target_asset_uuid,
        ae.asset_relation_type_id,
        ARRAY(
            SELECT AS STRUCT
            aea.id AS attribute_id,
            aea.attribute_code,
            aea.attribute
            FROM {_c.ASSET_EDGE_ATTRIBUTE_TABLE_NAME} aea
            WHERE aea.asset_edge_id = ae.asset_edge_id
        ) AS asset_edge_attributes
        FROM {_c.ASSET_EDGE_TABLE_NAME} ae
        WHERE (ae.source_asset_uuid IN UNNEST(@{UUIDS_PARAM_KEY})
           OR ae.target_asset_uuid IN UNNEST(@{UUIDS_PARAM_KEY}))
        AND ae.state IN UNNEST(@{ALLOWED_STATES_PARAM})
        ORDER BY ae.asset_edge_id
    """

    properties_query = f"""
        SELECT *
        FROM {_c.ASSET_PROPERTY_TABLE_NAME}
        WHERE asset_uuid IN UNNEST(@{UUIDS_PARAM_KEY})
        AND state IN UNNEST(@{ALLOWED_STATES_PARAM})
    """

    # ALLOWED_STATES_PARAM is not needed for workflow states, state deletion in workflows might require special attention
    workflow_states_query = f"""
        SELECT state.*
        FROM (
            SELECT ARRAY_AGG(
                t ORDER BY t.change_timestamp DESC LIMIT {100 if deep else 1}
            ) as states
            FROM {_c.ASSET_WORKFLOW_STATE_TABLE_NAME} t
            WHERE t.asset_uuid IN UNNEST(@{UUIDS_PARAM_KEY})
            GROUP BY t.asset_uuid
        )
        CROSS JOIN UNNEST(states) AS state
    """

    terms_query = f"""
        SELECT
            t.asset_uuid,
            t.term_code,
            ARRAY(
                SELECT AS STRUCT tt.language_id, tt.value
                FROM {_c.TERM_TRANSLATION_TABLE_NAME} tt
                WHERE tt.term_id = t.term_id
            ) AS term_translation
        FROM {_c.TERM_TABLE_NAME} t
        WHERE t.asset_uuid IN UNNEST(@{UUIDS_PARAM_KEY})
        AND t.state IN UNNEST(@{ALLOWED_STATES_PARAM})
        GROUP BY t.asset_uuid, t.term_id, t.term_code
    """

    async with get_or_create_session(manager, session) as session:
        # 1. Fetch the Base Assets
        # Pass the list of strings to Spanner
        asset_results = session.execute_sql(
            asset_query, params={UUIDS_PARAM_KEY: spanner_asset_uuids, ALLOWED_STATES_PARAM: allowed_states}
        )
        
        # Create a lookup dictionary: { "uuid_string": AssetObject }
        assets_map = {}
        for row in asset_results:
            asset = _u.asset_from_spanner_row(row)
            assets_map[str(asset.get_uuid())] = asset

        if not assets_map:
            return []

        # 2. Define Parallel Tasks for hydration
        async def _fetch_relations():
            if not deep: return
            relation_results = session.execute_sql(
                relations_query, params={UUIDS_PARAM_KEY: spanner_asset_uuids, ALLOWED_STATES_PARAM: allowed_states}
            )
            for relation_result in relation_results:
                asset_edge_id, source_uuid_raw, target_uuid_raw, rel_type_id, attributes_array = relation_result
                
                attributes = _u.build_attributes_from_attributes_array(attributes_array)
                source_uuid_str = str(_u.convert_from_spanner_uuid(source_uuid_raw))
                target_uuid_str = str(_u.convert_from_spanner_uuid(target_uuid_raw))
                
                # Check if the source or target is in our current batch map and attach it
                if source_uuid_str in assets_map:
                    assets_map[source_uuid_str].add_relation_to_another_asset(
                        target_asset=_abm.Asset(uuid=UUID(target_uuid_str)),
                        relation_code=_c.AssetRelationCodes(rel_type_id),
                        attributes=attributes
                    )
                if target_uuid_str in assets_map:
                    assets_map[target_uuid_str].add_relation_from_another_asset(
                        source_asset=_abm.Asset(uuid=UUID(source_uuid_str)),
                        relation_code=_c.AssetRelationCodes(rel_type_id),
                        attributes=attributes
                    )

        async def _fetch_properties():
            property_results = session.execute_sql(
                properties_query, params={UUIDS_PARAM_KEY: spanner_asset_uuids, ALLOWED_STATES_PARAM: allowed_states}
            )
            for prop_row in property_results:
                prop = _u.asset_property_from_spanner_row(prop_row)
                asset_uuid_str = str(_u.convert_from_spanner_uuid(prop_row[0]))
                
                if asset_uuid_str in assets_map:
                    assets_map[asset_uuid_str].add_property(property_name=prop.get_name(), property=prop)

        async def _fetch_workflow_states():
            workflow_states_result = session.execute_sql(
                workflow_states_query, params={UUIDS_PARAM_KEY: spanner_asset_uuids}
            )
            for state_row in workflow_states_result:
                workflow_state = _u.asset_workflow_state_from_spanner_row(state_row)
                asset_uuid_str = str(_u.convert_from_spanner_uuid(state_row[0]))
                
                if asset_uuid_str in assets_map:
                    assets_map[asset_uuid_str].add_state(workflow_state)

        async def _fetch_terms():
            if not deep: return
            terms_result = session.execute_sql(
                terms_query, params={UUIDS_PARAM_KEY: spanner_asset_uuids, ALLOWED_STATES_PARAM: allowed_states}
            )
            for term_result in terms_result:
                asset_uuid_raw, term_code, translations_array = term_result
                asset_uuid_str = str(_u.convert_from_spanner_uuid(asset_uuid_raw))

                if asset_uuid_str in assets_map:
                    translations = {}
                    for translation_result in translations_array:
                        language_id, value = translation_result
                        language_code = _c.LanguageCodes.get_language_by_id(language_id)
                        translations[language_code] = _abm.TermTranslation(
                            language_code=language_code, value=value
                        )
                    
                    assets_map[asset_uuid_str].add_term(term_code=term_code, term=_abm.Term(translations=translations))

        # 3. Execute hydration concurrently
        await asyncio.gather(
            _fetch_relations(),
            _fetch_properties(),
            _fetch_workflow_states(),
            _fetch_terms()
        )

    # Return the fully hydrated assets in a list
    return list(assets_map.values())

async def get_valid_asset_types(manager, offset=0, limit=10):
    sql = f"""
            SELECT *
            FROM {_c.ASSET_TYPE_TABLE_NAME} 
            order by id asc
            LIMIT {limit} OFFSET {offset}
        """
    async with manager.get_session() as session:
        results = session.execute_sql(sql)
        for row in results:
            yield _u.asset_type_from_spanner_row(row)

async def get_valid_asset_classes(manager, offset=0, limit=10):
    sql = f"""
            SELECT *
            FROM {_c.ASSET_CLASS_TABLE_NAME} 
            order by id asc
            LIMIT {limit} OFFSET {offset}
        """
    async with manager.get_session() as session:
        results = session.execute_sql(sql)
        for row in results:
            yield _u.asset_class_from_spanner_row(row)

async def get_next_state_list(manager, asset_uuid) -> AsyncGenerator[_abm.State, None]:

    ASSET_UUID_PARAM_NAME = 'asset_uuid'
    sql = f"""
    
        GRAPH {_c.WORKFLOW_GRAPH_NAME} 
        MATCH (prev_state)-[e0:next_asset_workflow_step]->(next_state)
        WHERE (prev_state.id, e0.asset_workflow_type_id) = (
            SELECT AS STRUCT s.current_asset_workflow_state_type_id, s.asset_workflow_type_id
            FROM asset_workflow_state s
            WHERE s.asset_uuid = @{ASSET_UUID_PARAM_NAME}
            ORDER BY s.change_timestamp DESC
            LIMIT 1
        )
        RETURN to_json(next_state) AS next_state
    """

    async with manager.get_session() as session:
        # Use the session object to execute the query
        results = session.execute_sql(
            sql, params={ASSET_UUID_PARAM_NAME: _u.convert_into_spanner_uuid(asset_uuid)})
        # Process results (results is an iterator, may need to consume it)

        # next_status_list = [] # *** TODO I am not sure if it makes sense to return a stream here
        # for row in results:
        #     next_status_list.append(row[0][_su.SPANNER_GRAPH_PROPS_KW])
        # return next_status_list

        for row in results:
            yield _u.state_from_spanner_dict(row[0][_c.SPANNER_GRAPH_PROPS_KW])
            
async def search_assets(manager, asset_type_ids=None, asset_class_ids=None, offset=0, limit=10, deep=False) -> AsyncGenerator[_abm.Asset, None]:
    conditions = []
    if asset_type_ids:
        # Assuming asset_type_id is a list of IDs
        placeholders = ", ".join(str(asset_type_id)
                                    for asset_type_id in asset_type_ids)
        conditions.append(f"asset_type_id IN ({placeholders})")
    if asset_class_ids:
        # Assuming asset_class_id is a list of IDs
        placeholders = ", ".join(str(asset_class_ids)
                                    for asset_class_ids in asset_class_ids)
        conditions.append(f"asset_class_id IN ({placeholders})")

    async for asset in get_asset_paginated_generic(manager=manager, conditions=conditions, offset=offset, limit=limit, deep=deep):
        yield asset
        
async def get_asset_paginated_generic(manager, conditions=None, offset=0, limit=10, deep=False):
    where_clause = " AND ".join(conditions) if conditions else "TRUE"

    # UPDATED: ORDER BY asset_uuid
    sql = f"""SELECT * FROM asset
            WHERE {where_clause}
            ORDER BY asset_uuid asc
            LIMIT {limit} OFFSET {offset}
        """

    async with manager.get_session() as session:
        results = session.execute_sql(sql)
        for row in results:
            asset = _u.asset_from_spanner_row(row)
            if deep:
                asset = await get_asset_by_uuid(manager=manager, asset_uuid=asset.uuid, deep=True, session=session)
            yield asset

from google.cloud.spanner import param_types

async def get_asset_by_tree_path(
        manager,
        base_asset_uuid: UUID,
        path_asset_uuids: Optional[List[UUID]] = None, 
        target_uuid: Optional[UUID] = None,
        depth: Optional[int] = 100
    ) -> _abm.Asset:
    
    if path_asset_uuids is None:
        path_asset_uuids = []
    
    PATH_UUIDS_PARAM_NAME = 'path_uuids'
    BASE_ASSET_UUID_PARAM_NAME = 'base_asset_uuid'
    TARGET_UUID_PARAM_NAME = 'target_uuid'
    tree_attribute = _u.get_tree_attribute()

    # 1. CRITICAL FIX: Explicitly define types.
    # This stops the "INT64 vs BYTES" crash when the list is empty.
    query_param_types = {
        PATH_UUIDS_PARAM_NAME: param_types.Array(param_types.BYTES),
        BASE_ASSET_UUID_PARAM_NAME: param_types.BYTES,
        TARGET_UUID_PARAM_NAME: param_types.BYTES
    }
    
    # TODO just kept to fallback if needed, will be deleted
    sql_old = f"""
        SELECT
            to_json(gt.base_asset) AS base_asset,
            to_json(gt.relation) AS relation,
            to_json(gt.nodes) AS nodes,
            to_json(gt.target_asset) AS target_asset
        FROM GRAPH_TABLE(
            {_c.ASSET_RELATION_GRAPH_NAME}
            -- DEPTH 100
            MATCH p = (base_asset: asset)-[edge: related_to]->{{0,{depth}}}(target_asset: asset)
            WHERE
                base_asset.asset_uuid = @{BASE_ASSET_UUID_PARAM_NAME}

            -- 1. TARGET ANCHORING (Correct Logic)
            AND ({f"target_asset.asset_uuid = @{TARGET_UUID_PARAM_NAME}" if target_uuid else "TRUE"})

            -- 2. ACYCLIC CHECK
            AND ((
                SELECT COUNT(DISTINCT n.asset_uuid) 
                FROM UNNEST(nodes(p)) AS n
            ) = ARRAY_LENGTH(nodes(p)) or ARRAY_LENGTH(@{PATH_UUIDS_PARAM_NAME}) = 0)

            -- 3. MANDATORY INTERMEDIATES (Correct Logic)
            AND ((
                SELECT COUNT(DISTINCT n.asset_uuid)
                FROM UNNEST(nodes(p)) AS n
                WHERE n.asset_uuid IN UNNEST(@{PATH_UUIDS_PARAM_NAME})
            ) = ARRAY_LENGTH(@{PATH_UUIDS_PARAM_NAME}) or ARRAY_LENGTH(@{PATH_UUIDS_PARAM_NAME}) = 0)

            -- 4. THE PERFORMANCE FIX (Uncorrelated Subquery)
            AND NOT EXISTS (
                SELECT 1
                FROM UNNEST(edge) AS single_edge
                WHERE single_edge.asset_edge_id NOT IN (
                    SELECT asset_edge_id 
                    FROM {_c.ASSET_EDGE_ATTRIBUTE_TABLE_NAME}
                    WHERE attribute_code = '{tree_attribute.get_code()}'
                    AND attribute = '{tree_attribute.get_attribute()}'
                )
            )
        
        COLUMNS (
            base_asset, 
            edge as relation, 
            nodes(p) AS nodes, 
            target_asset
        )
        ) AS gt
        ORDER BY ARRAY_LENGTH(gt.relation) ASC
    """

    # TODO will be deleted or used based on the decision, requires ddl update -------------------------------------------
    sql = f"""
        SELECT
            to_json(gt.base_asset) AS base_asset,
            to_json(gt.relation) AS relation,
            to_json(gt.nodes) AS nodes,
            to_json(gt.target_asset) AS target_asset
        FROM GRAPH_TABLE(
            {_c.ASSET_RELATION_GRAPH_NAME}
            
            -- PERFORMANCE FIX: 
            -- 1. We use the 'attributed_link' label (mapped to the View).
            -- 2. We inject the attribute filters directly here. 
            -- This forces the DB to scan ONLY the valid edges before traversing.
            MATCH p = (base_asset: asset)-[edge: attributed_link 
                        WHERE edge.attribute_code = '{tree_attribute.get_code()}' 
                        AND edge.attribute = '{tree_attribute.get_attribute()}'
                        
                    ]->{{0,{depth}}}(target_asset: asset)
            
            WHERE
                base_asset.asset_uuid = @{BASE_ASSET_UUID_PARAM_NAME}

            -- 1. TARGET ANCHORING
            AND ({f"target_asset.asset_uuid = @{TARGET_UUID_PARAM_NAME}" if target_uuid else "TRUE"})

            -- 2. STRICT ACYCLIC CHECK (Prevent infinite loops)
            -- If the number of unique nodes equals the total number of nodes, there are no circles.
            AND (
                SELECT COUNT(DISTINCT n.asset_uuid) FROM UNNEST(nodes(p)) AS n
            ) = ARRAY_LENGTH(nodes(p))

            -- 3. MANDATORY INTERMEDIATES
            -- If the user provided a list of mandatory UUIDs, make sure every single one exists somewhere in this path.
            AND (
                ARRAY_LENGTH(@{PATH_UUIDS_PARAM_NAME}) = 0 
                OR 
                (
                    SELECT COUNT(DISTINCT n.asset_uuid) 
                    FROM UNNEST(nodes(p)) AS n 
                    WHERE n.asset_uuid IN UNNEST(@{PATH_UUIDS_PARAM_NAME})
                ) = ARRAY_LENGTH(@{PATH_UUIDS_PARAM_NAME})
            )
            -- NOTE: The slow "NOT EXISTS" subquery is removed.
            -- The filtering is now handled by the View inside the MATCH clause.
        
        COLUMNS (
            base_asset, 
            edge as relation, 
            nodes(p) AS nodes, 
            target_asset
        )
        ) AS gt
        ORDER BY ARRAY_LENGTH(gt.relation) ASC
    """
    
    async with manager.get_session() as session:
        result = session.execute_sql(
            sql,
            params={
                BASE_ASSET_UUID_PARAM_NAME: _u.convert_into_spanner_uuid(base_asset_uuid),
                PATH_UUIDS_PARAM_NAME: [_u.convert_into_spanner_uuid(uuid) for uuid in path_asset_uuids],
                TARGET_UUID_PARAM_NAME: _u.convert_into_spanner_uuid(target_uuid) if target_uuid else None
            },
            param_types=query_param_types  # <--- THIS IS REQUIRED
        )
    assets_by_id = {} # Key: UUID, Value: Asset Object
    base_asset = None

    # IF the target is not passed, there could be multiple results.
    # We take the first one (the shortest path).
    try:
        source_asset_row, relations_row, nodes_row, target_asset_row = result.to_dict_list().pop().values()
    except Exception:
        raise NotFound(f"No asset tree found for base_asset with uuid : {base_asset_uuid}")
        

    for _source_asset_row, _relation_row, _target_asset_row in zip(nodes_row._array_value[:-1], relations_row._array_value, nodes_row._array_value[1:]):

        # 1. Resolve Source Asset
        source_props = _source_asset_row.get(_c.SPANNER_GRAPH_PROPS_KW)
        # Assuming the dict has a unique ID field, e.g., 'uuid' or 'id'
        # UPDATED: uuid -> asset_uuid
        source_id = source_props.get('asset_uuid') 
        
        if source_id in assets_by_id:
            source_asset = assets_by_id[source_id]
        else:
            source_asset = _u.asset_from_spanner_dict(source_props)
            assets_by_id[source_id] = source_asset

        # 2. Set Base Asset (First source encountered)
        if not base_asset:
            base_asset = source_asset

        # 3. Resolve Target Asset
        target_props = _target_asset_row.get(_c.SPANNER_GRAPH_PROPS_KW)
        # UPDATED: uuid -> asset_uuid
        target_id = target_props.get('asset_uuid')

        if target_id in assets_by_id:
            raise Exception("Unexpected: Loop detected in tree traversal.")
            #target_asset = assets_by_id[target_id]
        else:
            target_asset = _u.asset_from_spanner_dict(target_props)
            assets_by_id[target_id] = target_asset

        # 4. Link them
        if len(_relation_row) > 0:
            rel_props = _relation_row.get(_c.SPANNER_GRAPH_PROPS_KW)
            relation = _u.build_relation_from_spanner_dict(
                source_asset, 
                target_asset, 
                rel_props
            )
            source_asset.attach_relation_to_another_asset(relation)

    return base_asset


async def create_registry_old(manager, registry_config: dict, workflow_code_map):

    root = registry_config.get("root", [])
    registry_property = root.get("property").get(_c.REGISTRY_PROPERTY_CODE)
    if not registry_property:
        raise ValueError(f"Registry config must have a property with code {_c.REGISTRY_PROPERTY_CODE}")

    registry_property_value = registry_property.get('value')
    existing_registry_asset = await get_asset_by_property(manager, _c.REGISTRY_PROPERTY_CODE, registry_property_value)
    if existing_registry_asset:
        raise AlreadyExists(f"Registry with property {_c.REGISTRY_PROPERTY_CODE}='{registry_property_value}' already exists with UUID {existing_registry_asset.get_uuid()}")

    registry_assets = []
    await _t.parse_registry_config(root, registry_assets, workflow_code_map)

    await create_configs(manager, registry_assets)

async def create_registry(manager, registry_config: dict, workflow_code_map):
    root_entries = registry_config.get("root", [])
    if isinstance(root_entries, dict):
        root_entries = [root_entries]

    all_registry_assets = []
# ---------------------------------------------------------
    # 1. VALIDATION
    # ---------------------------------------------------------
    # The Registry is now a child of the Team, located in:
    # relations -> has_registry -> [Registry Object]
    for entry_data in root_entries:
            # 1. Validation: Check for duplicate Registry
            for relation in entry_data["relations"]:
                for reg_data in relation[_c.AssetRelationCodes.HAS_REGISTRY.name.lower()]:
                    reg_prop = reg_data.get("property", {}).get(_c.REGISTRY_PROPERTY_CODE)
                    if reg_prop:
                        val = reg_prop.get('value')
                        existing = await get_asset_by_property(manager, _c.REGISTRY_PROPERTY_CODE, val)
                        if existing:
                            raise AlreadyExists(f"Registry with property {_c.REGISTRY_PROPERTY_CODE}='{val}' already exists.")

    # ---------------------------------------------------------
    # 2. PARSE (Start Recursion)
    # ---------------------------------------------------------
    for entry_data in root_entries:
        await _t.parse_registry_config(
            manager, 
            config_data=entry_data, 
            registry_assets=all_registry_assets, 
            workflow_code_map=workflow_code_map, 
            reference_map={}
        )
    await create_configs(manager, all_registry_assets)

async def create_configs(manager, asset_obj_list: _abm.AssetConfig):

    try:
        async with manager.transaction() as tx:
            for asset_obj in asset_obj_list:
                # NEEDS TO BE VERIFIED
                asset_dict = _u.asset_to_spanner_dict(asset=asset_obj)
                insert_and_return(
                    transaction=tx,
                    table_name=_c.ASSET_TABLE_NAME,
                    data_to_insert=asset_dict
                )

            for asset in asset_obj_list:
                await upsert_asset(manager=manager, asset=asset, tx=tx)
                if isinstance(asset, _abm.AssetConfig):
                    await create_asset_workflow_config_attachment(manager=manager, config_asset=asset, tx=tx)
        #
    except Exception as e:
        logger.error(f"Error occured while inserting {asset_obj_list}")
        raise e

async def create_asset_workflow_type(manager, workflow: _abm.AssetWorkflowType, tx
                                     ) -> _abm.AssetWorkflowType:
    # Insert main asset_workflow_type row
    workflow_dict = _u.asset_workflow_type_to_spanner_dict(workflow)
    workflow_type_id = insert_and_return(
        transaction=tx,
        table_name=_c.ASSET_WORKFLOW_TYPE_TABLE_NAME,
        data_to_insert=workflow_dict,
        returning_columns=["id"]
    ).pop()

    # Insert edges for this workflow type
    for edge in workflow.get_next_state_edges():
        edge_dict = _u.asset_workflow_edge_to_spanner_dict(workflow_type_id, edge)
        # UPDATED: returning id -> asset_workflow_edge_id
        edge_id = insert_and_return(
            transaction=tx,
            table_name=_c.ASSET_WORKFLOW_EDGE_TABLE_NAME,
            data_to_insert=edge_dict,
            returning_columns=["asset_workflow_edge_id"]
        ).pop()

        # Insert attributes for this edge if edge_id available
        if edge_id and edge.get_attributes():
            for attr_code, attributes in edge.get_attributes().items():
                for attribute in attributes:
                    attr_dict = _u.asset_workflow_edge_attribute_to_spanner_dict(
                        edge_id, attribute)
                    insert_and_return(
                        transaction=tx,
                        table_name=_c.ASSET_WORKFLOW_EDGE_ATTRIBUTE_TABLE_NAME,
                        data_to_insert=attr_dict,
                        explicit_param_types=_u.EDGE_ATTRIBUTE_TYPES
                    )
    workflow.set_id(workflow_type_id)
    # TODO Addition of the asset_workflow_config
    return workflow

async def get_workflow_type_by_name(manager, workflow_name: str, state: _c.StateCodes=None) -> Optional[_abm.AssetWorkflowType]:
    if not state:
        state = _c.StateCodes.ACTIVE
    WORKFLOW_NAME_PARAM = 'workflow_name'
    sql = f"""
            SELECT TO_JSON(w)
            FROM {_c.ASSET_WORKFLOW_TYPE_TABLE_NAME} w
            WHERE w.name = @{WORKFLOW_NAME_PARAM}
            AND w.state = {state.value}
            LIMIT 1
        """

    async with manager.get_session() as session:
        results = session.execute_sql(
            sql,
            params={
                WORKFLOW_NAME_PARAM: workflow_name
            }
        )
        #try:
        row = results.one().pop()
        return _u.asset_workflow_type_from_spanner_dict(row)
        #except NotFound:
        #    return None

async def get_asset_by_property(
    manager, 
    property_name: str, 
    value: Any, 
    asset_type: _c.AssetTypeCodes = None,  
    asset_class: _c.AssetClassCodes = None,
    # 1. Make it a list, defaulting to None so we can safely set it to ACTIVE
    allowed_states: Optional[List[_c.StateCodes]] = None 
) -> Optional[_abm.Asset]:

    # 2. Assign default, and strictly block empty lists
    if allowed_states is None:
        allowed_states = [_c.StateCodes.ACTIVE]
        
    if not allowed_states:
        raise ValueError("allowed_states cannot be empty. You must explicitly define which states to query.")

    PROPERTY_NAME_PARAM = 'property_name'
    VALUE_PARAM = 'value'
    ALLOWED_STATES_PARAM = 'allowed_states'

    state_values = [s.value for s in allowed_states]

    # 3. Clean, strict SQL using UNNEST
    sql = f"""
            SELECT TO_JSON(a)
            FROM asset a
            JOIN asset_property p ON a.asset_uuid = p.asset_uuid
            WHERE p.name = @{PROPERTY_NAME_PARAM} 
            AND JSON_VALUE(p.body, '$') = @{VALUE_PARAM}
            
            AND a.state IN UNNEST(@{ALLOWED_STATES_PARAM})
            AND p.state IN UNNEST(@{ALLOWED_STATES_PARAM})
            
            AND ({"TRUE" if asset_type is None else f"a.asset_type_id = {asset_type.value}"})
            AND ({"TRUE" if asset_class is None else f"a.asset_class_id = {asset_class.value}"})
            LIMIT 1
        """

    async with manager.get_session() as session:
        results = session.execute_sql(
            sql,
            params={
                PROPERTY_NAME_PARAM: property_name,
                VALUE_PARAM: value,
                ALLOWED_STATES_PARAM: state_values
            },
            param_types={
                ALLOWED_STATES_PARAM: param_types.Array(param_types.INT64)
            }
        )
        try:
            asset_dict = results.one().pop()
            return _u.asset_from_spanner_dict(asset_dict)
        except NotFound:
            return None