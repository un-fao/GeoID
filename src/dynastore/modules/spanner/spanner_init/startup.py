import dynastore.modules.spanner.datamind as datamind
import dynastore.modules.spanner.constants as _c
from dynastore.modules.spanner.dml_tools  import insert_and_return
from dynastore.modules.spanner.session_manager import SpannerSessionManager
from dynastore.modules.spanner.spanner_init.ddl import create_table_using_ddl
from google.api_core.exceptions import NotFound, InvalidArgument

async def initialize(module, manager: SpannerSessionManager):
    try:
        await datamind.get_assets(manager).__anext__() 
    except (StopAsyncIteration, InvalidArgument):
        await init_db(module,manager)
    
async def init_db(module, manager: SpannerSessionManager):
    await create_table_using_ddl(module.client, module.database)
    await insert_constants(manager)

async def insert_constants(manager):
    async with manager.transaction() as tx:
        await insert_enum_data(tx, _c.AssetClassCodes, _c.ASSET_CLASS_TABLE_NAME, ["id", "class_code"])
        await insert_enum_data(tx, _c.AssetTypeCodes, _c.ASSET_TYPE_TABLE_NAME, ["id", "type_code"])
        await insert_enum_data(tx, _c.AssetRelationCodes, _c.ASSET_RELATION_TYPE_TABLE_NAME, ["id", "relation_name"], has_state=True)
        await insert_enum_data(tx, _c.AssetWorkflowStateCodes, _c.ASSET_WORKFLOW_STATE_TYPE_TABLE_NAME, ["id", "name"], has_state=True)
        await insert_enum_data(tx, _c.PrincipalTypes, _c.PRINCIPAL_TYPE_TYPE_TABLE_NAME, ["id", "type_code"])
        #await self.insert_enum_data(tx, _c.GenericPermissionBits, _c.GENERIC_PERMISSION_BIT_TABLE_NAME, ["bit_value","name"])
        
        await insert_languages(tx, _c.LanguageCodes)
        # TODO this would require refinements, not worth at the moment
        # await self.insert_enum_data(tx,_c.LanguageCodes, _c.LANGUAGE_TABLE_NAME, ["id", "code", "name"])

async def insert_enum_data(tx, enum_class, table_name, column_names, has_state=False):
    results = tx.execute_sql(
        f"SELECT {column_names[0]} FROM {table_name}"
    )
    existing_keys = []
    for result in results:
        existing_keys.append(result[0])

    for item in enum_class:
        key_value = item.value
        # Check if row with key exists

        if key_value not in existing_keys:
            data_to_insert = {
                column_names[1]: item.name.lower(), column_names[0]: key_value}
            if has_state:
                data_to_insert.update(
                    {"state": _c.StateCodes.ACTIVE.value})
            insert_and_return(
                transaction=tx,
                table_name=table_name,
                data_to_insert=data_to_insert
            )

# TODO will be refined later        
async def insert_languages(tx, enum_class):
    results = tx.execute_sql(
        f"SELECT code FROM {_c.LANGUAGE_TABLE_NAME}"
    )
    existing_keys = []
    for result in results:
        existing_keys.append(result[0])
        
    for item in enum_class:
        code = item.name.lower()
        if code not in existing_keys:
            insert_and_return(
                transaction=tx,
                table_name=_c.LANGUAGE_TABLE_NAME,
                data_to_insert={
                    "id": item.get_id(),
                    "name": item.get_name(),
                    "code": item.name.lower(),
                    "state": _c.StateCodes.ACTIVE.value
                }
            )