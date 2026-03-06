import json
from typing import List
from uuid import UUID
from collections import OrderedDict
from google.cloud import spanner
from google.api_core.exceptions import NotFound

import dynastore.modules.spanner.utils as _u
import dynastore.modules.spanner.constants as _c
import dynastore.modules.spanner.models.business.asset as _abm
import dynastore.modules.spanner.models.business.principal as _pbm
import dynastore.modules.spanner.models.business.permission as _prbm

from dynastore.modules.spanner.dml_tools import insert_and_return, update_and_return

async def init_permission_registry(manager):
    # 1. Define Permission Bits
    read_permission_bit_on_metadata_context = _prbm.GenericPermissionBits(
        name="read", bit_value=1
    )
    list_permission_bit_on_metadata_context = _prbm.GenericPermissionBits(
        name="list", bit_value=2
    )
    create_permission_bit_on_metadata_context = _prbm.GenericPermissionBits(
        name="create", bit_value=4
    )
    update_permission_bit_on_metadata_context = _prbm.GenericPermissionBits(
        name="update", bit_value=8
    )
    delete_permission_bit_on_metadata_context = _prbm.GenericPermissionBits(
        name="delete", bit_value=16
    )
    manage_acl_permission_bit_on_metadata_context = _prbm.GenericPermissionBits(
        name="manage_acl", bit_value=32
    )
    test_permission_bit_on_metadata_context = _prbm.GenericPermissionBits(
        name="test_bit", bit_value=64
    )
    upload_permission_on_metadata_context = _prbm.GenericPermissionBits(
        name="upload", bit_value=128
    )
    download_permission_on_metadata_context = _prbm.GenericPermissionBits(
        name="download", bit_value=256
    )

    read_query_permission_bit_on_test_context = _prbm.GenericPermissionBits(
        name="read_query", bit_value=1
    )
    execute_query_permission_bit_on_test_context = _prbm.GenericPermissionBits(
        name="execute_query", bit_value=2
    )

    # 2. Define Contexts
    ckan_metadata_context = _prbm.Context(
        name="ckan_metadata_context", # Fixed: Changed code to name
        description="Context for CKAN metadata",
        generic_permission_bits=OrderedDict([
            (read_permission_bit_on_metadata_context.bit_value, read_permission_bit_on_metadata_context),
            (list_permission_bit_on_metadata_context.bit_value, list_permission_bit_on_metadata_context),
            (create_permission_bit_on_metadata_context.bit_value, create_permission_bit_on_metadata_context),
            (update_permission_bit_on_metadata_context.bit_value, update_permission_bit_on_metadata_context),
            (delete_permission_bit_on_metadata_context.bit_value, delete_permission_bit_on_metadata_context),
        ])
    )

    test_context = _prbm.Context(
        name="test_context",
        description="Context for test different contexts",
        generic_permission_bits=OrderedDict([
            (read_query_permission_bit_on_test_context.bit_value, read_query_permission_bit_on_test_context),
            (execute_query_permission_bit_on_test_context.bit_value, execute_query_permission_bit_on_test_context)
        ])
    )

    # 3. Define Permissions (With Labels)
    can_view = _prbm.Permission(
        permission_bits=OrderedDict([
            (ckan_metadata_context.name, OrderedDict([
                (read_permission_bit_on_metadata_context.bit_value, read_permission_bit_on_metadata_context),
                (list_permission_bit_on_metadata_context.bit_value, list_permission_bit_on_metadata_context),
            ]))
        ]),
        definition="metadata.view",
        code="metadata.view",
        label='{"EN": "View Metadata", "ES": "Ver Metadatos"}',
        description="Read metadata",
    )

    create_metadata = _prbm.Permission(
        permission_bits=OrderedDict([
            (ckan_metadata_context.name, OrderedDict([
                (create_permission_bit_on_metadata_context.bit_value, create_permission_bit_on_metadata_context),
            ]))
        ]),
        definition="metadata.create",
        code="metadata.create",
        label='{"EN": "Create Metadata", "ES": "Crear Metadatos"}',
        description="Create metadata"
    )

    modify_metadata = _prbm.Permission(
        permission_bits=OrderedDict([
            (ckan_metadata_context.name, OrderedDict([
                (update_permission_bit_on_metadata_context.bit_value, update_permission_bit_on_metadata_context),
            ]))
        ]),
        definition="metadata.update",
        code="metadata.update",
        label='{"EN": "Update Metadata", "ES": "Actualizar Metadatos"}',
        description="Update metadata"
    )

    delete_metadata = _prbm.Permission(
        permission_bits=OrderedDict([
            (ckan_metadata_context.name, OrderedDict([
                (delete_permission_bit_on_metadata_context.bit_value, delete_permission_bit_on_metadata_context),
            ]))
        ]),
        definition="metadata.delete",
        code="metadata.delete",
        label='{"EN": "Delete Metadata", "ES": "Eliminar Metadatos"}',
        description="Delete metadata"
    )

    grant_permission = _prbm.Permission(
        permission_bits=OrderedDict([
            (ckan_metadata_context.name, OrderedDict([
                (manage_acl_permission_bit_on_metadata_context.bit_value, manage_acl_permission_bit_on_metadata_context),
            ]))
        ]),
        definition="permission.grant",
        code="permission.grant",
        label='{"EN": "Grant Permission", "ES": "Conceder Permiso"}',
        description="Grant permission"
    )

    test_permission = _prbm.Permission(
        permission_bits=OrderedDict([
            (ckan_metadata_context.name, OrderedDict([
                (test_permission_bit_on_metadata_context.bit_value, test_permission_bit_on_metadata_context),
            ]))
        ]),
        definition="permission.test",
        code="permission.test",
        label='{"EN": "Test Permission", "ES": "Permiso de Prueba"}',
        description="Test permission"
    )

    composite_test_permission = _prbm.Permission(
        permission_bits=OrderedDict([
            (ckan_metadata_context.name, OrderedDict([
                (test_permission_bit_on_metadata_context.bit_value, test_permission_bit_on_metadata_context),
                (delete_permission_bit_on_metadata_context.bit_value, delete_permission_bit_on_metadata_context),
                (list_permission_bit_on_metadata_context.bit_value, list_permission_bit_on_metadata_context),
            ])),
            (test_context.name, OrderedDict([
                (read_query_permission_bit_on_test_context.bit_value, read_query_permission_bit_on_test_context),
                (execute_query_permission_bit_on_test_context.bit_value, execute_query_permission_bit_on_test_context),
            ])),
        ]),
        definition="permission.test.composite",
        code="permission.test.composite",
        label='{"EN": "Composite Test Permission", "ES": "Permiso de Prueba Compuesto"}',
        description="Composite Test permission"
    )

    # 4. Define Roles (With Labels and Fixed Keys)
    admin_role = _prbm.Role(
        uuid=UUID("11111111-1111-1111-1111-0000000000ad"),
        code="admin",
        label='{"EN": "Admin", "ES": "Administrador"}',
        description="Admin role",
        permissions=OrderedDict([
            (can_view.code, can_view), 
            (create_metadata.code, create_metadata),
            (modify_metadata.code, modify_metadata),
            (delete_metadata.code, delete_metadata),
            (grant_permission.code, grant_permission)
        ])
    )

    metadata_producer_role = _prbm.Role(
        uuid=UUID("11111111-1111-1111-1111-0000000000dd"),
        code="metadata_producer",
        label='{"EN": "Metadata Producer", "ES": "Productor de Metadatos"}',
        description="Metadata producer role",
        permissions=OrderedDict([
            (can_view.code, can_view),
            (create_metadata.code, create_metadata),
            (modify_metadata.code, modify_metadata)
        ])
    )
    
    editor_role = _prbm.Role(
        uuid=UUID("11111111-1111-1111-1111-0000000000ed"),
        code="editor",
        label='{"EN": "Editor", "ES": "Editor"}',
        description="Editor role",
        permissions=OrderedDict([
            (test_permission.code, test_permission)
        ])
    )
    
    publisher_role = _prbm.Role(
        uuid=UUID("11111111-1111-1111-1111-0000000000bb"),
        code="publisher",
        label='{"EN": "Publisher", "ES": "Publicador"}',
        description="Publisher role",
        permissions=OrderedDict([
            (grant_permission.code, grant_permission)
        ])
    )

    member_role = _prbm.Role(
        uuid=UUID("11111111-1111-1111-1111-0000000000eb"),
        code="member",
        label='{"EN": "Member", "ES": "Miembro"}',
        description="Member role",
        permissions=OrderedDict([
            (can_view.code, can_view)
        ])
    )

    # 5. Define Conditions
    at_the_end_of_today_condition = _prbm.Condition(
        name="at_the_end_of_today",
        description="Condition that is true at the end of today",
        expression=f"request.time >= {datetime.date.today() + datetime.timedelta(days=1)}",
        state_code=_c.StateCodes.ACTIVE
    )

    can_view_update_condition = _prbm.Condition(
        name="can_view_update_condition",
        description="Condition for updating view permission",
        expression="request.change.permission == metadata.view",
    )

    # 6. Define Principals
    admin_principal1 = _pbm.Principal(
        uuid=UUID("11111111-1111-1111-1111-000000000ad1"),
        principal_id="user:admin_user1@something.com",
        display_name="admin_user1",
        type_code=_c.PrincipalTypes.USER
    )

    editor_principal1 = _pbm.Principal(
        uuid=UUID("11111111-1111-1111-1111-000000000ed1"),
        principal_id="user:editor_user1",
        display_name="editor_user1",
        type_code=_c.PrincipalTypes.USER
    )
    
    editor_principal2 = _pbm.Principal(
        uuid=UUID("11111111-1111-1111-1111-000000000ed2"),
        principal_id="user:editor_user2",
        display_name="editor_user2",
        type_code=_c.PrincipalTypes.USER   
    )
    
    publisher_principal1 = _pbm.Principal(
        uuid=UUID("11111111-1111-1111-1111-000000000bb1"),
        principal_id="user:publisher_user1",
        display_name="publisher_user1",
        type_code=_c.PrincipalTypes.USER   
    )
    
    publisher_principal2 = _pbm.Principal(
        uuid=UUID("11111111-1111-1111-1111-000000000bb2"),
        principal_id="user:publisher_user2",
        display_name="publisher_user2",
        type_code=_c.PrincipalTypes.USER
    )
    
    member_principal1 = _pbm.Principal(
        uuid=UUID("11111111-1111-1111-1111-000000000eb1"),
        principal_id="user:member_user1",
        display_name="member_user1",
        type_code=_c.PrincipalTypes.USER
    )
    
    test_principal1 = _pbm.Principal(
        uuid=UUID("11111111-1111-1111-1111-000000000009"),
        principal_id="user:test_user1",
        display_name="test_user1",
        type_code=_c.PrincipalTypes.USER
    )
    # 7. Define Groups (Fixed Children Keys)
    publishers_group = _prbm.GroupPrincipal(
        uuid=UUID("11111111-1111-1111-1111-00000000bbb1"),
        principal_id="group:publishers",
        display_name="publishers",
        children=OrderedDict([
            (publisher_principal1.display_name, publisher_principal1),
            (publisher_principal2.display_name, publisher_principal2)
        ])
    )
    
    editors_group = _prbm.GroupPrincipal(
        uuid=UUID("11111111-1111-1111-1111-00000000edb1"),
        principal_id="group:editors",
        display_name="editors",
        children=OrderedDict([
            (editor_principal1.display_name, editor_principal1),
            (editor_principal2.display_name, editor_principal2)
        ])
    )

    metadata_producers_group = _prbm.GroupPrincipal(
        uuid=UUID("11111111-1111-1111-1111-00000000abb1"),
        principal_id="group:metadata_producers",
        display_name="metadata_producers",
        children=OrderedDict([
            (editors_group.display_name, editors_group),
            (publishers_group.display_name, publishers_group),
            (admin_principal1.display_name, admin_principal1)
        ])
    )

    # 8. Define Policies (Using .code for permission keys)
    assets = get_assets(manager)
    some_test_asset = await assets.__anext__()
    some_test_asset2 = await assets.__anext__()
    
    demo_policy = _prbm.Policy(
        name="demo_policy",
        description="A demo policy for testing",
        bindings=[
            _prbm.PolicyBinding(
                effect=_c.PermissionActionTypes.ALLOW,
                principals=OrderedDict([
                    (admin_principal1.uuid, admin_principal1)
                ]),
                permissions=OrderedDict([
                    (admin_role.code, admin_role)
                ]),
                asset=some_test_asset,
                context=ckan_metadata_context
            ),
            _prbm.PolicyBinding(
                effect=_c.PermissionActionTypes.ALLOW,
                principals=OrderedDict([
                    (editor_principal1.uuid, editor_principal1),
                    (editor_principal2.uuid, editor_principal2)
                ]),
                permissions=OrderedDict([
                    (editor_role.code, editor_role)
                ]),
                asset=some_test_asset,
                context=ckan_metadata_context,
            ),
            _prbm.PolicyBinding(
                effect=_c.PermissionActionTypes.DENY,
                principals=OrderedDict([
                    (editor_principal1.uuid, editor_principal1),
                ]),
                permissions=member_role.get_permissions(),
                asset=some_test_asset,
                context=ckan_metadata_context,
            ),
            _prbm.PolicyBinding(
                effect=_c.PermissionActionTypes.ALLOW,
                principals=OrderedDict([
                    (publishers_group.uuid, publishers_group)
                ]),
                permissions=OrderedDict([
                    (publisher_role.code, publisher_role)
                ]),
                asset=some_test_asset,
                context=ckan_metadata_context,
                condition=can_view_update_condition
            ),
            _prbm.PolicyBinding(
                effect=_c.PermissionActionTypes.ALLOW,
                principals=OrderedDict([
                    (metadata_producers_group.uuid, metadata_producers_group)
                ]),
                permissions=OrderedDict([
                    (metadata_producer_role.code, metadata_producer_role)
                ]),                    
                asset=some_test_asset,
                context=ckan_metadata_context,
                condition=None
            ),
            _prbm.PolicyBinding(
                effect=_c.PermissionActionTypes.ALLOW,
                principals=OrderedDict([
                    (member_principal1.uuid, member_principal1)
                ]),
                permissions=OrderedDict([
                    (member_role.code, member_role)
                ]),                    
                asset=some_test_asset,
                context=ckan_metadata_context,
                condition=at_the_end_of_today_condition
            )
        ]
    )
    
    demo_policy2 = _prbm.Policy(
        name="demo_policy2",
        description="Second demo policy for testing",
        bindings=[
            _prbm.PolicyBinding(
                effect=_c.PermissionActionTypes.ALLOW,
                principals=OrderedDict([
                    (admin_principal1.uuid, admin_principal1)
                ]),
                permissions=OrderedDict([
                    (admin_role.code, admin_role),
                ]),
                asset=some_test_asset2,
                context=ckan_metadata_context
            ),
            _prbm.PolicyBinding(
                effect=_c.PermissionActionTypes.DENY,
                principals=OrderedDict([
                    (admin_principal1.uuid, admin_principal1)
                ]),
                permissions=OrderedDict([
                    (grant_permission.code, grant_permission)
                ]),
                asset=some_test_asset2,
                context=ckan_metadata_context,
                condition=at_the_end_of_today_condition
            ),
            _prbm.PolicyBinding(
                effect=_c.PermissionActionTypes.ALLOW,
                principals=OrderedDict([
                    (editor_principal1.uuid, editor_principal1),
                    (editor_principal2.uuid, editor_principal2)
                ]),
                permissions=OrderedDict([
                    (editor_role.code, editor_role),
                    (test_permission.code, test_permission)
                ]),
                asset=some_test_asset2,
                context=ckan_metadata_context,
            ),
            _prbm.PolicyBinding(
                effect=_c.PermissionActionTypes.ALLOW,
                principals=OrderedDict([
                    (member_principal1.uuid, member_principal1)
                ]),
                permissions=OrderedDict([
                    (composite_test_permission.code, composite_test_permission)
                ]),
                asset=some_test_asset2,
                context=ckan_metadata_context
            ),
            _prbm.PolicyBinding(
                effect=_c.PermissionActionTypes.DENY,
                principals=OrderedDict([
                    (member_principal1.uuid, member_principal1)
                ]),
                permissions=OrderedDict([
                    (composite_test_permission.code, composite_test_permission)
                ]),
                asset=some_test_asset2,
                context=test_context
            ),
            _prbm.PolicyBinding(
                effect=_c.PermissionActionTypes.DENY,
                principals=OrderedDict([
                    (test_principal1.uuid, test_principal1)
                ]),
                permissions=OrderedDict([
                    (test_permission.code, test_permission)
                ]),
                asset=some_test_asset2,
                context=test_context
            )
        ]
    )
    await create_policy(manager, demo_policy) # CHECK THEY DO NOT FAIL IN SECOND CALL
    await create_policy(manager, demo_policy2)
    # await get_policy_by_principal_and_asset(
    #          principal = publisher_principal1,
    #          asset = some_test_asset,
    #          context = ckan_metadata_context
    # )
    policies1 = await get_policies_by_principal_and_assets(
        app_state=manager,
        principal= member_principal1,
        assets= [some_test_asset, some_test_asset2],
        context=ckan_metadata_context
    )        
    policies2 = await get_policies_by_principal_and_assets(
        app_state=manager,
        principal= admin_principal1,
        assets= [some_test_asset2],
        context=ckan_metadata_context
    )
    await get_permission_definition(manager=manager,permission=grant_permission, context=ckan_metadata_context)
    await check_permission_on_bitmask(manager=manager,permission=can_view, bit_mask=7, context=ckan_metadata_context)
    await get_permissions_by_role(manager=manager,role=admin_role, with_bits=True, context=ckan_metadata_context)
    await get_permissions_by_role(manager=manager,role=member_role, with_bits=False, context=None)
    await get_permission_bits_by_bitmask(manager=manager,bit_mask=71, context=ckan_metadata_context)
    await get_permission_codes_by_bitmask(manager=manager,bit_mask=71, context=ckan_metadata_context)

async def create_policy(manager, policy: _prbm.Policy):
    # This function is a mix of many things, we can use different parts in different functions
    async with manager.transaction() as tx:
        for binding in policy.get_bindings():
            # First create all contexts, they could be pointed by other permissions
            context = binding.get_context()
            binding.set_context(await create_or_get_context(manager, context, tx))

        for binding in policy.get_bindings():
            # we have to assume asset, principal already exists
            # it is necessary to create entries in asset_principal_permission, asset_principal_link (if it does not exist), permission_context
            # Ensure context exists (by name) or insert it
            asset = binding.get_asset()
            asset_uuid_spanner = _u.convert_into_spanner_uuid(asset.get_uuid())
            context = binding.get_context()
            condition = binding.get_condition()
            condition_id = None
            if condition:
                try:
                    q = f"SELECT id FROM {_c.CONDITION_TABLE_NAME} WHERE name = @name"
                    res = tx.execute_sql(q, params={"name": condition.name})
                    condition_id = res.one()[0]
                except NotFound:
                    cond_dict = {
                        "name": condition.name,
                        "description": condition.description if getattr(condition, "description", None) is not None else None,
                        "expression": json.dumps(condition.expression),
                        "state": condition.state_code.value if getattr(condition, "state_code", None) is not None else _c.StateCodes.ACTIVE.value
                    }
                    condition_id = insert_and_return(
                        transaction=tx,
                        table_name=_c.CONDITION_TABLE_NAME,
                        data_to_insert=cond_dict,
                        returning_columns=["id"],
                        explicit_param_types={"expression": spanner.param_types.JSON} # to avoid JSON being stored as BYTES
                    ).pop()
            for principal_uuid, principal in binding.get_principals().items():
                principal_uuid_spanner = _u.convert_into_spanner_uuid(principal.get_uuid())
                try:
                    query = f"SELECT uuid FROM {_c.PRINCIPAL_TABLE_NAME} WHERE uuid = @uuid"
                    res = tx.execute_sql(query, params={"uuid": principal_uuid_spanner})
                    res.one()
                    await create_principal_hierarchy(manager, principal, tx)
                except NotFound:
                    await create_principal(manager, principal, tx)
                # find or create asset_principal_link
                try:
                    q = f"SELECT id FROM {_c.ASSET_PRINCIPAL_LINK_TABLE_NAME} WHERE asset_uuid = @asset_uuid AND principal_uuid = @principal_uuid"
                    res = tx.execute_sql(q, params={"asset_uuid": asset_uuid_spanner, "principal_uuid": principal_uuid_spanner})
                    asset_principal_link_id = res.one()[0]
                except NotFound:
                    asset_principal_link_id = insert_and_return(
                        transaction=tx,
                        table_name=_c.ASSET_PRINCIPAL_LINK_TABLE_NAME,
                        data_to_insert={
                            "asset_uuid": asset_uuid_spanner,
                            "principal_uuid": principal_uuid_spanner
                        },
                        returning_columns=["id"]
                    ).pop()
                for permission_obj in binding.get_permissions().values():
                    # find or create permission row (use name as unique key)
                    permission_id = await create_or_get_permission(manager, permission_obj, tx)
                    try:
                        q = f"SELECT id FROM {_c.ASSET_PRINCIPAL_PERMISSION_TABLE_NAME} WHERE asset_principal_link_id = @link_id AND permission_id = @permission_id"
                        res = tx.execute_sql(q, params={"link_id": asset_principal_link_id, "permission_id": permission_id})
                        res.one()
                    except NotFound:
                        # use binding getter for effect if available

                        action_type = binding.get_effect().name
                        insert_and_return(
                            transaction=tx,
                            table_name=_c.ASSET_PRINCIPAL_PERMISSION_TABLE_NAME,
                            data_to_insert={
                                "asset_principal_link_id": asset_principal_link_id,
                                "permission_id": permission_id,
                                "action_type": action_type,
                                "condition_id": condition_id or None
                            },
                            explicit_param_types={
                                "condition_id": spanner.param_types.INT64
                                },
                            returning_columns=["id"]
                        )

async def create_or_get_context(manager, context: _prbm.Context, tx) -> _prbm.Context:
    context_id = context.get_id()
    if not context_id:
        try:
            q = f"SELECT id FROM {_c.CONTEXT_TABLE_NAME} WHERE name = @name"
            res = tx.execute_sql(q, params={"name": context.name})
            context_id = res.one()[0]
        except NotFound:
            #raise Exception(f"Context {context.name} not found")
            context_id = insert_and_return(
                transaction=tx,
                table_name=_c.CONTEXT_TABLE_NAME,
                data_to_insert={"name": context.name},
                returning_columns=["id"]
            ).pop()
            for bit in context.get_generic_permission_bits().values():
                insert_and_return(
                    transaction=tx,
                    table_name=_c.GENERIC_PERMISSION_BIT_TABLE_NAME,
                    data_to_insert={
                        "context_id": context_id,
                        "bit_value": bit.get_bit_value(),
                        "name": bit.name,
                        "state": _c.StateCodes.ACTIVE.value
                    },
                    returning_columns=["id"],
                    explicit_param_types={"bit_value": spanner.param_types.INT64}
                )
    context.set_id(context_id)
    return context

async def create_or_get_permission(manager, permission_obj: _prbm.AbstractPermission, tx) -> int:
    """
    Ensure permission (or role) exists in DB and return its id.
    - Creates permission row if missing.
    - For ROLE: ensures child permissions exist and creates permission_edge rows.
    - Ensures generic_permission_bit rows exist and creates permission_context rows (if context_id provided).
    """
    # --- find existing permission by code ---

        # safe definition retrieval (Role may not implement)
    try:
        q = f"SELECT id FROM {_c.PERMISSION_TABLE_NAME} WHERE code = @code"
        res = tx.execute_sql(q, params={"code": permission_obj.get_code()})
        permission_id = res.one()[0]
    except NotFound:
        # safe definition retrieval (Role may not implement)
        try:
            definition_val = permission_obj.get_definition()
        except Exception:
            definition_val = None

        perm_dict = {
            "code": permission_obj.get_code(),
            "label": permission_obj.get_label(),
            "description": permission_obj.get_description(),
            "permission_type": permission_obj.get_type_code().name,
            "definition": json.dumps(definition_val) if definition_val is not None else None,
            "state": _c.StateCodes.ACTIVE.value
        }

        permission_id = insert_and_return(
            transaction=tx,
            table_name=_c.PERMISSION_TABLE_NAME,
            data_to_insert=perm_dict,
            returning_columns=["id"],
            explicit_param_types= {"definition": spanner.param_types.JSON, "label": spanner.param_types.STRING}
        ).pop()

    # --- if ROLE: ensure children permissions and permission_edge entries ---

    if permission_obj.get_type_code() == _c.PermissionTypeCodes.ROLE:
        children = permission_obj.get_permissions()
        for child in children.values():
            # ensure child permission exists (recursive)
            child_permission_id = await create_or_get_permission(manager, child, tx)
            # ensure edge parent -> child exists
            try:
                q = f"SELECT id FROM {_c.PERMISSION_EDGE_TABLE_NAME} WHERE parent_permission_id = @p AND child_permission_id = @c"
                tx.execute_sql(q, params={"p": permission_id, "c": child_permission_id}).one()
            except NotFound:
                insert_and_return(
                    transaction=tx,
                    table_name=_c.PERMISSION_EDGE_TABLE_NAME,
                    data_to_insert={
                        "parent_permission_id": permission_id,
                        "child_permission_id": child_permission_id,
                        "state": _c.StateCodes.ACTIVE.value
                    },
                    returning_columns=["id"]
                )
    elif permission_obj.get_type_code() == _c.PermissionTypeCodes.PERMISSION:
        for context_name, permission_bits_dict in permission_obj.get_permission_bits().items(): 
            for bit_value, bit_obj in permission_bits_dict.items():
                get_context_id_query = f"SELECT id FROM {_c.CONTEXT_TABLE_NAME} WHERE name = @context_name"
                get_context_id_res = tx.execute_sql(get_context_id_query, params={"context_name": context_name})
                context_id = get_context_id_res.one()[0]
                try:
                    q = f"SELECT gpm.id FROM {_c.GENERIC_PERMISSION_BIT_TABLE_NAME} gpm WHERE gpm.bit_value = @bit_value and gpm.context_id = @context_id"
                    res = tx.execute_sql(q, params={"bit_value": bit_value, "context_id": context_id})
                    generic_bit_id = res.one()[0]
                except NotFound:
                    generic_bit_id = insert_and_return(
                        transaction=tx,
                        table_name=_c.GENERIC_PERMISSION_BIT_TABLE_NAME,
                        data_to_insert={"name": (bit_obj.get_name() if hasattr(bit_obj, "get_name") else str(bit_value)), "bit_value": bit_value, "context_id": context_id, "state":bit_obj.get_state().value},
                        returning_columns=["id"]
                    ).pop()
                bit_obj.set_id(generic_bit_id)
                try:
                    q = f"SELECT 1 FROM {_c.PERMISSION_BIT_MAP_TABLE_NAME} WHERE permission_id = @permission_id AND generic_permission_bit_id = @bit_id"
                    tx.execute_sql(q, params={"permission_id": permission_id, "bit_id": generic_bit_id}).one()
                except NotFound:
                    insert_and_return(
                        transaction=tx,
                        table_name=_c.PERMISSION_BIT_MAP_TABLE_NAME,
                        data_to_insert={
                            "permission_id": permission_id,
                            "generic_permission_bit_id": generic_bit_id
                        }
                    )

    return permission_id


async def get_policies_by_principal_and_assets(manager, principal: _pbm.Principal, assets: List[_abm.Asset], context: _prbm.Context) -> OrderedDict[str, _prbm.Policy]:
        """
        Build and return an OrderedDict mapping Asset UUIDs to Policy objects.
        Returns one row per asset from DB, containing all nested bindings.
        """
        if principal is None or not assets:
            raise ValueError("principal and a non-empty list of assets must be provided")

        principal_uuid_param = "principal_uuid"
        asset_uuids_param = "asset_uuids"

        sql = f"""
            WITH
            -- 1. Get the Principal (User + Groups)
            UserAndGroups AS (
                SELECT DISTINCT
                gt.group_uuid AS principal_uuid
                FROM
                principal AS U
                JOIN GRAPH_TABLE (
                    principal_graph
                    MATCH (G:principal)-[:has_member]->{{0,100}}(U_node:principal)
                    COLUMNS (
                    U_node.uuid AS user_uuid,
                    G.uuid AS group_uuid,
                    G.state AS group_state
                    )
                ) AS gt ON U.uuid = gt.user_uuid
                WHERE
                U.uuid = @{principal_uuid_param}
                AND U.state = {_c.StateCodes.ACTIVE.value}
                AND gt.group_state = {_c.StateCodes.ACTIVE.value}
            ),

            -- 2. Find all raw permission links for the list of assets
            DirectlyAssignedPermissions AS (
                SELECT DISTINCT
                apl.asset_uuid,
                app.permission_id,
                app.action_type,
                app.condition_id
                FROM
                UserAndGroups AS uag
                JOIN asset_principal_link AS apl ON uag.principal_uuid = apl.principal_uuid
                JOIN asset_principal_permission AS app ON apl.id = app.asset_principal_link_id
                WHERE
                apl.asset_uuid IN UNNEST(@{asset_uuids_param})
            ),

            -- 3. Expand permissions (Graph Traversal)
            ExpandedPermissionIDs AS (
                SELECT
                dap.asset_uuid,
                dap.action_type,
                dap.condition_id,
                gt.sub_perm_id AS permission_id,
                TO_JSON(nodes) as nodes
                FROM
                DirectlyAssignedPermissions AS dap
                JOIN permission AS TopPerm ON dap.permission_id = TopPerm.id
                JOIN (
                    SELECT
                    top_perm_id,
                    sub_perm_id,
                    sub_perm_state,
                    nodes
                    FROM
                    GRAPH_TABLE (
                        permission_graph
                        MATCH res = (TopPerm_node:permission)-[e:includes]->{{0,100}}(SubPerm:permission)
                        COLUMNS (
                        TopPerm_node.id AS top_perm_id,
                        SubPerm.id AS sub_perm_id,
                        SubPerm.state AS sub_perm_state,
                        nodes(res) as nodes
                        )
                    )
                ) AS gt ON TopPerm.id = gt.top_perm_id
                WHERE
                TopPerm.state = {_c.StateCodes.ACTIVE.value}
                AND gt.sub_perm_state = {_c.StateCodes.ACTIVE.value}
            ),

            -- 4. Level 1 Aggregation: Group by Asset + Binding Context (Create the PolicyBinding data)
            BindingsPerAsset AS (
                SELECT
                    epi.asset_uuid,
                    gp.context_id,
                    epi.action_type,
                    ANY_VALUE(cond.name) AS condition_name,
                    ANY_VALUE(cond.expression) AS condition_expression,
                    ANY_VALUE(cond.description) AS condition_description,
                    BIT_OR(gp.bit_value) AS effective_bit_mask,
                    -- Aggregate individual permissions for this specific binding
                    ARRAY_AGG(
                        STRUCT(
                        p.id AS permission_id,
                        p.code AS permission_code,
                        p.label AS permission_label,
                        gp.bit_value AS individual_bit_value,
                        epi.nodes AS path_nodes
                        )
                    ) AS assigned_permissions
                FROM
                    ExpandedPermissionIDs AS epi
                    JOIN permission AS p ON epi.permission_id = p.id
                    JOIN permission_bit_map AS pbm ON pbm.permission_id = p.id
                    JOIN context AS cx ON cx.name = '{context.get_name()}'
                    JOIN generic_permission_bit AS gp ON pbm.generic_permission_bit_id = gp.id and gp.context_id = cx.id
                    LEFT JOIN condition AS cond ON epi.condition_id = cond.id
                WHERE
                    p.state = {_c.StateCodes.ACTIVE.value}
                GROUP BY
                    epi.asset_uuid,
                    gp.context_id,
                    epi.action_type,
                    epi.condition_id
            )

            -- 5. Level 2 Aggregation: Group by Asset ONLY (Create the Policy object data)
            SELECT
                bpa.asset_uuid,
                -- Aggregate all Bindings into a list of STRUCTs for this asset
                ARRAY_AGG(
                    STRUCT(
                        bpa.context_id,
                        bpa.action_type,
                        bpa.condition_name,
                        bpa.condition_expression,
                        bpa.condition_description,
                        bpa.effective_bit_mask,
                        bpa.assigned_permissions
                    )
                ) AS policy_bindings
            FROM
                BindingsPerAsset AS bpa
            GROUP BY
                bpa.asset_uuid
        """

        spanner_asset_uuid_list = [_u.convert_into_spanner_uuid(a.get_uuid()) for a in assets]
        principal_bytes = _u.convert_into_spanner_uuid(principal.get_uuid())

        params = {
            principal_uuid_param: principal_bytes, 
            asset_uuids_param: spanner_asset_uuid_list
        }
        

        final_policies = OrderedDict()

        async with manager.get_session() as session:
            results = session.execute_sql(sql, params=params)

            for row in results:
                # 1 Row = 1 Asset
                spanner_asset_uuid, per_asset_results = row
                
                asset_uuid = _u.convert_from_spanner_uuid(spanner_asset_uuid)
                
                # Create a minimal asset object to satisfy the PolicyBinding contract
                current_asset = _abm.Asset(uuid=asset_uuid) 

                bindings_list = []
                
                for per_asset_result in per_asset_results:
                    context_id, action_type, condition_name, condition_expression, condition_description, bit_value, path = per_asset_result

                    condition_obj = None
                    
                    bindings_list.append(
                        _prbm.PolicyBinding(
                            effect=_c.PermissionActionTypes[action_type],
                            principals=OrderedDict([
                                (principal.get_uuid(), principal)
                            ]),
                            bit_mask = bit_value,
                            asset=current_asset,
                            context=context,
                            condition=_prbm.Condition(
                                name=condition_name,
                                description=condition_description,
                                expression=_u.convert_json_object(condition_expression)
                            ) if condition_name else None
                        )
                    )

                final_policies[asset_uuid] = _prbm.Policy(
                    bindings=bindings_list
                )

        return final_policies

async def get_permission_definition(manager, permission: _prbm.Permission, context: _prbm.Context) -> List[_prbm.GenericPermissionBits]:
    permission_code_param = "permission_code"
    context_name_param = "context_name"

    sql = f"""
        SELECT
            gpb.bit_value,
            gpb.name
        FROM
            permission AS p
            JOIN permission_bit_map AS pbm ON p.id = pbm.permission_id
            JOIN generic_permission_bit AS gpb ON pbm.generic_permission_bit_id = gpb.id
            JOIN context AS c ON gpb.context_id = c.id and gpb.context_id = c.id
        WHERE
            p.code = @{permission_code_param}
            AND c.name = @{context_name_param}
            AND p.state = {_c.StateCodes.ACTIVE.value}
    """

    params = {
        permission_code_param: permission.get_code(),
        context_name_param: context.get_name()
    }
    param_types = {
        permission_code_param: spanner.param_types.STRING,
        context_name_param: spanner.param_types.STRING
    }

    generic_permission_bits = []
    async with manager.get_session() as session:
        results = session.execute_sql(sql, params=params, param_types=param_types)

        for row in results:
            bit_value, name = row
            generic_permission_bits.append(
                _prbm.GenericPermissionBits(
                    bit_value=bit_value,
                    name=name
                )
            )
    return generic_permission_bits
    
async def check_permission_on_bitmask(manager, permission: _prbm.Permission, bit_mask: int, context: _prbm.Context) -> bool:
    permission_code_param = "permission_code"
    context_name_param = "context_name"
    bit_mask_param = "bit_mask"

    sql = f"""
        SELECT
            BIT_AND(gpb.bit_value & @{bit_mask_param}) AS matched_bits
        FROM
            permission AS p
            JOIN permission_bit_map AS pbm ON p.id = pbm.permission_id
            JOIN generic_permission_bit gpb ON pbm.permission_id = gpb.id
            JOIN context AS c ON gpb.context_id = c.id
        WHERE
            p.code = @{permission_code_param}
            AND c.name = @{context_name_param}
            AND p.state = {_c.StateCodes.ACTIVE.value}
        GROUP BY
            p.id
    """

    params = {
        permission_code_param: permission.get_code(),
        context_name_param: context.get_name(),
        bit_mask_param: bit_mask
    }
    param_types = {
        permission_code_param: spanner.param_types.STRING,
        context_name_param: spanner.param_types.STRING,
        bit_mask_param: spanner.param_types.INT64
    }

    async with manager.get_session() as session:
        results = session.execute_sql(sql, params=params, param_types=param_types)

        undefined = True
        for row in results:
            matched_bits, = row
            undefined = False
            if matched_bits == 0:
                return False
        return not undefined

async def get_permissions_by_role(manager, role: _prbm.Role, with_bits:bool=False, context:_prbm.Context=None) -> OrderedDict[str,_prbm.Permission]:

    """
    Return all concrete Permission objects that are reachable from the given role
    by traversing the permission_graph (role -> includes -> ... -> permission).
    Permissions are returned with their context and generic permission bits.
    """
    if role is None:
        return []
    
    if with_bits and not context:
        raise Exception("Context must be provided when bitmask is requested")
    
    # TODO if context is not provided it will not work properly nowe
    context_name = context.get_name() if context else ""
    role_code_param = "role_code"

    sql = f"""
                WITH
                RoleNode AS (
                    SELECT id
                    FROM {_c.PERMISSION_TABLE_NAME}
                    WHERE code = @{role_code_param}
                    AND state = {_c.StateCodes.ACTIVE.value}
                ),
                GT AS (
                SELECT
                    top_perm_id,
                    sub_perm_id,
                    sub_perm_state
                FROM
                    GRAPH_TABLE (
                    permission_graph
                    MATCH res = (TopPerm_node:permission)-[e:includes]->{{0,100}}(SubPerm:permission)
                    COLUMNS (
                        TopPerm_node.id AS top_perm_id,
                        SubPerm.id AS sub_perm_id,
                        SubPerm.state AS sub_perm_state
                    )
                    )
                )
                SELECT
                    c.id AS context_id,
                    ANY_VALUE(c.name) AS context_name,
                    p.id AS permission_id,
                    ANY_VALUE(p.code) AS permission_code,
                    ANY_VALUE(p.label) AS permission_label,
                    ANY_VALUE(p.description) AS permission_description,
                ARRAY(
                    SELECT AS STRUCT
                        gpb.id AS bit_id,
                        gpb.name AS bit_name,
                        gpb.bit_value AS bit_value
                    FROM 
                        permission_bit_map pbm, generic_permission_bit gpb
                    WHERE 
                        -- correlated subquery using outer 'p' and 'c'
                        pbm.permission_id = p.id
                        AND pbm.generic_permission_bit_id = gpb.id
                        AND gpb.context_id = c.id
                ) AS bits_info


                FROM RoleNode rn
                    JOIN GT gt ON rn.id = gt.top_perm_id
                    JOIN permission p ON gt.sub_perm_id = p.id
                    -- MOVED CONTEXT JOIN HERE:
                    LEFT JOIN context c ON c.name = '{context_name}'
                WHERE
                    p.state = {_c.StateCodes.ACTIVE.value}
                    AND gt.sub_perm_state = {_c.StateCodes.ACTIVE.value}
                    AND p.permission_type = '{_c.PermissionTypeCodes.PERMISSION.name}'
                GROUP BY
                    p.id,
                    c.id
            """

    params = {role_code_param: role.get_code()}
    param_types = {role_code_param: spanner.param_types.STRING}


    async with manager.get_session() as session:
        results = session.execute_sql(sql, params=params, param_types=param_types)
        permissions = OrderedDict()
        for row in results:
            (
                context_id,
                context_name,
                permission_id,
                permission_code,
                permission_name,
                permission_description,
                bits_info
            ) = row

            generic_permission_bits_by_context = OrderedDict()
            if with_bits:
                context=_prbm.Context(
                    id=context_id,
                    name=context_name
                )
                for bit_row in bits_info:
                    bit_id, bit_name, bit_value = bit_row
                    generic_permission_bits_by_context[bit_value] = _prbm.GenericPermissionBits(
                        id=bit_id,
                        name=bit_name,
                        bit_value=bit_value
                    )
                        
                        
            permission = _prbm.Permission(
                id=permission_id,
                code=permission_code,
                name=permission_name,
                description=permission_description
            )
            if with_bits:
                permission.set_permission_bits(generic_permission_bits_by_context, context)
            permissions[permission_code] = permission

    return permissions

async def get_permission_bits_by_bitmask(manager, bit_mask: int, context: _prbm.Context) -> OrderedDict[int, _prbm.GenericPermissionBits]:
    bit_mask_param = "bit_mask"
    context_name_param = "context_name"
    sql = f"""
        SELECT
            gpb.bit_value,
            gpb.name
        FROM
            generic_permission_bit AS gpb,
            context as c
        WHERE
            (gpb.bit_value & @{bit_mask_param}) != 0
            and c.name = @{context_name_param}
    """

    params = {
        bit_mask_param: bit_mask,
        context_name_param: context.get_name()
    }


    generic_permission_bits = OrderedDict()
    async with manager.get_session() as session:
        results = session.execute_sql(sql, params=params)

        for row in results:
            bit_value, name = row
            generic_permission_bits[bit_value] = _prbm.GenericPermissionBits(
                    bit_value=bit_value,
                    name=name
                )
    return generic_permission_bits

async def get_permission_codes_by_bitmask(manager, bit_mask: int, context: _prbm.Context) -> OrderedDict[str, _prbm.Permission]:
    bit_mask_param = "bit_mask"
    context_name_param = "context_name"
    sql = f"""
        SELECT
            p.id,
            p.code,
            p.label,
            p.description
        FROM
            permission AS p
            JOIN permission_bit_map AS pbm ON p.id = pbm.permission_id
            JOIN generic_permission_bit gpb ON pbm.generic_permission_bit_id = gpb.id
            JOIN context AS c ON gpb.context_id = c.id
        WHERE
            (gpb.bit_value & @{bit_mask_param}) != 0
            AND c.name = @{context_name_param}
            AND p.state = {_c.StateCodes.ACTIVE.value}
    """

    params = {
        bit_mask_param: bit_mask,
        context_name_param: context.get_name()
    }

    permissions = OrderedDict()
    async with manager.get_session() as session:
        results = session.execute_sql(sql, params=params)

        for row in results:
            permission_id, permission_code, permission_label, permission_description = row
            permission = _prbm.Permission(
                id=permission_id,
                code=permission_code,
                name=permission_label,
                description=permission_description
            )
            permissions[permission_code] = permission
    return permissions
