import os
import json
import uuid
import dynastore.modules.spanner.constants as _c
import dynastore.extensions.datamind.legacy.constants as _lc
from dynastore.extensions.datamind.legacy import mapping as _lm
import dynastore.modules.spanner.datamind as datamind
import dynastore.modules.spanner.utils as _u
from dynastore.modules.spanner.models.business import asset as _abm
from dynastore.extensions.datamind.datamind import DatamindService
from dynastore.modules.spanner.spanner_module import SpannerModule
from dynastore.modules.spanner.session_manager import SpannerSessionManager
from google.api_core.exceptions import AlreadyExists, NotFound

import logging

logger = logging.getLogger(__name__)


async def initialize(extension: DatamindService, manager: SpannerSessionManager):
    extension.app.state.base_workflow_code_map = (
        await create_base_workflows_if_not_exists(manager)
    )
    await create_licenses_if_not_exists(extension, manager)


async def create_licenses_if_not_exists(
    extension: DatamindService, manager: SpannerSessionManager
):
    # TODO maybe we want to dynamically load from ckan

    licenses_path = os.path.join(_lc.legacy_static_file_path, "licenses.json")
    with open(licenses_path, "r") as f:
        licenses = json.load(f)

    # Get the license config

    license_config = await create_or_get_license_config(manager)

    for license_id, license_body in licenses.items():
        existing_license = await datamind.get_asset_by_property(
            manager,
            _c.LICENSE_ID_PROPERTY_CODE,
            license_id,
            asset_type=_c.AssetTypeCodes.LICENSE,
        )
        if existing_license:
            continue
        license_asset = _lm.map_license(license_body, license_config.get_uuid())
        await datamind.create_asset(manager=manager, asset=license_asset)


async def create_base_workflows_if_not_exists(manager):
    simple_workflow_name = "simple_workflow"
    simple_workflow = _abm.AssetWorkflowType(
        name=simple_workflow_name,
        description="Default workflow type",
        principal=datamind.system_principal,
        next_state_edges=[
            _abm.Relation(
                source=_c.AssetWorkflowStateCodes.CREATED,
                target=_c.AssetWorkflowStateCodes.ABORTED,
                attributes=_u.get_workflow_initial_state_attribute(),
            )
        ],
    )
    connection_workflow_name = "connection_workflow"
    connection_workflow = _abm.AssetWorkflowType(
        name=connection_workflow_name,
        description="Asset Connection Workflow type",
        principal=datamind.system_principal,
        next_state_edges=[
            _abm.Relation(
                source=_c.AssetWorkflowStateCodes.CREATED,
                target=_c.AssetWorkflowStateCodes.DRAFT,
                attributes=_u.get_workflow_initial_state_attribute(),
            ),
            _abm.Relation(
                source=_c.AssetWorkflowStateCodes.CREATED,
                target=_c.AssetWorkflowStateCodes.ABORTED,
            ),
            _abm.Relation(
                source=_c.AssetWorkflowStateCodes.DRAFT,
                target=_c.AssetWorkflowStateCodes.PUBLISHED,
            ),
        ],
    )
    upload_workflow_name = "upload_workflow"
    upload_workflow = _abm.AssetWorkflowType(
        name=upload_workflow_name,
        description="Asset Upload Workflow type",
        principal=datamind.system_principal,
        next_state_edges=[
            _abm.Relation(
                source=_c.AssetWorkflowStateCodes.UPLOADING,
                target=_c.AssetWorkflowStateCodes.CREATED,
                attributes=_u.get_workflow_initial_state_attribute(),
            )
        ],
    )

    metadata_iso_workflow_name = "iso_metadata_workflow"
    metadata_iso_workflow = _abm.AssetWorkflowType(
        name=metadata_iso_workflow_name,
        description="Metadata Iso Workflow type",
        principal=datamind.system_principal,
        next_state_edges=[
            _abm.Relation(
                source=_c.AssetWorkflowStateCodes.CREATED,
                target=_c.AssetWorkflowStateCodes.DRAFT,
                attributes=_u.get_workflow_initial_state_attribute(),
            ),
            _abm.Relation(
                source=_c.AssetWorkflowStateCodes.DRAFT,
                target=_c.AssetWorkflowStateCodes.WIP,
            ),
            _abm.Relation(
                source=_c.AssetWorkflowStateCodes.WIP,
                target=_c.AssetWorkflowStateCodes.PUBLISHED,
            ),
            _abm.Relation(
                source=_c.AssetWorkflowStateCodes.WIP,
                target=_c.AssetWorkflowStateCodes.ABORTED,
            ),
        ],
    )
    workflow_code_map = {}
    try:
        workflow_code_map.update(
            {
                simple_workflow_name: await datamind.get_workflow_type_by_name(
                    manager, simple_workflow.get_name()
                )
            }
        )
        workflow_code_map.update(
            {
                connection_workflow_name: await datamind.get_workflow_type_by_name(
                    manager, connection_workflow.get_name()
                )
            }
        )
        workflow_code_map.update(
            {
                upload_workflow_name: await datamind.get_workflow_type_by_name(
                    manager, upload_workflow.get_name()
                )
            }
        )
        workflow_code_map.update(
            {
                metadata_iso_workflow_name: await datamind.get_workflow_type_by_name(
                    manager, metadata_iso_workflow.get_name()
                )
            }
        )
    except NotFound:
        async with manager.transaction() as tx:
            workflow_code_map.update(
                {
                    simple_workflow_name: await datamind.create_asset_workflow_type(
                        manager, simple_workflow, tx
                    )
                }
            )
            workflow_code_map.update(
                {
                    connection_workflow_name: await datamind.create_asset_workflow_type(
                        manager, connection_workflow, tx
                    )
                }
            )
            workflow_code_map.update(
                {
                    upload_workflow_name: await datamind.create_asset_workflow_type(
                        manager, upload_workflow, tx
                    )
                }
            )
            workflow_code_map.update(
                {
                    metadata_iso_workflow_name: await datamind.create_asset_workflow_type(
                        manager, metadata_iso_workflow, tx
                    )
                }
            )

    return workflow_code_map


async def create_or_get_license_config(manager):
    license_config = await datamind.get_asset_by_property(
        manager,
        _c.NAME_PROPERTY_CODE,
        _lc.CKAN_LICENSE_CONFIG_CODE,
        asset_type=_c.AssetTypeCodes.LICENSE,
        asset_class=_c.AssetClassCodes.CONFIG,
    )
    if not license_config:
        from dynastore.tools.identifiers import generate_uuidv7

        license_config = _abm.AssetConfig(
            uuid=generate_uuidv7(),
            type_code=_c.AssetTypeCodes.LICENSE,
            class_code=_c.AssetClassCodes.CONFIG,
            state_code=_c.StateCodes.ACTIVE,
        )
        # TODO maybe this could be embedded in registry config logic
        license_config.add_property(
            _c.NAME_PROPERTY_CODE,
            _abm.Property(
                name=_c.NAME_PROPERTY_CODE,
                type=_c.PropertyDataTypes.STRING,
                body=_lc.CKAN_LICENSE_CONFIG_CODE,
            ),
        )
        license_config.add_property(
            _c.DEF_PROPERTY_CODE,
            _abm.Property(
                name=_c.DEF_PROPERTY_CODE,
                type=_c.PropertyDataTypes.STRING,
                # TODO we might want to have a different class for license objects
                body=_c.AssetClassCodes.DOCUMENT.name.lower(),
            ),
        )
        workflow_type = await datamind.get_workflow_type_by_name(
            manager, _lc.ETL_DEFAULT_WORKFLOW_NAME
        )
        license_config.set_workflow(workflow=workflow_type)
        await datamind.create_configs(manager=manager, asset_obj_list=[license_config])
    return await datamind.get_asset_by_property(
        manager,
        _c.NAME_PROPERTY_CODE,
        _lc.CKAN_LICENSE_CONFIG_CODE,
        asset_type=_c.AssetTypeCodes.LICENSE,
        asset_class=_c.AssetClassCodes.CONFIG,
    )
