import asyncio
import os
import logging
from enum import Enum
from uuid import UUID, uuid4
from typing import List, Optional

from fastapi import (
    APIRouter, 
    FastAPI, 
    Depends, 
    HTTPException, 
    status, 
    Body, 
    Query,
    Request
)
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, HTMLResponse
from contextlib import asynccontextmanager
from pyvis.network import Network
from google.api_core.exceptions import AlreadyExists

# --- Dynastore Modules ---

from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.modules import get_protocol
from dynastore.modules.spanner.spanner_module import SpannerModule
import dynastore.modules.spanner.constants as _c
import dynastore.modules.spanner.utils as _u
import dynastore.modules.spanner.datamind as datamind
import dynastore.modules.spanner.models as spanner_models
import dynastore.modules.spanner.constants as spanner_constants
import dynastore.modules.spanner.utils as spanner_utils
import dynastore.modules.spanner.models.business.asset as _abm
import dynastore.modules.spanner.models.business.principal as _pbm

import dynastore.modules.spanner.models.dto.property as _pdm
import dynastore.modules.spanner.models.dto.relation as _rdm
import dynastore.modules.spanner.models.dto.vocabulary as _vdm
import dynastore.modules.spanner.models.dto.asset as _adm
import dynastore.modules.spanner.models.dto.state as _sdm

# --- Legacy ---
from dynastore.extensions.datamind.legacy import constants as _lc
from dynastore.extensions.datamind.legacy import mapping as _lm
logger = logging.getLogger(__name__)

# --- Helper ---
def _get_manager():
    """
    Retrieves the Manager (Repository equivalent) from the SpannerModule.
    """
    spanner_module = get_protocol(SpannerModule)
    if not spanner_module:
        raise RuntimeError("SpannerModule not initialized")
    return spanner_module.get_manager()

class DatamindService(ExtensionProtocol):
    priority: int = 100
    router: APIRouter = APIRouter(prefix="/datamind", tags=["Datamind"])
    _spanner_module: SpannerModule

    def __init__(self, app: Optional[FastAPI] = None):
        super().__init__()
        self.app = app
        self._register_routes()

    def _register_routes(self):
        # Asset Endpoints
        self.router.add_api_route("/asset", self.create_asset, methods=["POST"], response_model=UUID)
        self.router.add_api_route("/asset", self.update_asset, methods=["PUT"], response_model=dict)
        self.router.add_api_route("/asset", self.get_asset, methods=["GET"], response_model=_adm.AssetDTO)
        self.router.add_api_route("/asset", self.delete_asset, methods=["DELETE"], response_model=dict)
        self.router.add_api_route("/asset/s", self.search_assets, methods=["GET"])
        self.router.add_api_route("/asset/types", self.get_valid_asset_types, methods=["GET"], response_model=dict)
        self.router.add_api_route("/asset/classes", self.get_valid_asset_classes, methods=["GET"], response_model=dict)
        self.router.add_api_route("/asset/gcp_path", self.get_gcp_path, methods=["GET"], response_model=str)

        # Config Endpoints
        self.router.add_api_route("/asset/config/attach", self.create_config_asset, methods=["PUT"], response_model=UUID)
        self.router.add_api_route("/asset/config/schemas", self.get_schema_by_config_uuid, methods=["GET"], response_model=List[_adm.AssetDTO])
        self.router.add_api_route("/asset/config/generator_templates", self.get_generator_template_by_config_uuid, methods=["GET"], response_model=list)
        self.router.add_api_route("/asset/config/renderer_templates", self.get_renderer_template_by_config_uuid, methods=["GET"], response_model=list)
        self.router.add_api_route("/asset/config/instances", self.get_instances_by_config_uuid, methods=["GET"], response_model=list)

        # Relation Endpoints
        self.router.add_api_route("/asset/relations", self.get_asset_relations, methods=["GET"], response_model=list)
        self.router.add_api_route("/asset/vocabulary/term", self.create_term, methods=["POST"], response_model=dict)
        self.router.add_api_route("/asset/{asset_uuid}/get_next_state_list", self.get_next_state_list, methods=["GET"], response_model=List[_sdm.StateDTO])

        # Visualization
        self.router.add_api_route("/visualize_graph", self.visualize_graph, methods=["GET"], response_class=HTMLResponse)

        # Legacy/Transformation Endpoints
        self.router.add_api_route("/team/{team_uuid}/metadata/{metadata_uuid}/resource/{resource_uuid}/get_model", self.get_model, methods=["GET"], response_model=dict)
        self.router.add_api_route("/registry", self.create_registry, methods=["POST"], response_model=dict)
        self.router.add_api_route("/legacy/package_show/", self.package_show, methods=["GET"], tags=["Legacy"], response_model=dict)
        self.router.add_api_route("/legacy/resource_show/", self.resource_show, methods=["GET"], tags=["Legacy"], response_model=dict)
        self.router.add_api_route("/legacy/package_publish/", self.package_publish, methods=["PUT"], tags=["Legacy"], response_model=dict)
        self.router.add_api_route("/legacy/package_list/", self.package_list, methods=["GET"], tags=["Legacy"], response_model=dict)
        self.router.add_api_route("/legacy/resource_create/", self.resource_create, methods=["POST"], tags=["Legacy"], response_model=dict)
        self.router.add_api_route("/legacy/resource_update/", self.resource_update, methods=["PUT"], tags=["Legacy"], response_model=dict)
        self.router.add_api_route("/legacy/package_create/", self.package_create, methods=["POST"], tags=["Legacy"], response_model=dict)
        self.router.add_api_route("/legacy/package_update/", self.package_update, methods=["PUT"], tags=["Legacy"], response_model=dict)
        self.router.add_api_route("/legacy/organization_create/", self.organization_create, methods=["POST"], tags=["Legacy"], response_model=dict)
        self.router.add_api_route("/legacy/user_show/", self.user_show, methods=["GET"], tags=["Legacy"], response_model=dict)
        self.router.add_api_route("/legacy/user_create/", self.user_create, methods=["POST"], tags=["Legacy"], response_model=dict)
        self.router.add_api_route("/legacy/user_update/", self.user_update, methods=["PUT"], tags=["Legacy"], response_model=dict)
        self.router.add_api_route("/legacy/user_delete/", self.user_delete, methods=["DELETE"], tags=["Legacy"], response_model=dict)
        self.router.add_api_route("/legacy/license_list/", self.license_list, methods=["GET"], tags=["Legacy"], response_model=dict)
        self.router.add_api_route("/legacy/license_create/", self.license_create, methods=["POST"], tags=["Legacy"], response_model=dict)

        # Event Endpoints
        self.router.add_api_route("/capture_event/", self.capture_event, methods=["POST"], tags=["Legacy"], response_model=dict)

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        spanner_module = get_protocol(SpannerModule)
        if not spanner_module:
            raise RuntimeError("SpannerModule not found.")
        self.spanner_module = spanner_module

        # Initialize legacy ETL, might be deleted in the future
        from dynastore.extensions.datamind.legacy.etl import startup
        manager = _get_manager()
        if manager:
            await startup.initialize(self, manager)
        else:
            logger.warning("Spanner manager not available. Skipping datamind legacy ETL initialization.")

        ###############################

        yield

    # ==========================================================================
    #                                 ASSET ENDPOINTS
    # ==========================================================================

    async def create_asset(
        self,
        assetDTO: _adm.AssetDTO = Body(...)
    ):
        manager = _get_manager()

        # Logic moved from Service: Validation
        if assetDTO.class_code or assetDTO.type_code:
            raise HTTPException(
                status_code=400, 
                detail="Class and type codes will be automatically determined from the config asset, they cannot be overridden"
            )
        
        asset_uuid = assetDTO.uuid or uuid4()
        asset = _abm.Asset(uuid=asset_uuid)

        # Logic moved from Service: Population
        _pdm.set_properties_to_asset(asset, assetDTO.properties)
        _rdm.set_relations_to_asset(asset, assetDTO.relations)
        _vdm.set_terms_to_asset(asset, assetDTO.terms)

        return await datamind.create_asset(manager, asset)

    async def update_asset(
        self,
        assetDTO: _adm.AssetDTO
    ):
        manager = _get_manager()
        asset_uuid = assetDTO.uuid
        
        # Logic moved from Service: State handling
        asset = _abm.Asset(uuid=asset_uuid, state_code=getattr(_c.StateCodes, assetDTO.state_code.upper()))

        if assetDTO.workflow_state:
            state = assetDTO.workflow_state.asset_state_code
            workflow_state_code = getattr(_c.AssetWorkflowStateCodes, state.upper())
            asset_state = _abm.AssetState(
                current_state=workflow_state_code, 
                reason=assetDTO.workflow_state.reason, 
                principal=None
            )
            asset.add_state(asset_state)

        # Logic moved from Service: Population
        _pdm.set_properties_to_asset(asset, assetDTO.properties)
        _rdm.set_relations_to_asset(asset, assetDTO.relations)
        _vdm.set_terms_to_asset(asset, assetDTO.terms)

        await datamind.update_asset(manager, asset)
        return {"success": True}

    async def get_asset(
        self,
        asset_uuid: UUID,
        only_active: Optional[bool] = True,
        deep: Optional[bool] = False
    ):
        manager = _get_manager()
        allowed_states = [_c.StateCodes.ACTIVE.value] if only_active else [state.value for state in _c.StateCodes]
        asset = await datamind.get_asset_by_uuid(manager=manager, asset_uuid=asset_uuid, allowed_states=allowed_states, deep=deep)
        return _adm.asset_to_asset_dto(asset, deep)

    async def delete_asset(
        self,
        asset_uuid: UUID,
        archive: Optional[bool] = False,
        cleanup: Optional[bool] = False
    ):
        manager = _get_manager()
        asset = _abm.Asset(uuid=asset_uuid)
        new_state = _c.StateCodes.DELETED if not archive else _c.StateCodes.ARCHIVED
        asset.set_state_code(new_state)
        
        await datamind.delete_asset(manager, asset, cleanup)
        return {"success": True}

    async def search_assets(
        self,
        types: Optional[str] = None,
        classes: Optional[str] = None,
        offset: Optional[int] = 0,
        limit: Optional[int] = 10
    ):
        manager = _get_manager()
        
        # Logic moved from Service: Parsing types/classes strings
        asset_type_ids = []
        if types:
            for code in types.replace(" ", "").split(","):
                val = getattr(_c.AssetTypeCodes, code.upper(), None)
                if not val:
                     raise HTTPException(status_code=400, detail=f"{code} is not a valid type")
                asset_type_ids.append(val.value)

        asset_class_ids = []
        if classes:
            for code in classes.replace(" ", "").split(","):
                val = getattr(_c.AssetClassCodes, code.upper(), None)
                if not val:
                    raise HTTPException(status_code=400, detail=f"{code} is not a valid class")
                asset_class_ids.append(val.value)

        assets = []
        async for asset in datamind.search_assets(manager, asset_type_ids, asset_class_ids, offset, limit):
            assets.append(_adm.asset_to_asset_dto(asset, deep=False))

        return JSONResponse(content=jsonable_encoder(assets))

    async def get_valid_asset_types(
        self,
        offset: Optional[int] = 0,
        limit: Optional[int] = 10
    ):
        manager = _get_manager()
        asset_types = []
        async for asset_type in datamind.get_valid_asset_types(manager, offset, limit):
            asset_types.append(asset_type)
        return JSONResponse(content=jsonable_encoder(asset_types))

    async def get_valid_asset_classes(
        self,
        offset: Optional[int] = 0,
        limit: Optional[int] = 10
    ):
        manager = _get_manager()
        asset_classes = []
        async for asset_class in datamind.get_valid_asset_classes(manager, offset, limit):
            asset_classes.append(asset_class)
        return JSONResponse(content=jsonable_encoder(asset_classes))

    async def get_gcp_path(
        self,
        asset_uuid: UUID
    ):
        manager = _get_manager()
        return await datamind.get_gcp_path_by_asset_uuid(manager, asset_uuid)

    # ==========================================================================
    #                                   CONFIG ENDPOINTS
    # ==========================================================================

    async def create_config_asset(
        self,
        request: Request,
        config_profile_code: str = Query(...),
        parent_profile_code: str = Query(...),
        relation_code: str = Query(...)
    ):
        manager = _get_manager()
        relation_code = getattr(_c.AssetRelationCodes, relation_code.upper())
        if not relation_code:
            raise HTTPException(status_code=400, detail=f"{relation_code} is not a valid relation code")
        
        config_asset = await datamind.get_asset_by_property(manager=manager, property_name=_c.PROFILE_PROPERTY_CODE, value=config_profile_code, asset_class=_c.AssetClassCodes.CONFIG)
        parent_config_asset = await datamind.get_asset_by_property(manager=manager, property_name=_c.PROFILE_PROPERTY_CODE, value=parent_profile_code, asset_class=_c.AssetClassCodes.CONFIG)
        config_asset.add_relation_from_another_asset(source_asset=parent_config_asset, relation_code=relation_code, attributes=_u.create_tree_attribute())

        return await datamind.update_asset(manager, config_asset)
    
    async def get_schema_by_config_uuid(
        self,
        config_uuid: UUID
    ):
        manager = _get_manager()
        # Logic moved from Service
        schema_assets = await datamind.get_asset_by_base_node_and_relation_type(
            manager,
            config_uuid, 
            _c.AssetRelationCodes.HAS_SCHEMA, 
            _c.TypeAttributeCodes.get_enum_code(), 
            _c.TypeAttributeCodes.HIERARCHY.value.lower()
        )
        return [_adm.asset_to_asset_dto(a) for a in schema_assets]

    async def get_generator_template_by_config_uuid(
        self,
        config_uuid: UUID
    ):
        manager = _get_manager()
        # Logic moved from Service
        assets = await datamind.get_asset_by_base_node_and_relation_type(
            manager,
            config_uuid, 
            _c.AssetRelationCodes.HAS_GENERATOR, 
            _c.TypeAttributeCodes.get_enum_code(), 
            _c.TypeAttributeCodes.HIERARCHY.value.lower()
        )
        return [_adm.asset_to_asset_dto(a) for a in assets]

    async def get_renderer_template_by_config_uuid(
        self,
        config_uuid: UUID
    ):
        manager = _get_manager()
        # Logic moved from Service
        assets = await datamind.get_asset_by_base_node_and_relation_type(
            manager,
            config_uuid, 
            _c.AssetRelationCodes.HAS_RENDERER, 
            _c.TypeAttributeCodes.get_enum_code(), 
            _c.TypeAttributeCodes.HIERARCHY.value.lower()
        )
        return [_adm.asset_to_asset_dto(a) for a in assets]

    async def get_instances_by_config_uuid(
        self,
        config_uuid: UUID,
        offset: Optional[int] = 0,
        limit: Optional[int] = 10
    ):
        manager = _get_manager()
        assets = []
        async for asset in datamind.get_asset_paginated_by_relation(
            manager,
            asset_relation_type_ids=_c.AssetRelationCodes.HAS_CONFIG.value, 
            target_asset_uuid=config_uuid, 
            offset=offset, 
            limit=limit
        ):
            assets.append(_adm.asset_to_asset_dto(asset, deep=False))
        return JSONResponse(content=jsonable_encoder(assets))

    # ==========================================================================
    #                                   RELATION ENDPOINTS
    # ==========================================================================

    async def get_asset_relations(
        self,
        relation_type_codes: Optional[str] = "",
        relations_to: Optional[str] = "", 
        relations_from: Optional[str] = "",
        offset: Optional[int] = 0,
        limit: Optional[int] = 10
    ):
        manager = _get_manager()
        
        # Logic moved from Service: ID resolution
        asset_relation_type_ids = []
        if relation_type_codes:
            for code in relation_type_codes.replace(" ", "").split(","):
                asset_relation_type_ids.append(getattr(_c.AssetRelationCodes, code.upper()).value)

        source_uuids = relations_to.replace(" ", "").split(",") if relations_to else []
        target_uuids = relations_from.replace(" ", "").split(",") if relations_from else []

        results = []
        async for relation_type_code, relation in datamind.get_asset_paginated_by_relation(
            manager,
            asset_relation_type_ids=asset_relation_type_ids, 
            source_asset_uuids=source_uuids, 
            target_asset_uuids=target_uuids, 
            offset=offset, 
            limit=limit
        ):
            results.append(_rdm.relation_to_asset_relation_dto(relation, relation_type_code))
        
        return JSONResponse(content=jsonable_encoder(results))

    async def create_term(
        self,
        asset_uuid: UUID,
        term_code: str,
        termDTO: _vdm.TermDTO
    ):
        manager = _get_manager()
        asset = _abm.Asset(uuid=asset_uuid, terms={term_code: _vdm.term_dto_to_term(termDTO)})
        await datamind.create_term(manager, asset)
        return {"success": True}

    
    async def get_next_state_list(
        self,
        asset_uuid: UUID
    ):
        manager = _get_manager()
        result = [
            #status.model_dump(mode='json') 
            _sdm.state_to_dto(status)
            async for status in datamind.get_next_state_list(manager, asset_uuid)
        ]
        return JSONResponse(content=jsonable_encoder(result), media_type="application/json")

    # ==========================================================================
    #                                   VISUALIZATION
    # ==========================================================================

    async def visualize_graph(self):
        manager = _get_manager()
        
        net = Network(notebook=True, directed=True,
                      select_menu=True, filter_menu=True, cdn_resources='in_line')
        net.repulsion(node_distance=150, central_gravity=0.1, spring_length=200, spring_strength=0.05, damping=0.09)
        
        # Use helper from datamind directly with manager
        nodes, edges = await datamind.get_graph_for_visualization(manager)
        
        for node in nodes:
            net.add_node(
                node.get(_c.GRAPH_NODE_UUID_KEY),
                color=node.get(_c.GRAPH_NODE_COLOR_KEY),
                label=node.get(_c.GRAPH_NODE_LABEL_KEY)
            )
        for edge in edges:
            net.add_edge(
                edge.get(_c.GRAPH_EDGE_SOURCE_KEY),
                edge.get(_c.GRAPH_EDGE_TARGET_KEY),
                label=edge.get(_c.GRAPH_EDGE_LABEL_KEY),
                title=edge.get(_c.GRAPH_EDGE_TITLE_KEY),
                arrows="to"
            )
        
        return net.generate_html()

    # ==========================================================================
    #                                   CKAN / TEAM ENDPOINTS
    # ==========================================================================

    #deprecated
    #@router.post("/team", response_model=UUID)
    # async def create_team(
        
    #     data: dict = Body(...)
    # ):
    #     manager = _get_manager()
    #     # Logic moved from Service
    #     asset = _abm.Asset(uuid=uuid4(), type_code=_c.AssetTypeCodes.TEAM.name, class_code=_c.AssetClassCodes.PROFILE.name)
    #     asset.add_property(_pdm.AssetProperty(name=_pdm.DATA_PROPERTY_CODE, type=_c.JSON_TYPE_CODE, body=data))
    #     return await datamind.create_asset(manager, asset=asset)

    #deprecated
    #@router.put("/team/{team_uuid}", response_model=dict)
    # async def update_team(
        
    #     team_uuid: UUID,
    #     data: dict = Body(...)
    # ):
    #     manager = _get_manager()
    #     # Logic moved from Service
    #     asset = _abm.Asset(uuid=team_uuid)
    #     asset.add_property(_pdm.AssetProperty(name=_pdm.DATA_PROPERTY_CODE, type=_c.JSON_TYPE_CODE, body=data))
    #     await datamind.update_asset(manager, asset=asset)
    #     return {"success": True}

    #deprecated
    #@router.get("/team/{team_uuid}", response_model=dict)
    # async def get_team(
        
    #     team_uuid: UUID,
    #     deep: Optional[bool] = False
    # ):
    #     manager = _get_manager()
    #     # Logic moved from Service
    #     asset = await datamind.get_asset_by_uuid(manager, team_uuid, deep)
    #     asset_data = asset.get_properties().get(_pdm.DATA_PROPERTY_CODE)
    #     asset_data["id"] = team_uuid
    #     return asset_data

    #deprecated
    #@router.post("/metadata", response_model=UUID)
    # async def create_metadata(
        
    #     team_uuid: UUID = Query(...),
    #     data: dict = Body(...)
    # ):
    #     manager = _get_manager()
    #     # Logic moved from Service
    #     asset = _abm.Asset(uuid=uuid4())
    #     asset.add_relation_from_another_asset(
    #         relation_type_code=_c.AssetRelationCodes.HAS_METADATA.name, 
    #         source_asset=_abm.Asset(uuid=team_uuid), 
    #         attributes=_u.create_tree_attribute()
    #     )
    #     asset.add_property(_pdm.AssetProperty(name=_pdm.DATA_PROPERTY_CODE, type=_c.JSON_TYPE_CODE, body=data))
    #     return await datamind.create_asset(manager, asset=asset)

    #deprecated
    #@router.put("/metadata/{metadata_uuid}", response_model=dict)
    # async def update_metadata(
        
    #     metadata_uuid: UUID,
    #     data: dict = Body(...)
    # ):
    #     manager = _get_manager()
    #     # Logic moved from Service
    #     asset = _abm.Asset(uuid=metadata_uuid)
    #     asset.add_property(_pdm.AssetProperty(name=_pdm.DATA_PROPERTY_CODE, type=_c.JSON_TYPE_CODE, body=data))
    #     await datamind.update_asset(manager, asset=asset)
    #     return {"success": True}

    #deprecated
    #@router.get("/metadata/{metadata_uuid}", response_model=dict)
    # async def get_metadata(
        
    #     metadata_uuid: UUID,
    #     deep: Optional[bool] = False
    # ):
    #     manager = _get_manager()
    #     # Logic moved from Service
    #     asset = await datamind.get_asset_by_uuid(manager, metadata_uuid, deep)
    #     return asset.get_properties().get(_pdm.DATA_PROPERTY_CODE)

    async def get_model(
        self,
        request: Request,
        team_uuid: UUID,
        metadata_uuid: UUID,
        resource_uuid: UUID
    ):
        manager = _get_manager()
        asset = await datamind.get_asset_by_tree_path(
            manager=manager,
            base_asset_uuid=team_uuid,
            path_asset_uuids=[metadata_uuid],
            target_uuid=resource_uuid,
            depth=5
        )
        
        return await _get_legacy_model(request= request, manager=manager, team_asset=asset)
        # TODO: refine and add the actual interpolation, this might require data property retrieval
            
    async def create_registry(
        self,
        request: Request,
        registry_config: dict = Body(...)
    ):
        manager = _get_manager()
        try:
            await datamind.create_registry(manager, registry_config, request.app.state.base_workflow_code_map)
        except AlreadyExists as e:
            raise HTTPException(status_code=409, detail=str(e))
        return JSONResponse(content={"success": True}, status_code=201)

    # ==========================================================================
    #                                   Legacy/TRANSFORMATION ENDPOINTS
    # ==========================================================================


    async def package_show(
        self,
        request: Request,
        uuid: UUID = Query(..., alias="id")  # CKAN often uses 'id' query param
    ):
        manager = _get_manager()
        return await _get_legacy_package(request, manager, uuid)

    async def resource_show(
        self,
        request: Request,
        uuid: UUID = Query(..., alias="id")  # CKAN often uses 'id' query param
    ):
        manager = _get_manager()
        return await _get_legacy_resource(request, manager, uuid)

    async def package_publish(
        self,
        request: Request,
        uuid: UUID = Query(..., alias="id")  # CKAN often uses 'id' query param
    ):
        manager = _get_manager()
        return await _legacy_package_publish(request, manager, uuid)

    async def package_list(
        self,
        request: Request,
        team_uuid: UUID = Query(..., alias="id")  # CKAN often uses 'id' query param
    ):
        manager = _get_manager()
        return await _get_legacy_package_list(request, manager, team_uuid)


    async def resource_create(
        self,
        request: Request,
        resource_body: dict = Body(...)
    ):
        manager = _get_manager()
        return await _create_legacy_resource(request=request, manager=manager, resource_body=resource_body)

    async def resource_update(
        self,
        request: Request,
        resource_body: dict = Body(...)
    ):
        manager = _get_manager()
        return await _update_legacy_resource(request=request, manager=manager, resource_body=resource_body)

    async def package_create(
        self,
        request: Request,
        package_body: dict = Body(...)
    ):
        manager = _get_manager()
        return await _create_legacy_package(request=request, manager=manager, package_body=package_body)

    async def package_update(
        self,
        request: Request,
        package_body: dict = Body(...)
    ):
        manager = _get_manager()
        return await _update_legacy_package(request=request, manager=manager, package_body=package_body)

    async def organization_create(
        self,
        request: Request,
        organization_body: dict = Body(...)
    ):
        manager = _get_manager()
        return await _create_legacy_organization(request, manager, organization_body)

    # ======= USER ENDPOINTS =======
    async def user_show(
        self,
        request: Request,
        user_uuid: UUID
    ):
        manager = _get_manager()
        return await _get_legacy_user(request, manager, user_uuid)

    async def user_create(
        self,
        request: Request,
        user_body: dict = Body(...)
    ):
        manager = _get_manager()
        return await _create_legacy_user(request, manager, user_body)

    async def user_update(
        self,
        request: Request,
        user_body: dict = Body(...)
    ):
        manager = _get_manager()
        return await _update_legacy_user(request, manager, user_body)

    async def user_delete(
        self,
        request: Request,
        user_uuid: UUID,
        hard_delete: Optional[bool] = Query(False)
    ):
        manager = _get_manager()
        return await _delete_legacy_user(request, manager, user_uuid, hard_delete)

    # ====== LICENSE ENDPOINTS ======
    async def license_list(
        self,
        offset: Optional[int] = Query(0),
        limit: Optional[int] = Query(10)
    ):
        # TODO this endpoint is slow since it requires deep fetching of all license assets, optimize if needed
        manager=_get_manager()
        license_assets = datamind.search_assets(
            manager=manager,
            asset_type_ids=[_c.AssetTypeCodes.LICENSE.value],
            asset_class_ids=[_c.AssetClassCodes.DOCUMENT.value],
            offset=offset,
            limit=limit,
            deep=True
        )
        result_list = []
        async for license_asset in license_assets:
            result_list.append(_lm.reverse_map_license(license_asset))
        return {
            "success": True,
            "result": result_list
        }
        
    async def license_create(
        self,
        request: Request,
        license_body: dict = Body(...)
    ):
        manager=_get_manager()
        await _create_legacy_license(request, manager, license_body)
        return {"success": True}
    # ==========================================================================
    #                                   EVENT ENDPOINTS
    # ==========================================================================
    
    async def capture_event(
        self,
        request: Request,
        event_body: dict = Body(...)
    ):
        logger.info(f"Received event: {event_body}, processing...")

        event_type_raw = (
            event_body.get("event_type")
            or event_body.get("type")
            or request.headers.get("ce-type")
        )
        if not event_type_raw:
            raise HTTPException(
                status_code=400,
                detail="Event type not found. Provide event_type/type in body or ce-type header.",
            )

        payload = event_body.get("obj_payload")
        if payload is None:
            raise HTTPException(status_code=400, detail="Event payload not found. Provide obj_payload.")

        try:
            event_type = LegacyEventType(event_type_raw)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Unsupported event type: {event_type_raw}")

        handler = LEGACY_EVENT_HANDLERS.get(event_type)
        if not handler:
            raise HTTPException(status_code=400, detail=f"No handler found for event type: {event_type.value}")

        await handler(_get_manager(), payload)
        # Receives events from gcp eventarc pipeline and dispatches to the correct LegacyEventType handler
        return HTMLResponse(content="Event captured", status_code=200)

# --- Events (Kept as is, using manager) ---
async def on_create_package(manager, package_body: dict, *args, **kwargs):
    # If org is not there, this will fail since we do not pass the request
    await _create_legacy_package(request=None, manager=manager, package_body=package_body)
    
async def on_update_package(manager, package_body: dict, *args, **kwargs):
    await _update_legacy_package(request=None, manager=manager, package_body=package_body)
    

class LegacyEventType(str, Enum):
    CREATE_PACKAGE = "create-package"
    UPDATE_PACKAGE = "update-package"
    CREATE_RESOURCE = "create-resource"
    UPDATE_RESOURCE = "update-resource"
    
    UPDATE_RESOURCE_BY_PACKAGE_INDEX = "update-resource_by_package_index"


LEGACY_EVENT_HANDLERS = {
    LegacyEventType.CREATE_PACKAGE: on_create_package,
    LegacyEventType.UPDATE_PACKAGE: on_update_package,
}

# --- TEMP---
async def _legacy_package_publish(request: Request, manager, package_uuid):
    package_asset = await datamind.get_asset_by_uuid(manager=manager, asset_uuid=package_uuid, deep=True)
    if not package_asset:
        return _return_ckan_not_found()
    # For now, just setting state to published
    package_asset.set_property(_lc.CKAN_PRIVATE_FLAG_CODE, False)
    await datamind.update_asset(manager=manager, asset=package_asset)
    return {"success": True}

async def _create_legacy_resource(request: Request, manager, resource_body):
    resource_legacy_type = resource_body.get(_lc.CKAN_RESOURCE_TYPE_CODE, _lc.CKAN_DEFAULT_RESOURCE_TYPE)
                                            # TODO This could change
    config_asset = await datamind.get_asset_by_property(manager, _c.PROFILE_PROPERTY_CODE, resource_legacy_type, asset_type=_c.AssetTypeCodes.RESOURCE, asset_class=_c.AssetClassCodes.CONFIG)
    mapped_asset = _lm.map_resource(resource_body, config_asset.get_uuid())
    await datamind.create_asset(manager, mapped_asset)
    return {"success": True}

async def _create_legacy_package(request: Request, manager, package_body):
    # 1. Validate license is valid and exists
    license_id = package_body.get("license_id")
    license_asset=None
    if license_id:
        license_asset = await datamind.get_asset_by_property(manager,_c.LICENSE_ID_PROPERTY_CODE, license_id, asset_type=_c.AssetTypeCodes.LICENSE)
        if not license_asset:
            raise ValueError(f"License with id {license_id} not found")
    # 2. Create principal if missing
    user_uuid = UUID(package_body.get('creator_user_id'))
    principal = await datamind.get_principal_by_uuid(manager, user_uuid)
    if not principal:
        principal = _pbm.Principal(
            uuid=user_uuid,
            # TODO, just to proceed, later ckan users will be synced with an etl
            principal_id=f"{_lc.CKAN_USER_ID_PREFIX}:{package_body.get('creator_user_id')}",
            type_code=_c.PrincipalTypes.USER,
        )
        await datamind.create_principal(manager, principal)
    # 3 create organization if missing
    organization_id = package_body.get("organization", {}).get("id")
    if not organization_id:
        raise ValueError("Package must have an organization")
    organization_uuid = UUID(organization_id)
    organization_asset = await datamind.get_asset_by_uuid(manager=manager, asset_uuid=organization_uuid)
    
    if not organization_asset:
        await _create_legacy_organization(request=request, manager=manager, organization_body=package_body.get("organization"))

    metadata_legacy_type = package_body.get(_lc.CKAN_METADATA_TYPE_CODE)
                                            # TODO This could change
    config_asset = await datamind.get_asset_by_property(manager,_c.PROFILE_PROPERTY_CODE, metadata_legacy_type, asset_type=_c.AssetTypeCodes.METADATA, asset_class=_c.AssetClassCodes.CONFIG)

    organization_tree = await datamind.get_asset_by_tree_path(base_asset_uuid=organization_uuid, depth=2, manager=manager)
    
    # TODO assuming each ckan organization have 1 registry, which is not guaranteed. Handle properly if have multiple
    organization_registry_asset = organization_tree.get_relations_by_code(_c.AssetRelationCodes.HAS_REGISTRY).pop().get_target()
    mapped_asset = _lm.map_dataset(package_body, config_asset.get_uuid(), organization_registry_asset)

    if license_asset:
        mapped_asset.add_relation_to_another_asset(relation_code=_c.AssetRelationCodes.HAS_LICENSE, target_asset=license_asset)
    await datamind.create_asset(manager=manager, asset=mapped_asset, principal=principal)
    for resource in package_body.get("resources"):
        await _create_legacy_resource(request=request, manager=manager, resource_body=resource)
    return {"success": True}

async def _update_legacy_package(request: Request, manager, package_body):
    # Similar logic to create, but updating existing assets
    package_uuid = UUID(package_body.get("id"))
    package_asset = await datamind.get_asset_by_uuid(manager=manager, asset_uuid=package_uuid, deep=True)
    if not package_asset:
        return _return_ckan_not_found()

    # THESE RELATIONS SHOULD BE UNIQUE
    registry_asset = package_asset.get_relations_by_code(relation_code=_c.AssetRelationCodes.HAS_METADATA).pop().get_source()
    config_asset = package_asset.get_relations_by_code(relation_code=_c.AssetRelationCodes.HAS_CONFIG).pop().get_target()

    # Map updated fields
    updated_asset = _lm.map_dataset(package_body=package_body, config_uuid=config_asset.get_uuid(), organization_registry_asset=registry_asset)

    package_asset.clear_properties()
    
    for prop in updated_asset.get_properties().values():
        package_asset.attach_property(prop)
    
    package_asset.clear_terms()
    
    for term in updated_asset.get_terms().values():
        package_asset.add_term(term_code=term.get_code(), term=term)
    # TODO for now only updating properties
    
    #for relation in updated_asset.get_relations().values():
    #    package_asset.add_relation(relation)

    for resource_body in package_body.get("resources", []):
        await _update_legacy_resource(request=request, manager=manager, resource_body=resource_body)
    await datamind.update_asset(manager=manager, asset=package_asset)
    return {"success": True}

async def _update_legacy_resource(request: Request, manager, resource_body):
    resource_uuid = UUID(resource_body.get("id"))
    resource_asset = await datamind.get_asset_by_uuid(manager=manager, asset_uuid=resource_uuid, deep=True)
    if not resource_asset:
        return await _create_legacy_resource(request=request, manager=manager, resource_body=resource_body)


    config_asset = resource_asset.get_relations_by_code(relation_code=_c.AssetRelationCodes.HAS_CONFIG).pop().get_target()
    updated_asset = _lm.map_resource(resource_data=resource_body, config_uuid=config_asset.get_uuid())

    resource_asset.clear_properties()
    for prop in updated_asset.get_properties().values():
        resource_asset.attach_property(prop)
    
    resource_asset.clear_terms()
    for term in updated_asset.get_terms().values():
        resource_asset.add_term(term_code=term.get_code(), term=term)
    # TODO for now only updating properties

    #for relation in updated_asset.get_relations().values():
    #    resource_asset.add_relation(relation)

    await datamind.update_asset(manager=manager, asset=resource_asset)
    return {"success": True}

async def _create_legacy_organization(request: Request, manager, organization_body):
    # 2. Access app via request.app instead of self.app
    # Note: Standard FastAPI stores variables in 'state', verify if you used 'app_state' or 'state'
    app_state = getattr(request.app, "app_state", None) or request.app.state

    # If using standard Starlette/FastAPI state object
    ckan_team_config_uuid = await _get_ckan_team_config_uuid(manager, app_state)
    mapped_asset = _lm.map_organization(organization_body, ckan_team_config_uuid)
    
    ckan_registry_config_uuid = await _get_ckan_registry_config_uuid(manager, app_state)
    # Later it could be improved using these kind of functions
    #team_config = await datamind.get_asset_by_tree_path(base_asset_uuid=legacy_team_config_uuid, depth=1, manager=manager)
    
    registry_document = _abm.Asset(uuid=uuid4())
    registry_document.add_relation_to_another_asset(relation_code=_c.AssetRelationCodes.HAS_CONFIG, target_asset=_abm.Asset(uuid=ckan_registry_config_uuid), attributes=_u.create_tree_attribute())
    mapped_asset.add_relation_to_another_asset(relation_code=_c.AssetRelationCodes.HAS_REGISTRY, target_asset=registry_document, attributes=_u.create_tree_attribute())

    await datamind.create_assets_in_single_transaction(manager=manager, assets=[registry_document, mapped_asset])


    return {"success": True}

async def _get_ckan_registry_config_uuid(manager, app_state):
    ckan_registry_config_uuid = getattr(app_state, "ckan_registry_config_uuid", None)
    if not ckan_registry_config_uuid:
        ckan_registry_config_asset = await datamind.get_asset_by_property(
            manager,
            _c.REGISTRY_PROPERTY_CODE,
            _lc.CKAN_REGISTRY_CODE,
            asset_type=_c.AssetTypeCodes.REGISTRY,
            asset_class=_c.AssetClassCodes.CONFIG
        )
        ckan_registry_config_uuid = ckan_registry_config_asset.get_uuid()
        app_state.ckan_registry_config_uuid = ckan_registry_config_uuid
    return ckan_registry_config_uuid

async def _get_ckan_team_config_uuid(manager, app_state):
    ckan_team_config_uuid = getattr(app_state, "ckan_team_config_uuid", None)
    if not ckan_team_config_uuid:
        ckan_team_config_asset = await datamind.get_asset_by_property(
            manager,
            _c.PROFILE_PROPERTY_CODE,
            _lc.CKAN_TEAM_PROFILE_CODE,
            asset_type=_c.AssetTypeCodes.TEAM,
            asset_class=_c.AssetClassCodes.CONFIG
        )
        if not ckan_team_config_asset:
            raise ValueError("CKAN Team Config Asset not found, registry should have been created beforehand")
        ckan_team_config_uuid = ckan_team_config_asset.get_uuid()
        app_state.ckan_team_config_uuid = ckan_team_config_uuid
    return ckan_team_config_uuid

async def _get_legacy_user(request: Request, manager, user_uuid):
    principal = await datamind.get_principal_by_uuid(manager, user_uuid)
    if not principal:
        return _return_ckan_not_found()

    user_dict = _lm.reverse_map_user(principal)
    return {
        "success": True,
        "result": user_dict
    }

async def _create_legacy_user(request: Request, manager, user_body):
    mapped_principal = _lm.map_user(user_body)
    await datamind.create_principal(manager=manager, principal=mapped_principal)
    return {"success": True}

async def _get_legacy_package_list(request: Request, manager, team_uuid):
    # Fetch all packages under the given team (organization)
    organization_asset = await datamind.get_asset_by_uuid(manager=manager, asset_uuid=team_uuid, deep=True)
    if not organization_asset:
        return _return_ckan_not_found()
    
    # Assuming each ckan organization have 1 registry, which is not guaranteed. Handle properly if have multiple
    organization_registry_asset_uuid = organization_asset.get_relations_by_code(_c.AssetRelationCodes.HAS_REGISTRY).pop().get_target().get_uuid()
    organization_registry_asset = await datamind.get_asset_by_uuid(manager=manager, asset_uuid=organization_registry_asset_uuid, deep=True)
    package_assets = organization_registry_asset.get_relations_by_code(_c.AssetRelationCodes.HAS_METADATA)
    
    package_list = []
    for relation in package_assets:
        package_asset = relation.get_target()
        #package_list.append(package_asset.get_term_translation("name", _lc.default_lang_code))
        # TODO fetch title instead
        package_list.append(package_asset.get_uuid())
    return {
        "success": True,
        "result": package_list
    }

async def _get_legacy_package(request: Request, manager, uuid):
    # 1. Fetch the main Package Asset
    package_asset = await datamind.get_asset_by_uuid(manager=manager, asset_uuid=uuid, deep=True)

    if not package_asset:
        return _return_ckan_not_found()

    package_dict = _lm.reverse_map_dataset(package_asset)

    # Prepare extraction tasks
    org_task_code = "org"
    resources_task_code = "resources"
    license_task_code = "license"
    config_task_code = "config"
    
    tasks = {}

    # --- Setup Organization Task ---
    registry_relations = package_asset.get_relations_by_code(_c.AssetRelationCodes.HAS_METADATA)
    if registry_relations:
        async def fetch_org():
            registry_uuid = registry_relations.pop().get_source().get_uuid()
            registry_asset = await datamind.get_asset_by_uuid(manager=manager, asset_uuid=registry_uuid, deep=True)
            org_relations = registry_asset.get_relations_by_code(_c.AssetRelationCodes.HAS_REGISTRY)
            if org_relations:
                parent_org_uuid = org_relations.pop().get_source().get_uuid()
                return await datamind.get_asset_by_uuid(manager=manager, asset_uuid=parent_org_uuid, deep=True)
            return None
        tasks[org_task_code] = fetch_org()

    # --- Setup Resources Task ---
    res_relations = package_asset.get_relations_by_code(_c.AssetRelationCodes.HAS_RESOURCE)
    if res_relations:
        resource_uuids = [rel.get_target().get_uuid() for rel in res_relations]
        tasks[resources_task_code] = datamind.get_assets_by_uuids(manager=manager, asset_uuids=resource_uuids, deep=True)

    # --- Setup License Task ---
    license_relation = package_asset.get_relations_by_code(_c.AssetRelationCodes.HAS_LICENSE)
    if license_relation:
        license_uuid = license_relation.pop().get_target().get_uuid()
        tasks[license_task_code] = datamind.get_asset_by_uuid(manager=manager, asset_uuid=license_uuid, deep=True)

    # --- Setup Config Task ---
    config_uuid = package_asset.get_config().get_uuid()
    tasks[config_task_code] = datamind.get_asset_by_uuid(manager=manager, asset_uuid=config_uuid)

    # 2. Execute all network requests concurrently
    results = await asyncio.gather(*tasks.values())
    resolved_tasks = dict(zip(tasks.keys(), results))

    # 3. Assemble Organization
    organization_dict = {}
    if resolved_tasks.get(org_task_code):
        organization_dict = _lm.reverse_map_organization(resolved_tasks[org_task_code])
    package_dict["organization"] = organization_dict
    package_dict["owner_org"] = organization_dict.get("id")

    # 4. Assemble Resources
    resources_list = []
    if resolved_tasks.get(resources_task_code):
        for res_asset in resolved_tasks[resources_task_code]:
            if res_asset:
                resources_list.append(_lm.reverse_map_resource(asset=res_asset, package_id=uuid))
    package_dict["resources"] = resources_list

    # 5. Assemble License
    if resolved_tasks.get(license_task_code):
        _lm.inject_license_fields(license_asset=resolved_tasks[license_task_code], package_dict=package_dict)

    # 6. Assemble Config
    if resolved_tasks.get(config_task_code):
        package_dict[_lc.CKAN_METADATA_TYPE_CODE] = resolved_tasks[config_task_code].get_property(_c.PROFILE_PROPERTY_CODE).get_body()

    return {
        "success": True,
        "result": package_dict
    }

async def _get_legacy_model(request: Request, manager, team_asset):
    """
    Retrieves the full legacy model hierarchy for a specific Resource UUID.
    Structure: Organization -> Package -> Resource
    """
    registry_asset = team_asset.get_relations_by_code(_c.AssetRelationCodes.HAS_REGISTRY).pop().get_target()
    package_asset = registry_asset.get_relations_by_code(_c.AssetRelationCodes.HAS_METADATA).pop().get_target()
    resource_asset = package_asset.get_relations_by_code(_c.AssetRelationCodes.HAS_RESOURCE).pop().get_target()

    team_asset_deep = await datamind.get_asset_by_uuid(manager=manager, asset_uuid=team_asset.get_uuid(), deep=True)
    package_asset_deep = await datamind.get_asset_by_uuid(manager=manager, asset_uuid=package_asset.get_uuid(), deep=True)
    resource_asset_deep = await datamind.get_asset_by_uuid(manager=manager, asset_uuid=resource_asset.get_uuid(), deep=True)

    organization_dict = _lm.reverse_map_organization(team_asset_deep)
    package_dict = _lm.reverse_map_dataset(package_asset_deep)
    
    # License injection is duplicated, could be put inside reverse_map_dataset
    license_relation = package_asset_deep.get_relations_by_code(_c.AssetRelationCodes.HAS_LICENSE)
    if license_relation:
        license_asset = await datamind.get_asset_by_uuid(manager=manager, asset_uuid=license_relation.pop().get_target().get_uuid(), deep=True)
        _lm.inject_license_fields(license_asset=license_asset, package_dict=package_dict)
    
    resource_dict = _lm.reverse_map_resource(asset=resource_asset_deep, package_id=package_asset_deep.get_uuid())

    model_result = {
        "organization": organization_dict,
        "package": package_dict,
        "resource": resource_dict,
        #"terriajs": {
        #    "base_url": terriajs_base_url
        #},
        #"ckan": {
        #    "base_url": ckan_base_url
        #}
    }

    return  model_result

async def _get_legacy_resource(request: Request, manager, uuid):
    # 1. Fetch the Resource Asset
    resource_asset = await datamind.get_asset_by_uuid(manager=manager, asset_uuid=uuid, deep=True)

    if not resource_asset:
        return _return_ckan_not_found()
    try:
        package_uuid = resource_asset.get_relations_by_code(_c.AssetRelationCodes.HAS_RESOURCE).pop().get_source().get_uuid()
    except:
        raise Exception("Package could not be retrieved")
    resource_dict = _lm.reverse_map_resource(asset=resource_asset, package_id=package_uuid)
    return {
        "success": True,
        "result": resource_dict
    }

async def _delete_legacy_user(request: Request, manager, user_uuid, hard_delete: Optional[bool] = False):
    # Logic moved from Service
    principal = _pbm.Principal(uuid=user_uuid)
    await datamind.delete_principal(manager, principal, hard_delete)
    return {"success": True}

async def _update_legacy_user(request: Request, manager, user_body):
    mapped_principal = _lm.map_user(user_body)
    # TODO what fields exacly do we want to be updateable, the decisions are as below
    # {
    #   "email_hash": "...",
    #   "about": null,    -> description
    #   "apikey": "...", -> ignored, keep the other fields
    #   "display_name": "...",  -> display_name
    #   "name": "...",
    #   "created": "...",
    #   "id": "...",   -> uuid
    #   "sysadmin": true, -> will be changed mostly
    #   "activity_streams_email_notifications": false,
    #   "state": "active",  -> state
    #   "number_of_edits": 0,
    #   "fullname": null,
    #   "email": "...",  -> email, cannot be updated
    #   "number_created_packages": 0
    # }
    await datamind.update_principal(manager=manager, principal=mapped_principal)
    return {"success": True}

async def _create_legacy_license(request: Request, manager, license_body):
    # Currently synchronous, as no async operations are needed here
    # TODO Might be good to refactor and reuse create_or_get_license_config 
    license_config = await datamind.get_asset_by_property(manager, _c.NAME_PROPERTY_CODE, _lc.CKAN_LICENSE_CONFIG_CODE, asset_type=_c.AssetTypeCodes.LICENSE, asset_class=_c.AssetClassCodes.CONFIG)
    
    # Unique license_id validation
    existing_license = await datamind.get_asset_by_property(manager,_c.LICENSE_ID_PROPERTY_CODE, license_body.get("id"), asset_type=_c.AssetTypeCodes.LICENSE)
    if existing_license:
        raise ValueError(f"License with id {license_body.get('id')} already exists")
    
    license_asset = _lm.map_license(license_body, license_config.get_uuid())

    await datamind.create_asset(manager=manager, asset=license_asset)

def _return_ckan_not_found():
    return {
        "success": False,
        "error": {"message": "Not Found", "__type": "Not Found Error"}
    }
