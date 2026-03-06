# Esempio: app/models/metadata.py
import base64
from pydantic import BaseModel, Field, HttpUrl, BeforeValidator, AfterValidator, ConfigDict  # model_config
from typing import Optional, Dict, Any, List
from datetime import datetime, UTC
import datetime as dt # For backward compatibility if needed, but we should use datetime class
from uuid import UUID, uuid4
import dynastore.modules.spanner.constants as _c
from dynastore.modules.spanner.models.business.asset import Asset, AssetState
from dynastore.modules.spanner.models.dto.workflow import AssetWorkflowStateDTO, state_history_from_asset
from dynastore.modules.spanner.models.dto.property import AssetPropertiesDTO, property_to_dto, properties_to_properties_dto
from dynastore.modules.spanner.models.dto.relation import AssetRelationsDTO, asset_relations_dto_from_asset
from dynastore.modules.spanner.models.dto.vocabulary import asset_terms_dto_from_asset, AssetTermsDTO


def parse_uuid(value: str) -> UUID:
    return UUID(value)


def serialize_uuid(uuid_obj: UUID) -> str:
    return str(uuid_obj)


class AssetDTO(BaseModel):
    uuid: Optional[UUID] = Field(default=None, description="UUID of the asset")
    class_code: Optional[str] = Field(default=None, description="Class code of the asset")
    # TODO these should also be converted to codes
    type_code: Optional[str] = Field(default=None, description="Type code of the asset")
    state_code: Optional[str] = Field(default=_c.StateCodes.ACTIVE.name, description="Status code of the asset")
    change_timestamp: Optional[datetime] = Field(default_factory=lambda: datetime.now(UTC))
    state_history: Optional[List[AssetWorkflowStateDTO]] = Field(
        default=None, description="State history of the asset")
    ################The rest is part of the upsert###############
    properties: Optional[AssetPropertiesDTO] = Field(default=None, description="Properties of the asset")
    relations: Optional[AssetRelationsDTO] = Field(default=None, description="Relations of the asset")
    terms: Optional[AssetTermsDTO] = Field(default=None, description="Terms attached to the asset")
    workflow_state: Optional[AssetWorkflowStateDTO] = Field(default=None, description="Properties of the asset")

def asset_to_asset_dto(asset: Asset, deep=False) -> AssetDTO:
    state_history = state_history_from_asset(asset, deep)
    return AssetDTO(
        uuid = asset.get_uuid(),
        class_code=asset.get_class_code().name,
        type_code=asset.get_type_code().name,
        state_code=asset.get_state_code().name,
        properties=asset_properties_dto_from_asset(asset),
        terms=asset_terms_dto_from_asset(asset),
        relations=asset_relations_dto_from_asset(asset) if deep else None,
        state_history= state_history if deep else None,
        workflow_state= None if not state_history else state_history[0]
    )


def asset_properties_dto_from_asset(asset:Asset) -> AssetPropertiesDTO:
    properties = asset.get_properties() or {}
    opt_body=None
    data_body=None
    tags_body=[]
    property_dtos = {}
    for name, prop in properties.items():
        if name == _c.DATA_PROPERTY_CODE:
            data_body = prop
        elif name == _c.OPT_PROPERTY_CODE:
            opt_body = prop
        elif name == _c.TAGS_PROPERTY_CODE:
            tags_body = prop.body
        else:
            property_dtos.update({name: property_to_dto(name, prop)})

    return properties_to_properties_dto( data_body, opt_body, property_dtos, tags_body)


def assetDTO_to_asset(assetDTO: AssetDTO) -> Asset:
    return Asset(uuid=assetDTO.uuid or uuid4(),
                 class_code= getattr(_c.AssetClassCodes, assetDTO.class_code.upper()),
                 type_code=getattr(_c.AssetTypeCodes, assetDTO.type_code.upper()),
                 state_code=getattr(_c.StateCodes, assetDTO.state_code.upper())
                 )


def asset_from_spanner_row(row: tuple) -> AssetDTO:
    """Converts a Spanner row (tuple) to an AssetDTO object."""
    return AssetDTO(
        uuid=UUID(bytes=base64.b64decode(row[0])),  # UUID from BYTES(16)
        class_code=_c.AssetClassCodes(row[1]).name,
        type_code=_c.AssetTypeCodes(row[2]).name,
        state_code=_c.AssetClassCodes(row[3]).name
    )


def asset_from_spanner_dict(row: dict) -> AssetDTO:
    """Converts a Spanner row (dictionary) to an AssetDTO object."""
    return AssetDTO(
        uuid=UUID(bytes=base64.b64decode(row["uuid"])),  # UUID from BYTES(16)
        asset_class_id=row["asset_class_id"],
        asset_type_id=row["asset_type_id"],
        state=row["state"],
        change_timestamp=row["change_timestamp"]
    )


def asset_to_spanner_dict(asset: AssetDTO) -> dict:
    """Converts an AssetDTO object to a dictionary suitable for Spanner insertion/update."""
    return {
        "uuid": base64.b64encode(asset.uuid.bytes),
        "asset_class_id": asset.asset_class_id,
        "asset_type_id": asset.asset_type_id,
        "state": asset.state,
        "change_timestamp": asset.change_timestamp,
    }