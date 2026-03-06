
from pydantic import BaseModel, Field  
from typing import Optional, Dict, Any, List
from uuid import UUID
import dynastore.modules.spanner.constants as _c

from dynastore.modules.spanner.models.business.asset import Asset, Relation, AssetRelation, Attribute


class AssetRelationAttributeDTO(BaseModel):
    code: Optional[str] = Field(default=None, description="Attribute code")
    attribute: Any = Field(default=None, description="Attribute json value")

class AssetRelationDTO(BaseModel):
    source_uuid: Optional[UUID] = Field(default=None, description="Source asset object")
    target_uuid: Optional[UUID] = Field(default=None, description="Target asset object")
    attributes: Optional[AssetRelationAttributeDTO] = Field(default=None, description="Asset relation attributes by relation type code")
    relation_type_code: str = Field(..., description="Relation type code")


class AssetFromRelationDTO(BaseModel):
    source_uuid: UUID = Field(default=None, description="Source asset object")
    attributes: Optional[Dict[str, List[AssetRelationAttributeDTO]]] = Field(
        ..., description="Asset relation attributes by relation type code")
    relation_type_code: str = Field(..., description="Relation type code")


class AssetToRelationDTO(BaseModel):
    target_uuid: UUID = Field(default=None, description="Target asset object")
    attributes: Optional[Dict[str, List[AssetRelationAttributeDTO]]] = Field(..., description="Asset relation attributes by relation type code")
    relation_type_code: str = Field(..., description="Relation type code")


class AssetRelationsDTO(BaseModel):
    relations_from: List[AssetFromRelationDTO] = Field(default=None, description="Inner relations of the asset")
    relations_to: List[AssetToRelationDTO] = Field(default=None, description="Outer relations of the asset")


def asset_relations_dto_from_asset(asset: Asset):
    relations_from: List[AssetFromRelationDTO] = []
    relations_to: List[AssetToRelationDTO] = []
    for relation_code, relations_list in asset.get_relations().items():
        for relation in relations_list:
            # current asset is the target
            if relation.target.uuid == asset.uuid:
                relations_from.append(
                    AssetFromRelationDTO(
                        source_uuid=relation.source.uuid,
                        attributes=create_attributes_from_relation(relation),
                        relation_type_code=_c.AssetRelationCodes(
                            relation_code).name.lower()
                    )
                )
            # current asset is the source
            elif relation.source.uuid == asset.uuid:
                relation_to = AssetToRelationDTO(
                    target_uuid=relation.target.uuid,
                    attributes=create_attributes_from_relation(relation),
                    relation_type_code=_c.AssetRelationCodes(
                        relation_code).name.lower()
                )
                type_attribute = relation.get_attributes().get(_c.TypeAttributeCodes.get_enum_code())

                relations_to.append(
                    relation_to
                )

    # Return DTO with lists (can be empty if no relations)
    return AssetRelationsDTO(
        relations_from=relations_from,
        relations_to=relations_to
    )

def create_attributes_from_relation(relation: AssetRelation):
    return {
        k: [
            AssetRelationAttributeDTO(
                code=k, attribute=a.attribute) if isinstance(a, Attribute) else a
            for a in v
        ]
        for k, v in relation.attributes.items()
    }

def set_relation_to_dto_to_asset_relation(relation_to: AssetToRelationDTO, asset: Asset):
    asset.add_relation_to_another_asset(target_asset=Asset(uuid=relation_to.target_uuid), relation_code=getattr(_c.AssetRelationCodes, relation_to.relation_type_code.upper()), attributes=create_attributes_from_attributes_dict(relation_to.attributes))

def set_relation_from_dto_to_asset_relation(relation_from: AssetFromRelationDTO, asset: Asset):
    asset.add_relation_from_another_asset(source_asset=Asset(uuid=relation_from.source_uuid), relation_code=getattr(_c.AssetRelationCodes, relation_from.relation_type_code.upper()), attributes=create_attributes_from_attributes_dict(relation_from.attributes))

def create_attributes_from_attributes_dict(attributes_dict: dict):
    return {
    attribute_code: [
        # TODO should be fixed, mark: attribute_list_fix
        Attribute(code=attribute_code, attribute=attribute.attribute)
        for attribute in attributes
    ]
    for attribute_code, attributes in attributes_dict.items()
}

def set_relations_to_asset(asset: Asset, relations: AssetRelationsDTO):
    if not relations:
        return

    if relations.relations_from:
        for relation_from in relations.relations_from:
            set_relation_from_dto_to_asset_relation(
                relation_from=relation_from, asset=asset)
    if relations.relations_to:
        for relation_to in relations.relations_to:
            set_relation_to_dto_to_asset_relation(
                relation_to=relation_to, asset=asset)



def set_attributes_to_relation(attributes: List[AssetRelationAttributeDTO], relation: AssetRelation):
    for attribute in attributes:
        relation.add_attribute(
            Attribute(
                code=attribute.code, attribute=attribute.attribute)
        )


def asset_relationDTO_to_asset_relation(relationDTO: AssetRelationDTO) -> AssetRelation:
    return AssetRelation(source=Asset(uuid=relationDTO.source_uuid), target=Asset(uuid=relationDTO.target_uuid), attributes=relationDTO.attributes)


def relation_to_asset_relation_dto(relation: AssetRelation, relation_type_code: Optional[_c.AssetRelationCodes] = None) -> AssetRelationDTO:
    #attributes_dto = attributes_to_dto_list(relation.attributes)
    return AssetRelationDTO(
        relation_type_code=relation_type_code.name.lower(),
        source_uuid=relation.source.uuid if relation.source else None,
        target_uuid=relation.target.uuid if relation.target else None,
        attributes=None
    )
