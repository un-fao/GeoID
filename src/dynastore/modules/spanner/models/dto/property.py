from pydantic import BaseModel, Field, Json, model_validator
from typing import Optional, Any, List, Union
import dynastore.modules.spanner.constants as _c
from dynastore.modules.spanner.models.business.asset import Asset, Property

class AssetPropertyDTO(BaseModel):
    name: str = Field(..., description="Property name")
    #body: Union[str, dict, list, int] = Field(..., description="Property body")
    body: Any = Field(..., description="Property body")
    type_code: _c.PropertyDataTypes = Field(..., description="Type of the property. Sample values: xml, json, string")
    deleted: Optional[Union[bool, None]] = Field(default=None, description="Property state") # if None or false it is not deleted

class AssetPropertiesDTO(BaseModel): 
    data: Optional[AssetPropertyDTO] = Field(default=None, description="Data property")
    # We might not want to allow this
    properties: Optional[list[AssetPropertyDTO]]= Field(default=None, description="Asset properties")
    opt: Optional[AssetPropertyDTO] = Field(default=None, description="Data property")
    tags: Optional[List[Any]] = Field(default=None, description="Tags")

    @model_validator(mode='before')
    @classmethod
    def enforce_data_name(cls, values):
        data = values.get(_c.DATA_PROPERTY_CODE)
        if data is None:
            return values
        # If data is dict, set name to "data"
        if isinstance(data, dict):
            data['name'] = _c.DATA_PROPERTY_CODE
            values[_c.DATA_PROPERTY_CODE] = data
        # If data is already a model instance (unlikely in 'before' mode), set attribute
        else:
            data.name = _c.DATA_PROPERTY_CODE
            values[_c.DATA_PROPERTY_CODE] = data
        return values
    

def property_to_dto(name: str, prop: Property) -> AssetPropertyDTO:
    if not prop:
        return None
    return AssetPropertyDTO(
        name=name,
        body=prop.get_body(),
        type_code=prop.get_type(),
        deleted=False if prop.get_state() == _c.StateCodes.ACTIVE else True
    )


def properties_to_properties_dto(data_body, opt_body, properties, tags_body):

    return AssetPropertiesDTO(
        data=property_to_dto(_c.DATA_PROPERTY_CODE, data_body),
        properties=list(properties.values()),
        opt=property_to_dto(_c.OPT_PROPERTY_CODE, opt_body),
        tags=tags_body
    )


def set_properties_to_asset(asset: Asset, properties: AssetPropertiesDTO):
    if properties.data:
        asset.add_property(property_name=_c.DATA_PROPERTY_CODE, property=Property(
            body=properties.data.body, type=properties.data.type_code))
    if properties.properties:
        for property in properties.properties:
           asset.add_property(property_name=property.name, property=Property(
                body=property.body, type=property.type_code, state=_c.StateCodes.DELETED if property.deleted else _c.StateCodes.ACTIVE))
    if properties.opt:
        pass  # TODO Is there any special logic to apply ?
    if properties.tags:
        asset.add_property(property_name=_c.TAGS_PROPERTY_CODE, property=Property(
            body=properties.tags, type=_c.PropertyDataTypes.ARRAY))

