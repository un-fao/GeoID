from collections import OrderedDict
from typing import Generic, TypeVar
from dataclasses import dataclass, field
from typing import Any, Optional, Dict
from uuid import UUID, uuid4
from abc import ABC
import datetime
import copy
import dynastore.modules.spanner.constants as _c
import dynastore.modules.spanner.models.business.principal as _pbm
from pydantic import BaseModel



@dataclass
class Property():
    body: Any
    type: _c.PropertyDataTypes
    name: Optional[str] = None
    id: Optional[int] = None
    #change_history: Optional[list[PropertyChange]] = None
    state: _c.StateCodes = _c.StateCodes.ACTIVE
    # def __init__.(self, **kwargs):
    #     # self.name = kwargs.get("name")  # maybe we want to create if null
    #     self.id = kwargs.get("id")
    #     self.body = kwargs.get("body")
    #     self.type = kwargs.get("type")

    def __post_init__(self, **kwargs):
        type_code = kwargs.get('type_code')
        if type_code:
            self.type = getattr(_c.PropertyDataTypes, type_code.upper())
    def get_type(self):
        if not self.type:
            return _c.PropertyDataTypes.STRING
        return self.type

    def get_name(self):
        return self.name
    
    def get_body(self):
        return self.body

    def set_body(self, body):
        self.body = body

    def get_state(self):
        return self.state

    def to_dict(self):
        return {
            "id": self.id,
            # "name": self.name,
            "body": self.body,
            # "type": self.type.value  # Convert enum to its value
            "type": self.type
        }

## JUST FAKE DEFINITIONS FOR SOME CLASSES
@dataclass
class Attribute:

    code: str
    attribute: str


    # Getter and setter for code
    def get_code(self) -> str:
        return self.code

    def set_code(self, value: str):
        self.code = value

    # Getter and setter for attribute
    def get_attribute(self) -> Any:
        return self.attribute

    def set_attribute(self, value: Any):
        self.attribute = value


@dataclass
class AssetState():
    # asset_state_history
    current_state: _c.AssetWorkflowStateCodes
    reason: str
    #workflow_type_id: int
    principal: Optional[_pbm.Principal] = None
    id: Optional[int] = None
    change_timestamp: Optional[datetime.datetime] = None

    # id
    def get_id(self) -> int:
        return self._id

    def set_id(self, value: int):
        self._id = value

    # current_state
    def get_current_state(self) -> _c.AssetWorkflowStateCodes:
        return self.current_state

    def set_current_state(self, current_state: _c.AssetWorkflowStateCodes):
        self.current_state = current_state

    # reason
    def get_reason(self) -> str:
        return self.reason

    def set_reason(self, reason: str):
        self.reason = reason

    # change_timestamp
    def get_change_timestamp(self) -> datetime.datetime:
        return self.change_timestamp

    def set_change_timestamp(self, change_timestamp: datetime.datetime):
        self.change_timestamp = change_timestamp

    # workflow_type_id
    # def get_workflow_type_id(self) -> Principal:
    #     return self.principal

    # def set_workflow_type_id(self, workflow_type_id: int):
    #     self.workflow_type_id = workflow_type_id

    # principal
    def get_principal(self) -> _pbm.Principal:
        return self.principal

    def set_principal(self, principal: _pbm.Principal):
        self.principal = principal

    # def __init__(self, **kwargs):
    #     self.workflow = kwargs.get('workflow', [])
    #     self.current_state = kwargs.get('current_state', [])
    #     self.reason = kwargs.get('reason')
    #     self.principal = kwargs.get('principal', Principal())

    def get_next_state(self) -> list[_c.AssetWorkflowStateCodes]:
        return self.workflow.get_next_states(self.current_state)

@dataclass
class TermTranslation():
    language_code: str
    value: str

    # def __init__(self, **kwargs):
    #     # self.name = kwargs.get("name")  # maybe we want to create if null
    #     self.language_code = kwargs.get("language_code")
    #     self.value = kwargs.get("value")

    def get_value(self):
        return self.value

    def set_value(self, value):
        self.value = value

    def get_language_code(self):
        return self.language_code

    def set_language_code(self, language_code):
        self.language_code = language_code

@dataclass
class Term():

    translations: Dict[str, TermTranslation] = field(default_factory=dict) # "en":"value", "es": "valor"
    # default_language_code: str

    id: Optional[int] = None
    # TODO in table this is called term_code according to naming convention it could be better to make it just code
    code: str = field(default_factory=str)
    state_code: _c.StateCodes = _c.StateCodes.ACTIVE
    #def __init__(self, **kwargs):
        # maybe we want to create if null
        #pass
        # self.code = kwargs.get("code")

    def get_id(self):
        return self.id

    def set_id(self, id):
        self.id = id

    def get_code(self):
        return self.code

    def set_code(self, code):
        self.code = code

    def get_translations(self):
        return self.translations

    def get_translation(self, language_code):
        return self.translations.get(language_code)

    def add_translation(self, language_code, value):
        if self.translations.get(language_code):
            raise (
                f"Translation already exists for term {self.code} in language {language_code}")
        self.translations.update({language_code: TermTranslation(language_code=language_code, value=value)})

    def get_state_code(self) -> _c.StateCodes:
        if not self.state_code:
            return _c.StateCodes.ACTIVE
        return self.state_code

    # def get_default_translation(self) -> str:
    #     return self.translations.get(self.default_language_code)

    # def get_translations(self) -> list[str]:
    #     return list(self.translations.values())

    # def get_translation_by_language(self, language_code) -> str:
    #     return self.translations.get(language_code)


######## END OF FAKE DEFINITIONS ##########
S = TypeVar('S')
T = TypeVar('T')

@dataclass
class Relation(Generic[S,T]):
    #id: int
    source: S
    target: T
    attributes: Dict[str, Attribute] = field(default_factory=dict)
    code: Optional[_c.AssetRelationCodes] = None
    state_code: _c.StateCodes = _c.StateCodes.ACTIVE
    

    # def __init__(self, **kwargs):
    #     self.source = kwargs.get("source")
    #     self.target = kwargs.get("target")
    #     self.attributes = kwargs.get("attributes", None)
    def get_code(self) -> _c.AssetRelationCodes:
        return self.code

    def get_source(self) -> S:
        return self.source
    
    def get_target(self) -> T:
        return self.target
    
    def get_attributes(self) -> Dict[str, Any]:
        return self.attributes or {}

    def get_attribute(self, attribute_code) -> list:
        return self.attributes.get(attribute_code)

    def add_attribute(self, attribute: Attribute) -> None:
        attribute_code = attribute.get_code()
        attributes = self.attributes.get(attribute_code) or []
        if attribute not in attributes:
            attributes.append(attribute)
        else:
            raise Exception (f"Attribute with code {attribute_code} and with value {attribute.get_attribute()} already exists in relation")
        self.attributes.update({attribute_code: attributes})

    def get_state_code(self):
        return self.state_code

    def __post_init__(self):
        if not self.source and not self.target:
            raise Exception("Relation source and target must be provided")

        if self.attributes is None:
            self.attributes = {}

    def to_dict(self, from_root=True):
            if not from_root:
                return {
                "source": self.source,
                "attributes": self.attributes
                }
            else:
                return {
                "target": self.target,
                "attributes": self.attributes
                }

@dataclass
class AssetWorkflowType():
    # asset_workflow_type
    name: str
    description: str
    principal: _pbm.Principal # will only be created while we monitor, create, update workflow type 
    next_state_edges: list[Relation[_c.AssetWorkflowStateCodes,
                               _c.AssetWorkflowStateCodes]]
    
    id: Optional[int] = None
    creation_timestamp: Optional[datetime.datetime] = field(default_factory=datetime.datetime.now)
    state_code: _c.StateCodes = _c.StateCodes.ACTIVE

    # asset_workflow_edge
    


    # def __init__(self, **kwargs):
    #     self.next_states = kwargs.get('next_states', [])
    #     self.prev_states = kwargs.get('prev_states', [])

    def __copy__(self):
        return copy.copy(self)

    def get_id(self):
        return self.id

    def set_id(self, id):
        self.id = id
    
    def get_name(self):
        return self.name

    def get_description(self):
        return self.description

    def get_principal(self):
        return self.principal

    def get_creation_timestamp(self):
        return self.creation_timestamp

    def get_next_state_edges(self):
        return self.next_state_edges

    def get_next_states(self, current_state: _c.AssetWorkflowStateCodes) -> list[_c.AssetWorkflowStateCodes]:
        return [relation.target for relation in self.next_state_edges if relation.source == current_state]

    def get_prev_states(self, current_state: _c.AssetWorkflowStateCodes) -> list[_c.AssetWorkflowStateCodes]:
        return [relation.source for relation in self.next_state_edges if relation.target == current_state]

    def add_next_state_edge(self, source_state: _c.AssetWorkflowStateCodes, target_state: _c.AssetWorkflowStateCodes, attributes: Optional[Dict[str, Attribute]] = None):
        relation = Relation(source=source_state,
                            target=target_state, attributes=attributes or {})
        self.next_state_edges.append(relation)

    def get_state_code(self):
        return self.state_code

    def to_dict(self):
        dict_obj = copy.deepcopy(self.__dict__)
        for relation_key, relation_assets in dict_obj.get('relations', {}).items():
            new_relation_assets = []
            for asset in relation_assets:
                new_relation_assets.append(asset.to_dict())
            dict_obj['relations'][relation_key] = new_relation_assets
        properties = []
        for property_key, property_value in dict_obj.get('properties', {}).items():
            dict_obj['properties'][property_key] = property_value.to_dict()
        return dict_obj

@dataclass
class AbstractAsset(Generic[T]):
    # Note making all public otherwise to_dict should not ship them which might make it useless
    # TODO add attributes (might require new draft of db with new ddl)
    relations: Optional[Dict[str, list[Relation[T, T]]]] = field(default_factory=dict)
    properties: Optional[Dict[str, Property]] = field(default_factory=dict)
    terms: Optional[Dict[str, Term]] = field(default_factory=dict)
    # maybe could be an ordered dict ?
    state_history: Optional[list[AssetState]] = None
    # actions: {str, list[T]} = {}
    
    type_code: _c.AssetTypeCodes = None#field(default_factory=Enum)
    class_code: _c.AssetClassCodes = None#field(default_factory=Enum)
    state_code: _c.StateCodes = None  # field(default_factory=Enum)

    # uuid: UUID = field(default=uuid4(), init=True) # causes the same uuid created in all instances 
    uuid: UUID = field(default_factory=uuid4, init=True) 

    # def __init__(self, **kwargs):
    #     self.uuid = kwargs.get("uuid")
    #     self.type_code = kwargs.get("type_code")
    #     self.class_code = kwargs.get("class_code")
    #     self.state_code = kwargs.get("state_code")

    #     self.relations = kwargs.get('relations', {})
    #     self.properties = kwargs.get('properties', {})
    #     self.terms = kwargs.get('terms', {})

    def __post_init__(self, **kwargs):
        if not self.relations:
            self.relations = {}

        if not self.properties:
            self.properties = {}

        if not self.terms:
            self.terms = {}

        if not isinstance(self.uuid, UUID):
            self.uuid = UUID(self.uuid)

    def __copy__(self):
        return copy.copy(self)

    def get_type_code(self) -> _c.AssetTypeCodes:
        return self.type_code

    def get_class_code(self) -> _c.AssetClassCodes:
        return self.class_code

    def get_state_code(self) -> _c.StateCodes:
        if not self.state_code:
            return _c.StateCodes.ACTIVE
        return self.state_code

    def set_type_code(self, type_code: _c.AssetTypeCodes):
       self.type_code = type_code

    def set_class_code(self, class_code: _c.AssetClassCodes):
        self.class_code = class_code

    def set_state_code(self, state_code: _c.StateCodes):
        self.state_code = state_code

    def set_uuid(self, uuid: UUID = uuid4()):
        self.uuid = uuid

    def get_uuid(self) -> UUID:
        return self.uuid
    
    def get_encoded_id(self):
        pass

    def attach_relation_to_another_asset(self, relation: Relation):
            """
            Receives a fully formed Relation object and stores it.
            """
            # We assume the relation object has a .code attribute
            if not relation.code:
                raise Exception("Relation must have a code to be added to asset directly")

            if relation.code in self.relations:
                self.relations[relation.code].append(relation)
            else:
                self.relations[relation.code] = [relation]

    def attach_relation_from_another_asset(self, relation: Relation):
        """
        Receives a fully formed Relation object and stores it.
        (Logic is currently identical to 'to', but kept separate per request)
        """
        # We assume the relation object has a .code attribute
        if not relation.code:
            raise Exception("Relation must have a code to be added to asset directly")
        if relation.code in self.relations:
            self.relations[relation.code].append(relation)
        else:
            self.relations[relation.code] = [relation]

    # --- WRAPPER / FACTORY METHODS (Creation) ---

    def add_relation_to_another_asset(self, relation_code, target_asset, attributes=None):
        """
        Creates the Relation object (Self -> Target) and passes it to the core logic.
        """
        new_relation = Relation(
            source=self,
            target=target_asset,
            attributes=attributes,
            code=relation_code
        )

        # Call the core logic method defined above
        self.attach_relation_to_another_asset(new_relation)

    def add_relation_from_another_asset(self, relation_code, source_asset, attributes=None):
        """
        Creates the Relation object (Source -> Self) and passes it to the core logic.
        """
        new_relation = Relation(
            source=source_asset,
            target=self,
            attributes=attributes,
            code=relation_code
        )

        # Call the core logic method defined above
        self.attach_relation_from_another_asset(new_relation)

    def get_relations(self) -> {str, list[Relation[T, T]]}:
        return self.relations

    def get_relations_by_code(self, relation_code) -> list[Relation[T, T]]:
        if relation_code in self.relations:
            return self.relations.get(relation_code)
        return None

    def get_relation_to_another_asset(self, relation_code, target_asset_uuid) -> Relation[T, T]:
        relations = self.relations.get(relation_code)
        if not relations:
            return None
        for relation in relations:
            if relation.get_target().get_uuid() == target_asset_uuid:
                return relation
        return None

    def get_relation_from_another_asset(self, relation_code, source_asset_uuid) -> Relation[T, T]:
        relations = self.relations.get(relation_code)
        if not relations:
            return None
        for relation in relations:
            if relation.get_source().get_uuid() == source_asset_uuid:
                return relation
        return None

    def get_property(self, property_name) -> Property:
        return self.properties.get(property_name)


    def set_property(self, property_name, value) -> None:
        prop = self.properties.get(property_name)
        if prop:
            prop.set_body(value)
        else:
            prop = Property(name=property_name, body=value)
            self.properties[property_name] = prop



    def get_properties(self) -> Property:
        return self.properties

    def add_property(self, property_name, property: Property):

        self.properties.update({property_name: property})

    def attach_property(self, property: Property):
        self.add_property(property.get_name(), property)

    def clear_properties(self):
        self.properties = {}

    def add_data(self, data: Any, data_type:_c.PropertyDataTypes):
        data_property = Property(
            name=_c.DATA_PROPERTY_CODE,
            body=data,
            type=data_type or _c.PropertyDataTypes.OBJECT
        )
        self.add_property(data_property.get_name(), data_property)

    def add_state(self, state_history):
        if not self.state_history:
            self.state_history = [state_history]
        else:
            self.state_history.append(state_history)

    def get_current_state(self):
        # TODO could be converted to 0th index if becomes necessary 
        if self.state_history and len(self.state_history) > 0:
            return self.state_history[-1]
        return None
    
    def get_state_history(self):
        return self.state_history

    def get_canonical_language(self):
        canonical_language = self.terms.get(_c.CANONICAL_LANGUAGE_CODE_KEY)
        if canonical_language:
            return canonical_language

        return _c.DEFAULT_CANONICAL_LANGUAGE_CODE

    def hasterms(self) -> bool:
        pass

    def get_term(self, term_code) -> Term:
        return self.terms(term_code)

    # Technically possibly to have multiple terms with the same code
    def get_terms(self) -> {str, Term}:
        return self.terms

    def add_term(self, term_code, term):
        self.terms.update({term_code: term})

    def clear_terms(self):
        self.terms = {}

    def add_term_translation(self, term_code: str, value: str, language_code: str = None):
        """
        Convenience method to add a term translation without manually creating objects.
        If the Term doesn't exist, it creates it.
        If the Term exists, it appends the new translation.
        """
        # 1. Safety check for empty values
        if not value:
            return

        # 2. Default language fallback
        if not language_code:
            language_code = self.get_canonical_language()

        # 3. Get existing Term or Create new one
        term = self.terms.get(term_code)
        if not term:
            term = Term(code=term_code)
            self.terms[term_code] = term

        # 4. Add the translation (using the fixed logic above)
        term.add_translation(language_code, value)
    
    def get_term_translation(self, term_code, language_code) -> Optional[str]:
        term = self.terms.get(term_code)
        if not term:
            return None
        translation = term.get_translation(language_code)
        if not translation:
            return None
        return translation.get_value()

    def to_dict(self):
        dict_obj = copy.deepcopy(self.__dict__)
        for relation_key, relation_assets in dict_obj.get('relations', {}).items():
            new_relation_assets = []
            for asset in relation_assets:
                new_relation_assets.append(asset.to_dict())
            dict_obj['relations'][relation_key] = new_relation_assets
        properties = []
        for property_key, property_value in dict_obj.get('properties', {}).items():
            dict_obj['properties'][property_key] = property_value.to_dict()
        return dict_obj


class AssetConfig(AbstractAsset[AbstractAsset]):
    workflow: Optional[AssetWorkflowType] = None
    def __post_init__(self, **kwargs):
        self.class_code = _c.AssetClassCodes.CONFIG
        self.workflow = None
    # TODO will have the specialized logic to get these from its relations
    def get_workflow(self) -> AssetWorkflowType:
        return self.workflow

    def set_workflow(self, workflow: AssetWorkflowType):
        self.workflow = workflow

    def get_schema(self) -> AbstractAsset[AbstractAsset]:
        pass

    def get_template(self) -> AbstractAsset[AbstractAsset]:
        pass

@dataclass
class Asset(AbstractAsset[AbstractAsset]):
    def get_config(self) -> AssetConfig:  # from the relations get the config(profile) from has_config
        config_rel_list = self.relations.get(_c.AssetRelationCodes.HAS_CONFIG)
        if not config_rel_list:
            # TODO maybe we can log warning
            return None
        return config_rel_list[0].get_target()


@dataclass
class AssetRelation(Relation[Asset, Asset]):
    pass
    # def __init__(self, **kwargs):
    #     Relation.__init__(self, **kwargs)
    
@dataclass
class State():
    id: int
    name: str
    def get_id(self) -> int:
        return self.id
    def get_name(self) -> str:
        return self.name
    def set_id(self, value: int):
        self.id = value
    def set_name(self, value: str):
        self.name = value