from collections import OrderedDict
from typing import Generic, TypeVar
from dataclasses import dataclass, field
from typing import Any, Optional, Dict
from uuid import UUID, uuid4
from pydantic import BaseModel
import dynastore.modules.spanner.models.business.principal as _pbm
import dynastore.modules.spanner.models.business.asset as _abm
import dynastore.modules.spanner.constants as _c


class GenericPermissionBits(BaseModel):
    id: Optional[int] = None
    name: str
    bit_value: int
    state: _c.StateCodes = _c.StateCodes.ACTIVE
    # @classmethod
    # def from_enum(cls, generic_permission_bit: _c.GenericPermissionBits) -> "GenericPermissionBits":
    #     return cls(
    #         id=None,
    #         name=generic_permission_bit.name,
    #         description=None,
    #         bit_value=generic_permission_bit.value,
    #     )

    def get_id(self) -> int:
        return self.id

    def set_id(self, value: int) -> None:
        self.id = value

    def get_name(self) -> str:
        return self.name

    def set_name(self, value: str) -> None:
        self.name = value

    def get_bit_value(self) -> int:
        return self.bit_value

    def set_bit_value(self, value: int) -> None:
        self.bit_value = value

    def get_state(self) -> _c.StateCodes:
        return self.state

    def set_state(self, value: _c.StateCodes) -> None:
        self.state = value

class Context(BaseModel):
    id: Optional[int] = None
    name: str
    description: Optional[str] = None
    generic_permission_bits: Optional[OrderedDict[int, GenericPermissionBits]] = None

    # state code, does it make sense to soft delete these?

    def get_id(self) -> int:
        return self.id

    def set_id(self, value: int) -> None:
        self.id = value

    def get_name(self) -> str:
        return self.name

    def set_name(self, value: str) -> None:
        self.name = value

    def get_description(self) -> Optional[str]:
        return self.description

    def set_description(self, value: Optional[str]) -> None:
        self.description = value

    def get_generic_permission_bits(self) -> Optional[OrderedDict[int, GenericPermissionBits]]:
        return self.generic_permission_bits

    def set_generic_permission_bits(self, value: OrderedDict[int, GenericPermissionBits]) -> None:
        self.generic_permission_bits = value

    def add_generic_permission_bit(self, generic_permission_bit: GenericPermissionBits) -> None:
        if self.generic_permission_bits is None:
            self.generic_permission_bits = OrderedDict()
        self.generic_permission_bits[generic_permission_bit.get_bit_value()] = generic_permission_bit

class Condition(BaseModel):
    name: str
    description: str
    expression: Any # JSON or STRING TRIPLE
    state_code: _c.StateCodes = _c.StateCodes.ACTIVE
    #context: Context
    id: Optional[int] = None

    def get_name(self) -> str:
        return self.name

    def set_name(self, value: str) -> None:
        self.name = value

    def get_description(self) -> Optional[str]:
        return self.description

    def set_description(self, value: str) -> None:
        self.description = value

    def get_expression(self) -> Any:
        return self.expression

    def set_expression(self, value: Any) -> None:
        self.expression = value

    def get_state_code(self) -> _c.StateCodes:
        return self.state_code

    def set_state_code(self, value: _c.StateCodes) -> None:
        self.state_code = value

    # def get_context(self) -> Optional[Context]:
    #     return self.context

    # def set_context(self, value: Context) -> None:
    #     self.context = value

    def get_id(self) -> Optional[int]:
        return self.id

    def set_id(self, value: int) -> None:
        self.id = value

class AbstractPermission(BaseModel):
    code: str
    label: Optional[str] = None
    description: Optional[str] = None
    state_code: _c.StateCodes = _c.StateCodes.ACTIVE
    id: Optional[int] = None

    def get_code(self) -> str:
        return self.code

    def set_code(self, value: str) -> None:
        self.code = value

    def get_label(self) -> str:
        return self.label

    def set_label(self, value: str) -> None:
        self.label = value

    def get_description(self) -> str:
        return self.description

    def set_description(self, value: str) -> None:
        self.description = value

    def get_state_code(self) -> _c.StateCodes:
        return self.state_code

    def set_state_code(self, value: _c.StateCodes) -> None:
        self.state_code = value

    def get_id(self) -> Optional[int]:
        return self.id

    def set_id(self, value: Optional[int]) -> None:
        self.id = value

class Permission(AbstractPermission):
    definition: Any = None
    #context: Optional[Context] = None
    #                                    (context_name,(bit_value, generic_permission_bit))
    permission_bits: Optional[OrderedDict[str, OrderedDict[int, GenericPermissionBits]]] = None
    
    @staticmethod
    def get_type_code():
        return _c.PermissionTypeCodes.PERMISSION

    def get_definition(self) -> Any:
        return self.definition

    def set_definition(self, value: Any) -> None:
        self.definition = value

    # def get_context(self) -> Context:
    #     return self.context
    
    # def set_context(self, value: Context) -> None:
    #     self.context = value

    def get_permission_bits(self) -> OrderedDict[str, OrderedDict[int, GenericPermissionBits]]:
        return self.permission_bits

    def get_permission_bits_by_context_name(self, context_name) -> Optional[OrderedDict[int, GenericPermissionBits]]:
        return self.permission_bits.get(context_name) if self.permission_bits else None

    def set_permission_bits(self, value: OrderedDict[int, GenericPermissionBits], context: Context) -> None:
        if self.permission_bits is None:
            self.permission_bits = OrderedDict()
        self.permission_bits[context.get_name()] = value

    # Not used for now, could be tested and used if necessary
    # def add_permission_bits(self, generic_permission_bit: GenericPermissionBits, context: Context) -> None:
    #     if self.permission_bits is None:
    #         self.permission_bits = OrderedDict()
    #     context_bits = self.permission_bits.get(context.get_name())
    #     if context_bits is None:
    #         context_bits = OrderedDict()
    #         self.permission_bits[context.get_name()] = context_bits
    #     context_bits[generic_permission_bit.get_bit_value()] = generic_permission_bit

class Role(AbstractPermission):
    permissions: OrderedDict[str, AbstractPermission] = field(default_factory=OrderedDict)

    @staticmethod
    def get_type_code():
        return _c.PermissionTypeCodes.ROLE

    def post_init__(self):
        if not self.permissions:
            self.permissions = OrderedDict()

    def get_permissions(self) -> OrderedDict[str, AbstractPermission]:
        return self.permissions

    def set_permissions(self, value: OrderedDict[str, AbstractPermission]) -> None:
        self.permissions = value
        
    def add_permission(self, permission: AbstractPermission) -> None:
        self.permissions[permission.name] = permission
    
    def get_permission_by_name(self, name: str) -> Optional[AbstractPermission]:
        return self.permissions.get(name)
    
    def remove_permission_by_name(self, name: str) -> None:
        if name in self.permissions:
            del self.permissions[name]

# TODO a validator to ensure that id actually starts with group
class GroupPrincipal(_pbm.Principal):
    # keep a fixed type code and prevent changes both via direct attribute
    type_code: _c.PrincipalTypes = _c.PrincipalTypes.GROUP
    
    #                             (name, child_principal)
    children: Optional[OrderedDict[str, _pbm.Principal]] = None
    #asset_permission: Optional[Dict[Asset, list[PermissionCondition]]] = None

    def __init__(self, *args, **kwargs):
        # pop children so it's not forwarded to base Principal.__init__
        children = kwargs.pop("children", None)
        kwargs.pop("type_code", None)   # <- prevent base __init__ from assigning type_code
        kwargs.pop("type", None) 
        super().__init__(*args, **kwargs)
        # ensure deterministic container
        self.type_code = _c.PrincipalTypes.GROUP
        self.children = children if children is not None else OrderedDict()

    # Created on the todo above, but does not work due to Pydantic limitations with inheritance
    # @field_validator('principal_id', mode='before') 
    # @classmethod
    # def validate_id_on_update(cls, id_value):
    #     if id_value is None:
    #         return id_value 

    #     if not isinstance(id_value, str):
    #         # Let Pydantic type check handle this, or raise appropriate error
    #         return id_value 

    #     if not id_value.lower().startswith(_c.PrincipalTypes.GROUP.name.lower()):
    #         raise ValueError("GroupPrincipal id must start with 'group'")

    #     return id_value

    def get_children(self) -> Optional[OrderedDict[str, _pbm.Principal]]:
        return self.children

    def set_children(self, value: Optional[OrderedDict[str, _pbm.Principal]]) -> None:
        self.children = value

    def add_child(self, child: _pbm.Principal) -> None:
        if self.children is None:
            self.children = OrderedDict()
        self.children[child.name] = child
        
    def remove_child_by_name(self, child_name: str) -> None:
        if self.children and child_name in self.children:
            del self.children[child_name]

    # also override the setter used elsewhere to be defensive
    def set_type_code(self, value: _c.PrincipalTypes) -> None:
        raise AttributeError("type_code is immutable for GroupPrincipal")

class PolicyBinding(BaseModel):
    """
    A single "statement" that binds Principals, Grants, and Assets.
    """
    # The action_type (e.g., ALLOW, DENY)
    effect: _c.PermissionActionTypes 
    
    # "Who" this binding applies to (list of Users or Groups)
    principals: OrderedDict[UUID, _pbm.Principal]
    
    # "What" is being granted (list of Roles or atomic Permissions)
    #                       (permission_code, permission)
    permissions: Optional[OrderedDict[str, AbstractPermission]] = field(default_factory=OrderedDict)
    
    # "On What" asset this binding applies
    asset: _abm.Asset
    
    # calculated mask of all permission bits in the binding
    bit_mask: Optional[int] = None
    
    # Context of the binding
    context: Context

    # In which condition this binding is valid (optional)
    condition: Optional[Condition] = None

    def get_effect(self) -> _c.PermissionActionTypes:
        return self.effect

    def set_effect(self, value: _c.PermissionActionTypes) -> None:
        self.effect = value

    def get_principals(self) -> OrderedDict[UUID, _pbm.Principal]:
        return self.principals

    def set_principals(self, value: OrderedDict[UUID, _pbm.Principal]) -> None:
        self.principals = value

    def get_permissions(self) -> OrderedDict[str, AbstractPermission]:
        return self.permissions

    def set_permissions(self, value: OrderedDict[str, AbstractPermission]) -> None:
        self.permissions = value
        self.calculate_bit_mask()

    def get_asset(self) -> _abm.Asset:
        return self.asset

    def set_asset(self, value: _abm.Asset) -> None:
        self.asset = value

    def get_context(self) -> Context:
        return self.context

    def set_context(self, value: Context) -> None:
        self.context = value

    def get_condition(self) -> Optional[Condition]:
        return self.condition

    def set_condition(self, value: Optional[Condition]) -> None:
        self.condition = value

    def calculate_bit_mask(self) -> Optional[int]:
        # calculate bit mask from permissions
        bit_mask = 0
        for permission in self.permissions.values():
            if isinstance(permission, Permission) and permission.get_permission_bits():
                for bit_value in permission.get_permission_bits().keys():
                    bit_mask |= bit_value
        self.bit_mask = bit_mask

    def add_permission(self, permission: AbstractPermission) -> None:
        pass
        # add permission and recalculate or increase the bit mask

    def get_bit_mask(self) -> Optional[int]:
        if self.bit_mask is None:
            self.calculate_bit_mask()
        return self.bit_mask

class Policy(BaseModel):
    """
    A container for a set of bindings.
    """
    name: Optional[str] = None
    description: Optional[str] = None
    bindings: list[PolicyBinding]

    def get_name(self) -> str:
        return self.name

    def set_name(self, value: str) -> None:
        self.name = value

    def get_description(self) -> str:
        return self.description

    def set_description(self, value: str) -> None:
        self.description = value

    def get_bindings(self) -> list[PolicyBinding]:
        return self.bindings

    def set_bindings(self, value: list[PolicyBinding]) -> None:
        self.bindings = value

