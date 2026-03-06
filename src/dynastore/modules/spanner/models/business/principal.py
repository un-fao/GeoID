from dataclasses import dataclass, field
from typing import  Optional
from uuid import UUID, uuid4
from abc import ABC
import datetime
import dynastore.modules.spanner.constants as _c
from pydantic import BaseModel

@dataclass
class Principal(ABC):
    uuid: UUID
    principal_id : str = None # TODO should be made mandatory, keeping it optional to not fail existing stuff
    display_name: Optional[str] = None
    type_code: _c.PrincipalTypes = None
    state_code: _c.StateCodes = None  # field(default_factory=Enum)
    email: Optional[str] = None
    creation_time: Optional[datetime.datetime] = None
    description: Optional[str] = None
    metadata: Optional[dict] = None

    # Getter and setter for uuid
    def get_uuid(self) -> UUID:
        return self.uuid

    def set_uuid(self, value: UUID):
        self.uuid = value

    def get_principal_id(self) -> Optional[str]:
        return self.principal_id
    
    def set_principal_id(self, value: Optional[str]):
        self.principal_id = value

    def get_type_code(self) -> _c.PrincipalTypes:
        return self.type_code
    
    def set_type_code(self, value: _c.PrincipalTypes):
        self.type_code = value

    def get_email(self) -> Optional[str]:
        return self.email
    
    def set_email(self, value: Optional[str]):
        self.email = value

    # Getter and setter for display_name
    def get_display_name(self) -> str:
        return self.display_name

    def set_display_name(self, value: str):
        self.display_name = value

    def get_state_code(self) -> _c.StateCodes:
        if not self.state_code:
            return _c.StateCodes.ACTIVE
        return self.state_code
    
    def get_creation_time(self) -> datetime.datetime:
        return self.creation_time
    
    def set_creation_time(self, value: datetime.datetime):
        self.creation_time = value

    def get_description(self) -> Optional[str]:
        return self.description

    def set_description(self, value: Optional[str]):
        self.description = value

    def get_metadata(self) -> Optional[dict]:
        return self.metadata

    def set_metadata(self, value: Optional[dict]):
        self.metadata = value