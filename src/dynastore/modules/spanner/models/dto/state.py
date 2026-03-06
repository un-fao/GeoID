import dynastore.modules.spanner.models.business.asset as _abm
from pydantic import BaseModel

class StateDTO(BaseModel):
    id: int
    name: str

def state_to_dto(state: _abm.State) -> StateDTO:
    return StateDTO(
        id=state.get_id(),
        name=state.get_name()
    )