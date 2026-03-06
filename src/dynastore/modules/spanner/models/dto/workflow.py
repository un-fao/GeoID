# Esempio: app/models/metadata.py
from pydantic import BaseModel, Field, ConfigDict, field_serializer
from typing import List, Optional, Any
from uuid import UUID
from dynastore.modules.spanner.models.dto.asset import Asset
import dynastore.modules.spanner.constants as _c



# class AssetWorkflow(BaseModel):
#     id: UUID = Field(..., description="Unique identifier for the lifecycle")
#     name: str = Field(..., description="Name of the workflow")
#     description: str = Field(..., description="Description of the workflow")
#     principal_uuid: UUID = Field(..., description="ID of the agent creating this workflow ")
    
class AssetWorkflowStateDTO(BaseModel):
    asset_uuid: Optional[UUID] = Field(default=None, description="Unique identifier for asset")
    asset_state_code: str = Field(...,description="Current state of the asset")
    principal_uuid: Optional[UUID] = Field(default=None, description="ID of the associated agent triggering this change")
    reason: Optional[str] = Field(default=None, description="Reason for this change")

    # TODO this might ne necessary in some other DTO's
    model_config = ConfigDict(
        use_enum_values=False
    )

    @field_serializer('asset_state_code')
    def serialize_state_code(self, v: Any):
        if hasattr(v, 'name'):
            return v.name
        return str(v)
ASSET_WORKFLOW_STATE_TABLE_NAME="asset_workflow_state"


def state_history_from_asset(asset: Asset, deep: bool) -> List[AssetWorkflowStateDTO]:
    if not asset.get_state_history():
        return None
    history = [] 
    for state in asset.get_state_history():
        history.append(state_dto_from_asset_and_state(asset=asset, state=state))
    return history

def state_dto_from_asset_and_state(asset: Asset, state: _c.AssetWorkflowStateCodes):
    return AssetWorkflowStateDTO(
        asset_uuid=asset.get_uuid(),
        # asset_workflow_id=state.get_current_state().set_workflow_type_id(),
        asset_state_code=state.get_current_state().name,
        principal_uuid=state.get_principal().get_uuid(),
        reason=state.get_reason()
    )
