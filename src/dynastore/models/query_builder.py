#    Copyright 2025 FAO
# 
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
# 
#        http://www.apache.org/licenses/LICENSE-2.0
# 
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
# 
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Dict, Any, Union

class FieldSelection(BaseModel):
    """Represents a field to select with optional transformation/aggregation."""
    field: str
    alias: Optional[str] = None
    aggregation: Optional[str] = None  # "count", "sum", "ST_Union", etc.
    transformation: Optional[str] = None  # "ST_AsGeoJSON", "upper", etc.
    transform_args: Dict[str, Any] = {}  # Arguments for transformation

class FilterCondition(BaseModel):
    """Represents a filter condition."""
    field: str
    operator: str  # "=", "!=", ">", "<", "LIKE", "ST_Intersects", etc.
    value: Any
    spatial_op: bool = False

class SortOrder(BaseModel):
    """Represents sort order."""
    field: str
    direction: str = "ASC"  # "ASC" or "DESC"

class QueryRequest(BaseModel):
    """Structured query request."""
    select: List[FieldSelection] = Field(default_factory=lambda: [FieldSelection(field="*")])
    filters: List[FilterCondition] = Field(default_factory=list)
    sort: Optional[List[SortOrder]] = None
    group_by: Optional[List[str]] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    
    # --- Advanced Escape Hatches for OGC / Complex Queries ---
    raw_selects: List[str] = Field(default_factory=list, description="Raw SQL SELECT expressions.")
    raw_where: Optional[str] = Field(None, description="Raw SQL WHERE expression.")
    raw_params: Dict[str, Any] = Field(default_factory=dict, description="Parameters for raw_where/raw_selects.")
    include_total_count: bool = Field(False, description="If True, includes COUNT(*) OVER() as _total_count.")
    
    @field_validator('select')
    @classmethod
    def validate_select(cls, v):
        if not v:
            return [FieldSelection(field="*")]
        return v