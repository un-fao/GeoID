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

from enum import Enum
from pydantic import BaseModel, Field, field_validator, ConfigDict
from typing import List, Optional, Dict, Any, Union

class FilterOperator(str, Enum):
    """Enumeration of supported filter operators.

    Each member has a descriptive value (e.g. ``"eq"``, ``"bbox"``) that is
    used for serialisation/deserialisation. Call ``to_sql()`` to get the
    corresponding SQL token (e.g. ``"="``, ``"&&"``, ``"ST_Intersects"``).
    """

    EQ = "eq"
    NE = "ne"
    NEQ = "neq"   # alias for NE
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
    LIKE = "like"
    ILIKE = "ilike"
    IN = "in"
    NIN = "nin"
    IS_NULL = "isnull"
    IS_NOT_NULL = "isnotnull"

    # Spatial Operators (rendered as ST_* functions)
    INTERSECTS = "intersects"
    DISJOINT = "disjoint"
    TOUCHES = "touches"
    OVERLAPS = "overlaps"
    CROSSES = "crosses"
    WITHIN = "within"
    CONTAINS = "contains"
    DWITHIN = "dwithin"
    BEYOND = "beyond"
    BBOX = "bbox"

    # Range / PostGIS infix Operators
    RANGE_CONTAINS = "@>"
    RANGE_WITHIN = "<@"
    RANGE_OVERLAPS = "&&"

    def to_sql(self) -> str:
        """Return the SQL operator/function token for this member."""
        return _FILTER_OPERATOR_SQL_MAP.get(self.value, self.value)

    @property
    def is_spatial(self) -> bool:
        """True for operators that render as ST_* functions."""
        return self.to_sql().upper().startswith("ST_")

    @property
    def is_range(self) -> bool:
        """True for PostgreSQL range / infix operators."""
        return self in (
            FilterOperator.RANGE_CONTAINS,
            FilterOperator.RANGE_WITHIN,
            FilterOperator.RANGE_OVERLAPS,
        )

    @property
    def needs_numeric_cast(self) -> bool:
        """True when a text JSONB accessor needs ``::numeric`` before comparison."""
        return self in (
            FilterOperator.GT, FilterOperator.GTE,
            FilterOperator.LT, FilterOperator.LTE,
        )

    @classmethod
    def from_str(cls, value: str) -> "FilterOperator":
        """Parse from a descriptive string *or* a raw SQL symbol.

        Examples::

            FilterOperator.from_str("eq")            # -> FilterOperator.EQ
            FilterOperator.from_str("ST_Intersects") # -> FilterOperator.INTERSECTS
            FilterOperator.from_str("&&")            # -> FilterOperator.RANGE_OVERLAPS
        """
        try:
            return cls(value.lower())
        except ValueError:
            sql_upper = value.upper()
            for member in cls:
                if member.to_sql().upper() == sql_upper:
                    return member
            raise ValueError(f"Unknown FilterOperator: {value!r}")


# Module-level mapping — kept outside the enum body to avoid Python treating
# dict literals as enum members when using ClassVar inside Enum subclasses.
_FILTER_OPERATOR_SQL_MAP: Dict[str, str] = {
    "eq":          "=",
    "ne":          "!=",
    "neq":         "!=",
    "gt":          ">",
    "gte":         ">=",
    "lt":          "<",
    "lte":         "<=",
    "like":        "LIKE",
    "ilike":       "ILIKE",
    "in":          "IN",
    "nin":         "NOT IN",
    "isnull":      "IS NULL",
    "isnotnull":   "IS NOT NULL",
    # Spatial — rendered as ST_* functions
    "intersects":  "ST_Intersects",
    "disjoint":    "ST_Disjoint",
    "touches":     "ST_Touches",
    "overlaps":    "ST_Overlaps",
    "crosses":     "ST_Crosses",
    "within":      "ST_Within",
    "contains":    "ST_Contains",
    "dwithin":     "ST_DWithin",
    "beyond":      "ST_Beyond",
    "bbox":        "&&",
    # Range / infix (pass-through)
    "@>":          "@>",
    "<@":          "<@",
    "&&":          "&&",
}


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
    operator: Union[str, FilterOperator]  # "=", "!=", ">", "<", "LIKE", "ST_Intersects", etc.
    value: Any
    spatial_op: bool = False


class SortOrder(BaseModel):
    """Represents sort order."""

    field: str
    direction: str = "ASC"  # "ASC" or "DESC"


class QueryRequest(BaseModel):
    """Structured query request."""

    select: List[FieldSelection] = Field(
        default_factory=lambda: [FieldSelection(field="*")]
    )
    filters: List[FilterCondition] = Field(default_factory=list)
    sort: Optional[List[SortOrder]] = None
    group_by: Optional[List[str]] = None
    limit: Optional[int] = None
    offset: Optional[int] = None

    # --- Advanced Escape Hatches for OGC / Complex Queries ---
    raw_selects: List[str] = Field(
        default_factory=list, description="Raw SQL SELECT expressions."
    )
    raw_where: Optional[str] = Field(None, description="Raw SQL WHERE expression.")
    raw_params: Dict[str, Any] = Field(
        default_factory=dict, description="Parameters for raw_where/raw_selects."
    )
    cql_filter: Optional[str] = Field(
        None, description="A raw CQL2 filter string to be parsed and validated."
    )
    include_total_count: bool = Field(
        False, description="If True, includes COUNT(*) OVER() as _total_count."
    )

    @field_validator("select")
    @classmethod
    def validate_select(cls, v):
        if not v:
            return [FieldSelection(field="*")]
        return v


class QueryResponse(BaseModel):
    """
    Context wrapper for query results.
    Encapsulates the data stream along with the configuration and metadata used to generate it.
    """

    items: Any = Field(..., description="The result iterator (AsyncIterator) or list")
    total_count: Optional[int] = Field(
        None, description="Total count of items matching the filter (if requested)"
    )

    # Contextual Configs (to avoid re-fetching)
    catalog_id: str
    collection_id: str
    collection_config: Optional[Any] = Field(
        None, description="The resolved CollectionPluginConfig"
    )

    # Execution Metadata
    execution_params: Dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __aiter__(self):
        """Allows transparent iteration over the items stream."""
        return self.items.__aiter__()
