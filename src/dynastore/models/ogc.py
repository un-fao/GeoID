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

from typing import List, Optional, Dict, Any, Literal, Union
from pydantic import BaseModel
from dynastore.models.shared_models import Link

class GeoJSONGeometry(BaseModel):
    """Simple GeoJSON geometry model for validation and serialization."""
    type: str
    coordinates: Any

class Feature(BaseModel):
    type: Literal["Feature"] = "Feature"
    id: Union[str, int, None] = None
    geometry: Optional[GeoJSONGeometry] = None
    properties: Dict[str, Any]
    links: Optional[List[Link]] = None

class FeatureCollection(BaseModel):
    type: Literal["FeatureCollection"] = "FeatureCollection"
    features: List[Feature]
    links: Optional[List[Link]] = None
    timeStamp: Optional[str] = None
    numberMatched: Optional[int] = None
    numberReturned: Optional[int] = None
