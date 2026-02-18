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

from typing import Dict, Any, Union, Optional, List
from pydantic import BaseModel, Field, field_validator
from dynastore.models.localization import LocalizedText


class WebPageConfig(BaseModel):
    id: str
    title: LocalizedText
    icon: str = "fa-circle"
    description: Optional[LocalizedText] = None

    @field_validator("title", "description", mode="before")
    @classmethod
    def convert_to_localized_text(cls, v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, str):
            return LocalizedText(en=v)
        if isinstance(v, dict):
            # Check if it's already compatible or needs conversion
            return LocalizedText(**v)
        if isinstance(v, LocalizedText):
            return v
        return v


class StaticProviderConfig(BaseModel):
    prefix: str
    files: list[str] = Field(default_factory=list)


class DocItem(BaseModel):
    id: str
    title: str
    path: str
    category: str
