#    Copyright 2026 FAO
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

import json
import ast
import logging
from typing import Any, Annotated, Union
from pydantic.functional_validators import BeforeValidator

def parse_dict(value: str) -> Any:
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        parsed = ast.literal_eval(value)
        # if not isinstance(parsed, dict):
        #     raise ValueError("Input must be a dictionary")
        return parsed

# Define an annotated type that applies the parse_dict function
FlexibleDictParam = Annotated[Any, BeforeValidator(parse_dict)]

def parse_template(value: str) -> Union[str, dict]:
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        try:
            parsed = ast.literal_eval(value)
            if not isinstance(parsed, dict):
                raise ValueError("Input must be a dictionary")
            return parsed
        except Exception as e:
            logging.warning("Unable to load template as JSON (%s), using plain text", e)
        return value


TemplateParam = Annotated[Any, BeforeValidator(parse_template)]
