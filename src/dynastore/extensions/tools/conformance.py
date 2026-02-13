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

# dynastore/tools/conformance.py

import logging
from typing import List, Set
from pydantic import BaseModel

logger = logging.getLogger(__name__)

# --- Data Model ---

class Conformance(BaseModel):
    conformsTo: List[str]

# --- In-Memory Registry ---

_REGISTERED_URIS: Set[str] = set()

# --- Public API for Registration and Retrieval ---

def register_conformance_uris(uris: List[str]):
    """
    Allows a DynaStore extension to register its supported conformance classes.
    This is called by each extension once at application startup.
    """
    _REGISTERED_URIS.update(uris)
    logger.info(f"Registered {len(uris)} new conformance URIs.")

def get_active_conformance() -> Conformance:
    """
    Returns the dynamically-built list of all registered conformance classes.
    """
    return Conformance(conformsTo=sorted(list(_REGISTERED_URIS)))
