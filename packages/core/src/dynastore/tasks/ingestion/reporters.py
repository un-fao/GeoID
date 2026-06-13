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

from typing import Dict, Type

from dynastore.tasks.reporters import ReportingInterface
from dynastore.tools.typed_store._snake import to_snake

_ingestion_reporter_registry: Dict[str, Type[ReportingInterface]] = {}


def ingestion_reporter(cls: Type[ReportingInterface]) -> Type[ReportingInterface]:
    """Register a reporter under its snake_case identity.

    Mirrors ``PersistentModel.class_key()`` — ``GcsDetailedReporter`` →
    ``gcs_detailed_reporter`` — so reporter ids share the convention used by
    every other typed registration in the codebase (PluginConfig, drivers).
    Imports ``to_snake`` from the leaf module ``typed_store._snake`` (no
    project imports) to keep this file off the ``PluginConfig`` registration
    chain that would otherwise circular-import via ``typed_store.base``.
    """
    reporter_name = to_snake(cls.__name__)
    if reporter_name in _ingestion_reporter_registry:
        raise TypeError(f"Reporter with name '{reporter_name}' is already registered.")
    _ingestion_reporter_registry[reporter_name] = cls
    return cls
