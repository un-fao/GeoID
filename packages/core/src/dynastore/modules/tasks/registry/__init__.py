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

"""Durable task-capability registry: the cross-pod record of what each
deployed service can run, self-published via version-gated autodiscovery.

Pure data + IO; no routing policy (that lives in the routing config)
and no claim-path coupling (the resolver is wired into CapabilityMap elsewhere).
"""
from dynastore.modules.tasks.registry.model import (
    CapabilityRow,
    TASK_CAPABILITY_REGISTRY_TABLE,
    compute_publish_digest,
)

__all__ = [
    "CapabilityRow",
    "TASK_CAPABILITY_REGISTRY_TABLE",
    "compute_publish_digest",
]
