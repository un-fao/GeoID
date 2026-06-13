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

"""Protocol implemented by plugins that ship example notebooks.

`NotebooksModule.lifespan()` enumerates implementers via
`dynastore.tools.discovery.get_protocols(NotebookContributorProtocol)`
and seeds the returned `NotebookContribution`s into
`notebooks.platform_notebooks` (ON CONFLICT DO NOTHING).
"""
from typing import Protocol, runtime_checkable

from dynastore.modules.notebooks.contribution import NotebookContribution


@runtime_checkable
class NotebookContributorProtocol(Protocol):
    def get_notebooks(self) -> list[NotebookContribution]: ...
