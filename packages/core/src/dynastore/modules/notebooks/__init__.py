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

from .notebooks_module import NotebooksModule
from .models import NotebookCreate, Notebook, NotebookBase, PlatformNotebookCreate, PlatformNotebook, OwnerType
from .notebooks_db import init_notebooks_storage
from .example_registry import register_platform_notebook, get_registered_notebooks

# Built-in notebook registrations live in module-specific submodules
# (catalog/storage/elasticsearch/ingestion). They are loaded by
# NotebooksModule.lifespan() so SCOPE-trimmed Cloud Run Job images that lack
# a module's heavy deps (e.g. shapely via storage) can still import this
# package safely.

__all__ = [
    "NotebooksModule",
    "init_notebooks_storage",
    "NotebookCreate",
    "Notebook",
    "NotebookBase",
    "PlatformNotebookCreate",
    "PlatformNotebook",
    "OwnerType",
    "register_platform_notebook",
    "get_registered_notebooks",
]
