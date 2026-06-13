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

from typing import TYPE_CHECKING

__all__ = [
    "ElasticsearchModule",
]

if TYPE_CHECKING:
    from .module import ElasticsearchModule


def __getattr__(name: str):
    # Lazy re-export. ElasticsearchModule pulls the opensearch client at import
    # time, so importing a dependency-light submodule of this package (e.g.
    # ``items_projection`` for the pure ``strip_reserved_members`` helper used by
    # the OGC Features extension) must NOT eagerly drag ``opensearchpy`` in via
    # this package ``__init__``. Resolve the module only on explicit attribute
    # access so packages without the elasticsearch extras can still import the
    # helper submodules.
    if name == "ElasticsearchModule":
        from .module import ElasticsearchModule

        return ElasticsearchModule
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
