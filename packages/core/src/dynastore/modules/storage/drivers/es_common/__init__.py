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

"""Shared Elasticsearch helpers used by both ES items drivers (public + private).

The CQL2→ES translator (:mod:`.cql_to_es`) lets the STAC ``/search`` dispatch
serve CQL2 attribute/spatial/temporal filters from Elasticsearch instead of
falling back to PostgreSQL, reusing the queryables SSOT to map property names to
ES field paths.
"""

from dynastore.modules.storage.drivers.es_common.cql_to_es import (
    UntranslatableFilterError,
    build_es_field_mapping,
    cql_ast_to_es_query,
    merge_es_filter,
)

__all__ = [
    "UntranslatableFilterError",
    "build_es_field_mapping",
    "cql_ast_to_es_query",
    "merge_es_filter",
]
