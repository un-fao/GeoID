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

"""Regression guard: ``DriverContext`` must be validate-constructable.

#1680 moved ``DbResource`` under ``TYPE_CHECKING`` to break a
models→modules import. Without a runtime fallback for the forward ref,
pydantic 2.12 can no longer build the model's schema, so *every*
``DriverContext(db_resource=...)`` raises ``PydanticUserError`` — which
``IamService.resolve_schema`` silently swallows, collapsing all
catalog-scoped auth to the platform ``iam`` schema. This pins that the
model stays constructable with no explicit ``model_rebuild()`` call by the
caller.
"""
from __future__ import annotations

from dynastore.models.driver_context import DriverContext


def test_driver_context_constructs_with_none_db_resource():
    ctx = DriverContext(db_resource=None)
    assert ctx.db_resource is None


def test_driver_context_constructs_with_arbitrary_db_resource():
    sentinel = object()
    ctx = DriverContext(db_resource=sentinel)
    assert ctx.db_resource is sentinel
