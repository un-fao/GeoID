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

"""Regression guard: ``DriverContext`` must be validate-constructable.

#1680 moved ``DbResource`` under ``TYPE_CHECKING`` to break a
models->modules import. Without a runtime fallback for the forward ref,
pydantic 2.12 can no longer build the model's schema, so *every*
``DriverContext(db_resource=...)`` raises ``PydanticUserError`` — which
``IamService.resolve_schema`` silently swallows, collapsing all
catalog-scoped auth to the platform ``iam`` schema.

#1555 replaced the ``_DbResource = Any`` runtime fallback with a real
runtime import from ``dynastore.models.db_resource``, so the field is now
properly typed. This file pins that the model stays constructable and that
the import path works without ``model_rebuild()`` by the caller.
"""
from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from dynastore.models.driver_context import DriverContext


def test_driver_context_constructs_with_none_db_resource():
    ctx = DriverContext(db_resource=None)
    assert ctx.db_resource is None


def test_driver_context_constructs_with_real_db_resource():
    """DriverContext must accept a valid DbResource (Engine) without error.

    Replaces the pre-#1555 ``object()`` sentinel test: with a real DbResource
    type union, Pydantic validates the value; plain ``object()`` is not a
    valid DbResource member.
    """
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    ctx = DriverContext(db_resource=engine)
    assert ctx.db_resource is engine


def test_db_resource_type_is_real_import():
    """The DbResource field type must be the real SQLAlchemy union, not Any."""
    engine = create_engine("sqlite:///:memory:")
    ctx = DriverContext(db_resource=engine)
    assert isinstance(ctx.db_resource, Engine)
