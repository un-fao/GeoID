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

"""Template-placeholder guard for ``validate_sql_identifier`` (issue #1191).

A client that issues a request against ``/catalogs/{{m.catalog}}/...`` without
substituting the placeholder otherwise sends the literal token down to the
routing resolver, where it surfaces as an opaque ``routed-resolve unavailable``
lookup miss.  ``validate_sql_identifier`` rejects it up front with an
actionable message; the global ``InvalidIdentifierError`` handler maps that to
HTTP 400.
"""

from __future__ import annotations

import pytest

from dynastore.tools.db import InvalidIdentifierError, validate_sql_identifier


@pytest.mark.parametrize(
    "templated",
    [
        "{{m.catalog}}",
        "{{ m.catalog }}",
        "prefix-{{catalog_id}}",
        "}}orphan",
        "{{open",
    ],
)
def test_templated_identifier_is_rejected_with_actionable_message(templated: str) -> None:
    with pytest.raises(InvalidIdentifierError) as excinfo:
        validate_sql_identifier(templated)

    message = str(excinfo.value).lower()
    assert "template placeholder" in message
    assert "substitute" in message
    # The offending value is echoed back so the client can spot the typo.
    assert templated in str(excinfo.value)


@pytest.mark.parametrize("identifier", ["fao-asis", "my_catalog", "cat1", "_x", "a.b"])
def test_well_formed_identifier_still_passes(identifier: str) -> None:
    assert validate_sql_identifier(identifier) == identifier.lower()
