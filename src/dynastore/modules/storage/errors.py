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

"""Storage module error types."""


class ReadOnlyDriverError(Exception):
    """Write attempted on a read-only driver."""


class SoftDeleteNotSupportedError(Exception):
    """Soft delete requested but driver doesn't support it."""


class ConflictError(Exception):
    """Write refused because identity resolution matched an existing entity
    and the collection write policy rejects the write (``REFUSE_FAIL``).

    Carries the matched ``geoid`` and the ``matcher`` name that triggered the
    conflict so callers can surface actionable diagnostics (HTTP 409 body,
    batch error reports).
    """

    def __init__(
        self,
        message: str,
        *,
        geoid: object | None = None,
        matcher: str | None = None,
    ) -> None:
        super().__init__(message)
        self.geoid = geoid
        self.matcher = matcher
