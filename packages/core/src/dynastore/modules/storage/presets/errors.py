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

"""Domain exceptions for the preset lifecycle layer.

These replace direct ``HTTPException`` raises so the core module stays
free of FastAPI / Starlette imports.  The extension boundary in
``extensions/tools/exception_handlers.py`` maps each class to the
correct HTTP status code and preserves the original detail payload.
"""
from __future__ import annotations

from typing import Union


class PresetConflictError(Exception):
    """Raised when a preset lifecycle operation cannot proceed due to a
    state or parameter conflict.  Maps to HTTP 409 Conflict.

    ``detail`` may be a plain string or a structured dict — the handler
    preserves whatever is passed here as the ``HTTPException.detail``.
    """

    def __init__(self, detail: Union[str, dict]) -> None:
        super().__init__(str(detail) if isinstance(detail, str) else repr(detail))
        self.detail: Union[str, dict] = detail


class PresetOperationError(Exception):
    """Raised when the preset's own ``apply`` or ``revoke`` call raises an
    unexpected exception.  Maps to HTTP 500 Internal Server Error.

    ``detail`` carries a structured dict:
    ``{"message": "...", "error": "<truncated traceback>"}``.
    """

    def __init__(self, detail: dict) -> None:
        super().__init__(detail.get("message", "Preset operation failed."))
        self.detail: dict = detail


class PresetNotFoundError(Exception):
    """Raised when a preset audit row is not found for the requested operation.
    Maps to HTTP 404 Not Found.
    """

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail: str = detail


class ServiceUnavailableError(Exception):
    """Raised when a required service dependency (e.g. the database engine)
    is not available at the time of the call.  Maps to HTTP 503
    Service Unavailable.
    """

    def __init__(self, detail: str = "Service unavailable.") -> None:
        super().__init__(detail)
        self.detail: str = detail
