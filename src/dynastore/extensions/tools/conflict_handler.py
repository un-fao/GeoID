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

"""
Centralized conflict/constraint violation handling for REST APIs.

This module provides utilities to convert low-level database constraint violations
(unique constraint, foreign key, duplicate object) into HTTP 409 Conflict responses.
"""

import logging
from typing import Optional, Union
from fastapi import HTTPException, status
from sqlalchemy.exc import IntegrityError

from dynastore.modules.db_config.exceptions import (
    UniqueViolationError,
    ForeignKeyViolationError,
    DuplicateObjectError,
    DatabaseError,
    PGCODE_EXCEPTION_MAP,
)

logger = logging.getLogger(__name__)


def conflict_to_409(
    error: Union[DatabaseError, IntegrityError],
    resource_name: str = "Resource",
    resource_id: Optional[str] = None
) -> HTTPException:
    """
    Convert database constraint violation to HTTP 409 Conflict response.
    
    Handles both raw SQLAlchemy IntegrityError and custom DatabaseError exceptions.
    Automatically generates appropriate error messages based on exception type.
    
    Args:
        error: The database exception (IntegrityError or DatabaseError subclass)
        resource_name: Name of the resource (e.g., "Catalog", "Collection", "Asset")
        resource_id: ID/code of the resource (optional, improves error message)
    
    Returns:
        HTTPException with 409 Conflict status code
    
    """
    # Convert raw IntegrityError if needed
    if isinstance(error, IntegrityError):
        pgcode = getattr(error.orig, 'pgcode', None) if error.orig else None
        if pgcode and pgcode in PGCODE_EXCEPTION_MAP:
            error = PGCODE_EXCEPTION_MAP[pgcode](str(error.orig), original_exception=error)
        else:
            error = DuplicateObjectError(str(error.orig), original_exception=error)
    
    # Generate message based on exception type
    if isinstance(error, (UniqueViolationError, DuplicateObjectError)):
        detail = f"{resource_name} '{resource_id}' already exists." if resource_id else f"{resource_name} already exists."
    elif isinstance(error, ForeignKeyViolationError):
        detail = f"Cannot create {resource_name} '{resource_id}': referenced resource does not exist." if resource_id else "Cannot create resource: referenced resource does not exist."
    else:
        detail = f"Conflict creating {resource_name}." if resource_name else "Resource conflict."
    
    logger.warning(f"Conflict (409): {detail} [Original: {error}]")
    return HTTPException(status_code=status.HTTP_409_CONFLICT, detail=detail)
