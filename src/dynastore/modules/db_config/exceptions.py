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
This module defines a hierarchy of custom exceptions for handling specific
database errors, allowing the application to "fail fast" and provide
meaningful feedback to the user or calling service.
"""

class DatabaseError(Exception):
    """Base class for all custom database-related exceptions."""
    def __init__(self, message, original_exception=None):
        super().__init__(message)
        self.original_exception = original_exception
        self.details = str(original_exception) if original_exception else "No additional details."

    def __str__(self):
        return f"{super().__str__()} (Details: {self.details})"

class QueryExecutionError(DatabaseError):
    """Raised for general or unrecognized errors during query execution."""
    pass

class ResourceNotFoundError(DatabaseError):
    """Raised when a requested resource (record, file, etc.) is not found."""
    pass

class LogicalCollectionError(DatabaseError):
    """Raised when an operation that requires a physical data table is attempted on a logical-only collection."""
    pass
class ImmutableConfigError(ValueError): # Already inherits from ValueError, which is good.
    """Raised when an attempt is made to modify an immutable configuration field."""
    pass

class PluginNotRegisteredError(KeyError):
    """Raised when a configuration plugin_id is not found in the registry."""
    pass

class ConfigValidationError(ValueError):
    """Raised when a configuration body fails Pydantic validation."""
    pass

class InternalValidationError(ValueError):
    """Raised when an internal validation fails (e.g. unknown properties)."""
    pass
class DatabaseConnectionError(DatabaseError):
    """Raised when the connection to the database cannot be established or is lost."""
    pass

# --- Specific PostgreSQL Errors based on pgcode ---

class TableNotFoundError(DatabaseError):
    """Raised when a query references a table that does not exist (pgcode: 42P01)."""
    pass


class SchemaNotFoundError(DatabaseError):
    """Raised when a query references a schema that does not exist (pgcode: 3F000)."""
    pass


class DuplicateTableError(DatabaseError):
    """Raised when attempting to create a table that already exists (pgcode: 42P07)."""
    pass

class DuplicateObjectError(DatabaseError):
    """Raised when attempting to create an object that already exists (pgcode: 42710)."""
    pass

class PermissionDeniedError(DatabaseError):
    """Raised when the database user has insufficient privileges (pgcode: 42501)."""
    pass


class UniqueViolationError(DatabaseError):
    """Raised on violation of a unique constraint (pgcode: 23505)."""
    pass


class ForeignKeyViolationError(DatabaseError):
    """Raised on violation of a foreign key constraint (pgcode: 23503)."""
    pass


# Mapping from PostgreSQL error codes (pgcode) to our custom exception classes.
# See: https://www.postgresql.org/docs/current/errcodes-appendix.html
PGCODE_EXCEPTION_MAP = {
    '42P01': TableNotFoundError,
    '3F000': SchemaNotFoundError,
    '42P07': DuplicateTableError,
    '42501': PermissionDeniedError,
    '42710': DuplicateObjectError,
    '23505': UniqueViolationError,
    '23503': ForeignKeyViolationError,
    # Codes for connection issues
    '08000': DatabaseConnectionError,
    '08003': DatabaseConnectionError,
    '08006': DatabaseConnectionError,
}

# --- REST API Conflict Detection Utilities ---

def is_conflict_error(exc: Exception) -> bool:
    """
    Check if an exception represents a data conflict (HTTP 409 Conflict status).
    
    Identifies constraint violation and duplicate key errors that should be
    reported to REST API clients as 409 Conflict responses.
    """
    from sqlalchemy.exc import IntegrityError as SqlIntegrityError
    return isinstance(exc, (UniqueViolationError, ForeignKeyViolationError, DuplicateObjectError, SqlIntegrityError))


def get_conflict_context(exc: Exception) -> dict:
    """
    Extract context information from a database exception for REST API error responses.
    
    Analyzes database exceptions and provides comprehensive context information
    for generating HTTP 409 responses, including exception type, pgcode, and
    the original error message.
    
    Args:
        exc: The database exception to analyze
    
    Returns:
        Dictionary with keys:
        - 'is_conflict' (bool): True if this is a conflict-type error
        - 'error_type' (str): Class name of the exception
        - 'message' (str): Exception message
        - 'pgcode' (str|None): PostgreSQL error code if available
    """
    context = {
        'is_conflict': is_conflict_error(exc),
        'error_type': exc.__class__.__name__,
        'message': str(exc),
        'pgcode': None
    }
    
    # Try to extract pgcode if available through original exception chain
    original_exc = getattr(exc, 'original_exception', None)
    if original_exc:
        orig_db_exc = getattr(original_exc, 'orig', None)
        if orig_db_exc:
            pgcode = getattr(orig_db_exc, 'pgcode', None)
            if pgcode:
                context['pgcode'] = pgcode
    
    return context
