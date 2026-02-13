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
Pluggable exception handling system for REST API endpoints.

Provides a centralized, extensible mechanism to handle and convert application
exceptions to appropriate HTTP responses, allowing modules to register custom
exception handlers without coupling to specific services.

Integration with FastAPI:
    1. Call setup_exception_handlers(app) during app initialization
    2. Optionally decorate endpoints with @auto_handle_exceptions
    3. Or manually call handle_exception() in try-except blocks
"""

# from dynastore.models.protocols.database import DatabaseProtocol

import logging
import functools
from typing import Callable, Optional, List, Dict, Any, Union, TypeVar, Awaitable
from fastapi import FastAPI, HTTPException, status, Response, Request
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from fastapi.responses import JSONResponse
import traceback
import json

logger = logging.getLogger(__name__)

F = TypeVar('F', bound=Callable[..., Awaitable[Any]])


class ExceptionHandler:
    """
    Base handler interface for converting application exceptions to HTTP responses.
    
    Handlers are checked in registration order. The first handler that returns
    a non-None result is used. If no handler matches, the exception is re-raised.
    """
    
    def can_handle(self, exception: Exception) -> bool:
        """Check if this handler can process the exception."""
        raise NotImplementedError
    
    def handle(self, exception: Exception, context: Optional[Dict[str, Any]] = None) -> Optional[Union[HTTPException, Response]]:
        """
        Convert the exception to an HTTPException or Response (e.g. XML), or return None if unable to handle.
        
        Args:
            exception: The exception to handle
            context: Optional context dict with keys like 'resource_name', 'resource_id', 'operation'
        
        Returns:
            HTTPException or Response with appropriate status code and detail, or None to defer to next handler
        """
        raise NotImplementedError


class ConflictExceptionHandler(ExceptionHandler):
    """Handles database constraint violations (unique, foreign key, duplicate)."""
    
    def can_handle(self, exception: Exception) -> bool:
        from dynastore.modules.db_config.exceptions import is_conflict_error
        return is_conflict_error(exception)
    
    def handle(self, exception: Exception, context: Optional[Dict[str, Any]] = None) -> Optional[HTTPException]:
        from dynastore.extensions.tools.conflict_handler import conflict_to_409
        context = context or {}
        resource_name = context.get('resource_name', 'Resource')
        resource_id = context.get('resource_id')
        return conflict_to_409(exception, resource_name=resource_name, resource_id=resource_id)


class ProgrammingErrorHandler(ExceptionHandler):
    """
    Handles low-level programming errors that indicate a server-side issue,
    like KeyError, AttributeError, or TypeError, which often point to
    misconfiguration or bugs.
    """

    def can_handle(self, exception: Exception) -> bool:
        # Catches common developer errors that shouldn't leak as raw tracebacks.
        return isinstance(exception, (KeyError, AttributeError, TypeError))

    def handle(self, exception: Exception, context: Optional[Dict[str, Any]] = None) -> Optional[HTTPException]:
        err_type = exception.__class__.__name__
        operation = (context or {}).get('operation', 'the requested operation')
        detail = f"Could not complete {operation} due to an internal server configuration error. Please contact the administrator. (Reference: {err_type})"

        logger.error(f"Internal Programming Error (500): {err_type}: {str(exception)} during {operation}", exc_info=True)

        return HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=detail
        )

class ValidationExceptionHandler(ExceptionHandler):
    """Handles validation errors (ValueError, Pydantic ValidationError)."""
    
    def can_handle(self, exception: Exception) -> bool:
        from pydantic import ValidationError
        return isinstance(exception, (ValueError, ValidationError))
    
    def handle(self, exception: Exception, context: Optional[Dict[str, Any]] = None) -> Optional[HTTPException]:
        context = context or {}
        resource_id = context.get('resource_id', '')
        operation = context.get('operation', 'Operation')
        
        if resource_id:
            detail = f"{operation} failed validation for '{resource_id}': {str(exception)}"
        else:
            detail = f"{operation} failed validation: {str(exception)}"
        
        logger.warning(detail)
        return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=detail)


class ImmutableConfigExceptionHandler(ExceptionHandler):
    """Handles immutable configuration modification attempts."""
    
    def can_handle(self, exception: Exception) -> bool:
        from dynastore.modules.db_config.exceptions import ImmutableConfigError
        return isinstance(exception, ImmutableConfigError)
    
    def handle(self, exception: Exception, context: Optional[Dict[str, Any]] = None) -> Optional[HTTPException]:
        return HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exception))


class PluginNotFoundExceptionHandler(ExceptionHandler):
    """Handles plugin not found errors."""
    
    def can_handle(self, exception: Exception) -> bool:
        from dynastore.modules.db_config.exceptions import PluginNotRegisteredError
        return isinstance(exception, PluginNotRegisteredError)
    
    def handle(self, exception: Exception, context: Optional[Dict[str, Any]] = None) -> Optional[HTTPException]:
        return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exception))


class ConfigValidationExceptionHandler(ExceptionHandler):
    """Handles configuration validation errors."""
    
    def can_handle(self, exception: Exception) -> bool:
        from dynastore.modules.db_config.exceptions import ConfigValidationError
        return isinstance(exception, ConfigValidationError)
    
    def handle(self, exception: Exception, context: Optional[Dict[str, Any]] = None) -> Optional[HTTPException]:
        return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exception))


class ExceptionHandlerRegistry:
    """Central registry for managing pluggable exception handlers."""
    
    def __init__(self):
        self._handlers: List[ExceptionHandler] = []
        self._register_builtin_handlers()
    
    def _register_builtin_handlers(self):
        """Register built-in handlers in order of specificity."""
        # Register in specific → generic order

        self.register(ConflictExceptionHandler())
        self.register(ImmutableConfigExceptionHandler())
        self.register(PluginNotFoundExceptionHandler())
        self.register(ConfigValidationExceptionHandler())
        self.register(ProgrammingErrorHandler()) # Catch programming errors before generic validation
        self.register(ValidationExceptionHandler())  # Generic - must be last
    
    def register(self, handler: ExceptionHandler, prepend: bool = False) -> None:
        """
        Register a new exception handler.
        
        Args:
            handler: The handler instance to register
            prepend: If True, add to front of list for higher priority
        """
        if prepend:
            self._handlers.insert(0, handler)
            logger.debug(f"Registered exception handler (priority): {handler.__class__.__name__}")
        else:
            self._handlers.append(handler)
            logger.debug(f"Registered exception handler: {handler.__class__.__name__}")
    
    def handle(
        self, 
        exception: Exception, 
        context: Optional[Dict[str, Any]] = None,
        reraise_unhandled: bool = True
    ) -> Union[HTTPException, Response]:
        """
        Process exception through registered handlers.
        
        Args:
            exception: The exception to handle
            context: Optional context dict with keys like 'resource_name', 'resource_id', 'operation'
            reraise_unhandled: If True, re-raise if no handler matches
        
        Returns:
            HTTPException or Response with appropriate status code and detail
        
        Raises:
            Exception: The original exception if no handler matches and reraise_unhandled=True
        """
        context = context or {}
        
        # Try each handler in order
        for handler in self._handlers:
            try:
                if handler.can_handle(exception):
                    result = handler.handle(exception, context)
                    if result:
                        logger.debug(f"Exception handled by {handler.__class__.__name__}")
                        return result
            except Exception as e:
                logger.error(f"Error in exception handler {handler.__class__.__name__}: {e}", exc_info=True)
                continue
        
        # No handler matched
        if reraise_unhandled:
            raise exception
        
        # Fallback to generic 500
        logger.error(f"Unhandled exception: {exception}", exc_info=True)
        return HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred."
        )


# Global singleton registry
_global_registry = ExceptionHandlerRegistry()


def register_handler(handler: ExceptionHandler, prepend: bool = False) -> None:
    """Register a custom exception handler globally."""
    _global_registry.register(handler, prepend=prepend)


def handle_exception(
    exception: Exception,
    resource_name: Optional[str] = None,
    resource_id: Optional[str] = None,
    operation: Optional[str] = None,
    reraise_unhandled: bool = True
) -> Union[HTTPException, Response]:
    """
    Handle an exception using registered handlers.
    
    This is the main entry point for service endpoints.
    
    Args:
        exception: The exception to handle
        resource_name: Name of the resource (e.g., "Catalog", "Collection")
        resource_id: ID/code of the resource (optional)
        operation: Operation being performed (e.g., "Catalog creation")
        reraise_unhandled: If True, re-raise if no handler matches
    
    Returns:
        HTTPException or Response with appropriate status code and detail
    
    Example:
        try:
            result = await catalog_manager.create_catalog(data)
        except Exception as e:
            raise handle_exception(
                e,
                resource_name="Catalog",
                resource_id=data.get("id"),
                operation="Catalog creation"
            )
    """
    context = {}
    if resource_name:
        context['resource_name'] = resource_name
    if resource_id:
        context['resource_id'] = resource_id
    if operation:
        context['operation'] = operation
    
    return _global_registry.handle(exception, context=context, reraise_unhandled=reraise_unhandled)


class GlobalExceptionHandlingMiddleware(BaseHTTPMiddleware):
    """
    A middleware that wraps the entire application stack to catch any unhandled
    exception that occurs, including those from other middleware. It then uses
    the application's centralized `generic_exception_handler` to ensure a
    consistent, managed error response is returned.
    """
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        try:
            return await call_next(request)
        except Exception as exc:
            return await generic_exception_handler(request, exc)


def auto_handle_exceptions(
    resource_name: Optional[str] = None,
    resource_id_param: Optional[str] = None,
    operation: Optional[str] = None
) -> Callable[[F], F]:
    """
    Decorator to automatically handle exceptions in endpoint functions.
    
    Wraps endpoint to catch all exceptions and route through the handler registry.
    Reduces boilerplate by eliminating manual try-except blocks.
    
    Args:
        resource_name: Name of the resource (e.g., "Catalog", "Collection")
        resource_id_param: Parameter name from function args to extract resource_id
                          (e.g., "definition" to get definition.code)
        operation: Operation being performed (e.g., "Catalog creation")
    
    Returns:
        Decorated async function with automatic exception handling
    
    Example:
        @router.post("/catalogs")
        @auto_handle_exceptions(resource_name="Catalog", resource_id_param="definition", operation="Creation")
        async def create_catalog(definition: CatalogRequest):
            # No try-except needed! Exceptions are handled automatically
            result = await catalog_manager.create_catalog(definition.model_dump())
            return result
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                # Extract resource_id from function argument if specified
                extracted_resource_id = None
                if resource_id_param:
                    # Try to get from kwargs first
                    if resource_id_param in kwargs:
                        obj = kwargs[resource_id_param]
                        extracted_resource_id = _extract_resource_id(obj)
                    # Then try to get from positional args by matching parameter names
                    else:
                        import inspect
                        sig = inspect.signature(func)
                        params = list(sig.parameters.keys())
                        if resource_id_param in params:
                            idx = params.index(resource_id_param)
                            if idx < len(args):
                                obj = args[idx]
                                extracted_resource_id = _extract_resource_id(obj)
                
                raise handle_exception(
                    e,
                    resource_name=resource_name,
                    resource_id=extracted_resource_id,
                    operation=operation
                )
        
        return wrapper  # type: ignore
    
    return decorator


def _extract_resource_id(obj: Any) -> Optional[str]:
    """
    Extract resource ID from an object.
    
    Tries common patterns like .code, .id, .get("code"), .get("id")
    """
    if obj is None:
        return None
    
    if isinstance(obj, str):
        return obj
    
    # Try .id attribute
    if hasattr(obj, 'id'):
        return getattr(obj, 'id')

    # Try dict-like access
    if isinstance(obj, dict):
        return obj.get('id') or obj.get('code')
    
    # Try model_dump for Pydantic models
    if hasattr(obj, 'model_dump'):
        try:
            data = obj.model_dump()
            return data.get('id') or data.get('code')
        except:
            pass

    # Try .code attribute
    if hasattr(obj, 'code'):
        return getattr(obj, 'code')
    
    return None


async def generic_exception_handler(request: Request, exc: Exception) -> Response:
    """
    Central handler to log an exception and then format it into a JSON response.
    This is the primary exception handling entry point for the application, called
    by the GlobalExceptionHandlingMiddleware.
    """
    logger.warning(f"Unhandled exception caught by global handler: {exc}", exc_info=True)
    
    # Extract request context for logging
    request_context = {
        "method": request.method,
        "url": str(request.url),
        "path": request.url.path,
        "query_params": dict(request.query_params),
    }
    
    # Try to extract headers (excluding sensitive ones)
    try:
        headers = dict(request.headers)
        for sensitive_key in ['authorization', 'cookie']:
            headers.pop(sensitive_key, None)
        request_context["headers"] = headers
    except Exception:
        pass
    
    # Try to extract catalog/collection from path
    try:
        path_parts = request.url.path.strip('/').split('/')
        if 'catalogs' in path_parts:
            idx = path_parts.index('catalogs')
            if idx + 1 < len(path_parts):
                request_context["catalog_id"] = path_parts[idx + 1]
        if 'collections' in path_parts:
            idx = path_parts.index('collections')
            if idx + 1 < len(path_parts):
                request_context["collection_id"] = path_parts[idx + 1]
    except Exception:
        pass
    
    # Build context for exception handlers
    context = {
        "request_context": request_context,
        "catalog_id": request_context.get("catalog_id") or "_system_",
        "collection_id": request_context.get("collection_id"),
        "operation": f"{request.method} {request.url.path}",
    }
    
    # Log the exception to the database and get a log_id for the response
    log_id = None
    try:
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols.database import DatabaseProtocol
        engine = get_protocol(DatabaseProtocol)
        from dynastore.models.protocols.logs import LogsProtocol
        log_event = get_protocol(LogsProtocol)
        if log_event:
            log_id = await log_event(
                catalog_id=context.get("catalog_id", "_system_"),
                event_type="exception",
                level="ERROR",
                message=f"Error during {context.get('operation', 'Unknown Operation')}: {str(exc)}",
                collection_id=context.get("collection_id"),
                details={
                    "error_type": exc.__class__.__name__,
                    "traceback": traceback.format_exc(),
                    "request_context": context.get("request_context")
                },
                db_resource=engine,
                immediate=True
            )
            if log_id:
                context['log_id'] = log_id
                context['log_catalog'] = context.get("catalog_id", "_system_")
    except Exception as log_exc:
        logger.critical(f"CRITICAL: Failed to log original exception to database: {log_exc}", exc_info=True)
        logger.error(f"Original unlogged exception: {exc}", exc_info=True)

    # Use the registry to format the exception into a structured error, but don't re-raise it.
    result = _global_registry.handle(exc, context=context, reraise_unhandled=False)

    try:
        if isinstance(result, Response):
            return result

        response_content = {"detail": f"{getattr(result, 'detail', 'An unexpected error occurred.')} | Error: {str(exc)}"}
        status_code = getattr(result, 'status_code', 500)

        if log_id and status_code >= 500:
            log_url = f"/logs/catalogs/{context['log_catalog']}/logs/{log_id}"
            response_content["log_reference"] = {
                "log_id": log_id, "catalog_id": context['log_catalog'],
                "collection_id": context.get('log_collection'), "url": log_url
            }

        return JSONResponse(status_code=status_code, content=response_content, headers=getattr(result, "headers", None))
    except Exception as fallback_exc:
        logger.error(f"Error in exception handler fallback: {fallback_exc}", exc_info=True)
        return JSONResponse(status_code=500, content={"detail": "An unexpected error occurred in the exception handler."})


def setup_exception_handlers(app: FastAPI) -> None:
    """
    Initialize exception handling integration with FastAPI app.
    
    Sets up FastAPI exception handlers to use the pluggable registry.
    Call this during app startup/initialization.
    
    Args:
        app: FastAPI application instance
    
    Example:
        app = FastAPI()
        
        @app.on_event("startup")
        async def startup():
            setup_exception_handlers(app)
        
        # Or simply:
        setup_exception_handlers(app)
    """
    # Add our custom middleware to the application stack.
    # This will catch exceptions from all subsequent middleware and the app itself.
    app.add_middleware(GlobalExceptionHandlingMiddleware)

    # Register the generic handler for any exceptions that might still
    # be caught by FastAPI's built-in ExceptionMiddleware. This provides
    # defense-in-depth.
    app.add_exception_handler(Exception, generic_exception_handler)
    
    logger.info("Exception handling system initialized for FastAPI app")


def register_extension_handler(handler: ExceptionHandler, prepend: bool = False) -> None:
    """
    Register a custom exception handler from an extension.
    
    This is a convenience wrapper for modules that want to register their own handlers
    without importing the registry directly.
    
    Args:
        handler: Custom ExceptionHandler implementation
        prepend: If True, add to front of list for higher priority
    
    Example:
        from my_extension import MyCustomException, MyCustomHandler
        
        register_extension_handler(MyCustomHandler(), prepend=True)
    """
    register_handler(handler, prepend=prepend)
