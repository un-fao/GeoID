import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from dynastore.extensions.tools.exception_handlers import ExceptionHandlerRegistry, ExceptionHandler
from dynastore.extensions.logs.exception_handler import CatalogLoggingExceptionHandler
from dynastore.modules.catalog.log_manager import LogEntryCreate

class CustomException(Exception):
    pass

@pytest.mark.asyncio
async def test_catalog_logging_handler():
    """Verify that CatalogLoggingExceptionHandler triggers log_event."""
    
    # Mock log_event
    mock_logs_service = AsyncMock()
    with patch("dynastore.extensions.logs.exception_handler.get_protocol", return_value=mock_logs_service) as mock_get_proto:
        mock_log_event = mock_logs_service.log_event
        handler = CatalogLoggingExceptionHandler()
        
        # Should always handle
        assert handler.can_handle(ValueError("test"))
        
        # Test generic system log
        exc = ValueError("Something broke")
        context = {"operation": "Testing"}
        
        # Mock run_in_background to execute immediately
        with patch("dynastore.extensions.logs.exception_handler.run_in_background") as mock_bg:
            # We want to verify the task is created
            
            result = handler.handle(exc, context)
            
            # Handler should return None (allowing chain to continue)
            assert result is None
            
            # Verify run_in_background was called
            assert mock_bg.call_count == 1
            args, _ = mock_bg.call_args
            coro = args[0]
            
            # Await the coroutine to verify it calls log_event
            await coro
            
            # Verify log_event was called with expected params
            mock_log_event.assert_called_once()
            call_kwargs = mock_log_event.call_args.kwargs
            
            assert call_kwargs["catalog_id"] == "_system_"
            assert call_kwargs["level"] == "ERROR"
            assert "Something broke" in call_kwargs["message"]

@pytest.mark.asyncio
async def test_catalog_logging_handler_with_catalog_context():
    """Verify logging with specific catalog context."""
    mock_logs_service = AsyncMock()
    with patch("dynastore.extensions.logs.exception_handler.get_protocol", return_value=mock_logs_service) as mock_get_proto:
        mock_log_event = mock_logs_service.log_event
        handler = CatalogLoggingExceptionHandler()
        exc = CustomException("Catalog error")
        context = {
            "operation": "Update", 
            "catalog_id": "cat_123", 
            "collection_id": "col_abc"
        }
        
        with patch("dynastore.extensions.logs.exception_handler.run_in_background") as mock_bg:
            handler.handle(exc, context)
            
            args, _ = mock_bg.call_args
            await args[0]
            
            call_kwargs = mock_log_event.call_args.kwargs
            assert call_kwargs["catalog_id"] == "cat_123"
            assert call_kwargs["collection_id"] == "col_abc"

@pytest.mark.asyncio
async def test_exception_registry_chaining():
    """Verify that exceptions flow through the logging handler to the next handler."""
    registry = ExceptionHandlerRegistry()
    registry._handlers = [] # Clear built-ins to test custom chain logic
    
    # Mock handlers
    logging_handler = CatalogLoggingExceptionHandler()
    logging_handler.handle = MagicMock(return_value=None) # Acts like middleware
    
    final_handler = MagicMock()
    final_handler.can_handle.return_value = True
    final_handler.handle.return_value = "Response Object" # Mock response
    
    registry.register(logging_handler)
    registry.register(final_handler)
    
    # Trigger handle
    result = registry.handle(ValueError("Ouch"))
    
    # Verify logging handler was called
    logging_handler.handle.assert_called_once()
    
    # Verify processing continued to final handler
    final_handler.handle.assert_called_once()
    assert result == "Response Object"
