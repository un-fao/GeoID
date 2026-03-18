import asyncio

class CleanupRegistry:
    _handlers = []
    
    @classmethod
    def register(cls, handler):
        cls._handlers.append(handler)
        return handler
        
    @classmethod
    async def run_all(cls, conn):
        import logging
        logger = logging.getLogger(__name__)
        for handler in cls._handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(conn)
                else:
                    handler(conn)
            except Exception as e:
                logger.error(f"Cleanup handler {handler.__name__} failed: {e}")
