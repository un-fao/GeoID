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

import inspect

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
                if inspect.iscoroutinefunction(handler):
                    await handler(conn)
                else:
                    handler(conn)
            except Exception as e:
                logger.error(f"Cleanup handler {handler.__name__} failed: {e}")
