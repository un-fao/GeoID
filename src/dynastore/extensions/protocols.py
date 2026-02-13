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

from typing import Protocol, Optional, Any, TYPE_CHECKING, runtime_checkable
if TYPE_CHECKING:
    from fastapi import FastAPI, APIRouter
else:
    FastAPI = Any
    APIRouter = Any
from contextlib import asynccontextmanager
from dynastore.modules.protocols import HasConfigManager

class ExtensionProtocol(HasConfigManager, Protocol):
    """
    Defines the contract for a DynaStore FastAPI Web Extension.

    This protocol is the blueprint for creating extensions. A conforming class
    is not required to have any specific members, but it can implement a set
    of conventional attributes and methods to hook into the application's
    lifecycle. The system discovers and uses these members if they exist.

    ---
    Conventional Members:
    All members are optional. The system checks for their presence at runtime.

    - `router: Optional[APIRouter] = None`: An optional FastAPI router instance
      containing the extension's endpoints. If provided, it will be automatically
      mounted to the main application during startup.

    - `__init__(self, app: FastAPI)`: An extension is not required to have a
      custom `__init__` method. However, if one is defined, it can optionally
      accept the main FastAPI app instance for stateful initialization.

    - `configure_app(app: FastAPI)`: An optional *static method* to perform early
      configuration on the FastAPI app, such as adding middleware. This is
      called during Phase 0 of the lifecycle.

    - `lifespan(app: FastAPI)`: An optional *static async context manager* for
      managing resources like database connections. It is entered during startup
      and exited gracefully on shutdown.

    - `readme.md`: An optional markdown file in the extension's root directory.
      If an extension provides a router and this file exists, a documentation
      endpoint will be automatically created at `/{router.prefix}/docs`. This
      endpoint will render the markdown file as an HTML page, providing a
      standardized way to document the extension's capabilities.
    ---

    Example (Extension adding only Middleware):
    ```python
    from fastapi.middleware.gzip import GZipMiddleware

    @dynastore_extension
    class GzipExtension:
        # This extension has no router, no instance state, and no __init__.
        def configure_app(self, app: FastAPI):
            app.add_middleware(GZipMiddleware, minimum_size=1000)
    ```

    Example (Instance-Based Extension):
    ```python
    @dynastore_extension
    class MyInstanceExtension:
        def __init__(self, app: FastAPI):
            self.counter = 0
            self.router = APIRouter(prefix="/instance")
            self.router.add_api_route("/count", self.get_count, methods=["GET"])

        def get_count(self):
            self.counter += 1
            return {"count": self.counter}
    ```
    """
    # The protocol only defines optional attributes. Optional methods are
    # treated as conventions described in the docstring above.
    router: Optional[APIRouter] = None

    _registered_name: str = "unregistered_extension" # Default value, will be set by decorator
    priority: int = 0  # Default priority

    def is_available(self) -> bool:
        """
        Returns whether the extension is currently available to provide its protocol capability.
        Used by the discovery mechanism for prioritized fallbacks.
        """
        return True

    @classmethod
    def get_name(cls) -> str:
        """
        Returns the registered name of the extension.
        This class method reads the name set by the @dynastore_extension decorator.
        """
