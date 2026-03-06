from typing import Protocol, runtime_checkable, Optional
import httpx

@runtime_checkable
class HttpxProtocol(Protocol):
    """Protocol for the shared HTTPX client extension."""
    
    def get_httpx_client(self) -> httpx.AsyncClient:
        """Returns the shared, singleton httpx.AsyncClient."""
        ...

    def get_proxy_httpx_client(self) -> httpx.AsyncClient:
        """Returns the shared, non-pooling httpx.AsyncClient for proxying."""
        ...
