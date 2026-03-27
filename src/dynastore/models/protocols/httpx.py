from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from httpx import AsyncClient


@runtime_checkable
class HttpxProtocol(Protocol):
    """Protocol for the shared HTTPX client extension."""

    def get_httpx_client(self) -> AsyncClient:
        """Returns the shared, singleton httpx.AsyncClient."""
        ...

    def get_proxy_httpx_client(self) -> AsyncClient:
        """Returns the shared, non-pooling httpx.AsyncClient for proxying."""
        ...
