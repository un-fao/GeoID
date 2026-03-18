# Protocol-Based Discovery & Decoupling

## Overview
Agro-Informatics Platform (AIP) - Catalog Services uses a "Duck Typing" dependency injection mechanism based on Python's `runtime_checkable` protocols. This approach allows components (Modules, Extensions, and Tasks) to discover each other's capabilities without direct imports, significantly reducing circular dependencies and architectural coupling.

## The `get_protocol` Tool
The core of this mechanism is the `get_protocol(Protocol)` utility located in `dynastore.tools.discovery`.

### How it Works
1.  **Capability over Class**: A component asks for a **CAPABILITY** (defined by a Protocol) rather than a specific **IMPLEMENTATION** class.
2.  **Unified Search**: It searches across all instantiated:
    *   **Modules**: Foundational services (DB, Auth, etc.)
    *   **Extensions**: Web-facing features (STAC, Tiles, etc.)
    *   **Tasks**: Background processing units.
3.  **Prioritization & Fallback**:
    *   **Priority**: Each implementation can declare a `priority` attribute (integer). Higher values have higher precedence.
    *   **Availability**: Implementations can define an `is_available() -> bool` method. The discovery tool skips any implementation where this returns `False`.
    *   **Selection**: `get_protocol()` returns the highest priority implementation that is currently available.
4.  **Caching**: The results are cached using `@lru_cache` to ensure performance remains high during frequent lookups.

### Discovery APIs
- `get_protocol(protocol: Type[T]) -> Optional[T]`: Returns the single best (highest priority, available) implementation.
- `get_protocols(protocol: Type[T]) -> List[T]`: Returns all available implementations sorted by priority.

## Example Use Case: Decentralized Policies
Previously, extensions like `tiles` and `stac` had to import the `apikey` module directly to register their public access policies. This created a hard dependency and potential circular imports.

### The Decoupled Approach:
1.  **Define the Protocol**: The `AuthorizationProtocol` (in `dynastore.models.auth`) defines the `register_policy` method.
2.  **Implement the Protocol**: The `ApiKeyModule` implements `register_policy`.
3.  **Discover and Use**:
    ```python
    from dynastore.models.auth import AuthorizationProtocol
    from dynastore.tools.discovery import get_protocol

    def register_extension_policies():
        authz = get_protocol(AuthorizationProtocol)
        if authz:
            authz.register_policy(my_policy)
    ```

## Benefits
-   **No Direct Imports**: Extensions don't need to know which module provides authorization.
-   **Pluggability**: The `ApiKeyModule` could be replaced by a `KeycloakModule` as long as it implements `AuthorizationProtocol`.
-   **Resilience**: Extensions gracefully handle cases where no authorization provider is present (e.g., in a minimal environment) or fall back to lower-priority providers.

## Implementation Details
-   **Tool**: `dynastore/tools/discovery.py`
-   **Protocols**: `dynastore/models/auth.py`, `dynastore/modules/protocols.py`
-   **Refactored Example**: [tiles/policies.py](file:///Users/ccancellieri/work/code/dynastore/src/dynastore/extensions/tiles/policies.py)
