# Architecture Overview

This document presents the foundational architecture of Agro-Informatics Platform (AIP) - Catalog Services, an enterprise-grade, cloud-native platform for the management, processing, and dissemination of geospatial data. The system is designed as a modular, microservices-based framework engineered for extreme scalability, maintainability, and interoperability.

## The Three Pillars Architecture

The entire Agro-Informatics Platform (AIP) - Catalog Services software architecture is predicated on a foundational pattern we call the "Three Pillars." This pattern mandates a strict separation of all application logic into one of three distinct component types: Modules, Extensions, or Tasks.

The purpose of this separation is to ensure that the system remains loosely coupled, highly cohesive, and resilient to change and failure over its entire lifecycle. Each pillar has a hermetically sealed set of responsibilities:

1.  **Modules:** The foundational, backend-agnostic libraries that provide the core, reusable business logic and data management services for a single, well-defined domain (e.g. `catalog`, `tasks`, `gcp`).
2.  **Extensions:** The public-facing components that are the primary consumers of the services provided by Modules. They translate incoming API requests into one or more calls to the foundational modules (e.g. `features`, `stac`, `wfs`).
3.  **Tasks:** Asynchronous, background workers designed specifically for long-running, resource-intensive jobs that would be unacceptable to run within the synchronous context of an API request.

### 1. Pillar I: Modules - The Foundational Layer

Modules are the bedrock of the system. A module is the ultimate owner and single source of truth for a given data concept and its corresponding database schema.

- **Independence & Isolation**: A module must have zero knowledge of other modules or any extensions. It cannot import or directly call functions from another module.
- **API Agnostic**: Modules expose generic, CRUD-style operations with clean, standard Python interfaces (e.g., `async def create_collection(...)`). They do not output STAC or GeoJSON formats.
- **Models**: A Pydantic model (`src/dynastore/models`) MUST NOT import or depend on anything from the `src/dynastore/modules` directory.
- **Self-Contained Schema**: Each module manages its own tables, indexes, and triggers via its `lifespan` context manager function. 

### 2. Pillar II: Extensions - The API Layer

Extensions act as a translation or "adapter" layer between the outside world and the system's core business logic.

- **Stateless**: Extensions must be completely stateless. All data persistence and business logic must be delegated to the modules. 
- **Composable**: The architecture allows creating bespoke microservices by enabling only the required extensions. A headless server can run only the `ingestion` extension, while a portal will run `features`, `stac`, and `tiles`.
- **Automated Documentation**: If an extension includes a `readme.md` in its root, the framework automatically serves it via a `GET /docs` endpoint.

### 3. Pillar III: Tasks - The Asynchronous Worker Layer

Tasks are run for bulk processing jobs (e.g., file ingestion). They report status to the `tasks` module, but execute separately.

- **Isolation**: Tasks run in completely separate application instances/containers. A CPU-hogging task cannot take down the user API.
- **Synchronous Connection**: Task workers use `psycopg2` (sync pools) rather than `asyncpg`. This fits single-threaded processing flows. To bridge this, tasks use `run_in_event_loop` to interact with Async module methods safely.
- **Generic Runner**: `main_task.py` acts as a universal runner that takes a `task_name` and JSON `payload` and dispatches it dynamically by loading predefined module environments.

### 4. The Anti-Pattern

Violating these boundaries is critical technical debt.
- A Module calling another module is strictly forbidden.
- An Extension issuing schema updates directly through SQL (instead of calling a module) is strictly forbidden.
- A Module constructing STAC JSON is forbidden. STAC logic belongs in the STAC Extension.

## Component-Local Environment Files

Any module, extension, or task can ship a `.env` file in its own directory alongside its Python source. The framework automatically discovers and loads it **before the component is instantiated**, using `python-dotenv`'s `load_dotenv(override=False)`.

### Convention

```
src/dynastore/modules/gcp/.env        # loaded before GCPModule.__init__
src/dynastore/extensions/tiles/.env   # loaded before TilesExtension.__init__
src/dynastore/tasks/my_task/.env      # loaded before MyTask.__init__
```

### Rules

1. **No override** — `load_dotenv` is called with `override=False`. Environment variables already present (from Docker Compose, CI secrets, or a parent `.env`) are never clobbered.
2. **Local development only** — `.env` files are intended for local developer machines. They must not contain production secrets and should be listed in `.gitignore`.
3. **No GCP-specific logic** — the mechanism is generic. Any component can use it.
4. **Paths must be runtime-local** — unlike Docker Compose paths (e.g. `/dynastore/src/...`), values in a component `.env` must resolve correctly on the developer's machine.

### Implementation

The helper lives in `dynastore/tools/env.py` (`load_component_dotenv`) and is called from:
- `dynastore/modules/__init__.py` — `instantiate_modules`
- `dynastore/extensions/registry.py` — `instantiate_extensions`
- `dynastore/tasks/__init__.py` — `manage_tasks` and `get_task_instance`
