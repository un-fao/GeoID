# Tasks Module

The `Tasks` module provides a background job and task management system for DynaStore. In Cellular Mode, tasks are decentralized and stored within tenant-specific schemas.

## Cellular Architecture

Tasks are no longer stored in a global `tasks.tasks` table. Instead:
- **Tenant Isolation**: Each catalog (tenant) has its own `tasks` table within its physical schema.
- **DDL Provisioning**: Task tables and their partitions are automatically provisioned when the first task for a tenant is created or when a catalog is initialized.
- **Schema Resolution**: Operations like `create_task` and `get_task` require an explicit `schema` parameter to target the correct isolated storage.

## Key Components

- **`TasksModule`**: Manages the module lifecycle and ensures partitions and retention policies are registered.
- **`tasks_module.py`**: Core logic for task CRUD operations, schema provisioning, and partition management.
- **`runners.py`**: Base classes and utilities for implementing task runners.
- **`models.py`**: Pydantic models for tasks (e.g., `Task`, `TaskCreate`).

## Task Attribution

- **Caller ID**: Every task is tagged with a `caller_id`.
- **User Tasks**: Attributed to the logged-in user's identifier.
- **System Tasks**: Attributed to `system:platform` (defined in `ApiKeyModule`) for background or administrative operations.

## Partitioning and Retention

The module uses PG-native partitioning (by timestamp) and automatic retention:
- **Partitions**: Monthly partitions are pre-provisioned.
- **Retention**: Data is automatically pruned based on the registered policy (default: 1 month).
