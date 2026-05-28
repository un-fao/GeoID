# Events Module

This document describes the event system that enables decoupled communication between components via webhook subscriptions and durable event dispatch.

## Architecture

```
Event Producer → EventsModule.create_event() → tasks.tasks (event_dispatch task)
                                                    ↓
Task Worker picks up event_dispatch → resolves subscribers → POST webhook_url
```

Events are dispatched via the task queue, not inline. This ensures:
- Event dispatch never blocks the API request
- Failed deliveries are retried via the task retry mechanism
- Events survive process crashes (durable outbox pattern)

## Subscription Model

```
platform.event_subscriptions
├── subscription_id   UUID (PK)
├── subscriber_name   VARCHAR(255)    — unique per event_type
├── event_type        VARCHAR(255)    — e.g. "catalog.created", "task.failed"
├── webhook_url       VARCHAR(2048)   — POST target
├── auth_config       JSONB           — API key or bearer token for webhook auth
└── created_at        TIMESTAMPTZ
    UNIQUE (subscriber_name, event_type)
```

Subscriptions are upserted: re-subscribing with the same `subscriber_name + event_type` updates the webhook URL and auth config.

## Event Types

Events are defined via the `define_event()` primitive:

| Event Type | Scope | Trigger |
|------------|-------|---------|
| `catalog.created` | PLATFORM | New catalog created |
| `catalog.deleted` | PLATFORM | Catalog deleted |
| `collection.created` | CATALOG | New collection in a catalog |
| `collection.deleted` | CATALOG | Collection deleted |
| `items.upserted` | COLLECTION | Items inserted/updated |
| `items.deleted` | COLLECTION | Items deleted |
| `task.completed` | SYSTEM | Background task finished |
| `task.failed` | SYSTEM | Background task failed |

## API

```
POST /events/subscribe     — create/update a subscription
DELETE /events/unsubscribe  — remove a subscription
GET  /events/types          — list registered event types
```

## Platform API Key

Inter-service webhook calls are authenticated using `PLATFORM_EVENTS_API_KEY`. On first startup, if the env var is not set, a random key is generated and persisted via `PropertiesProtocol`.

## Files

| Path | Purpose |
|------|---------|
| `src/dynastore/modules/events/events_module.py` | EventsModule — subscription CRUD, dispatch, catalog lifecycle listeners (inlined) |
| `src/dynastore/modules/events/primitives.py` | Event type definitions, EventRegistry |
| `src/dynastore/modules/events/models.py` | EventSubscription, AuthConfig models |
