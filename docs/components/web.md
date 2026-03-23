# Web Extension & Extensible UI

This document describes the web dashboard framework, the `@expose_web_page` decorator system, and the shared UI components.

## Architecture

The web extension serves the single-page dashboard and manages page registration from all extensions. Each extension can contribute pages, static assets, and API endpoints to the dashboard without modifying the web extension itself.

```
WebModule (module)          — page registry, scan_and_register_providers()
WebService (extension)      — routes, SPA shell, /web/config/pages endpoint
@expose_web_page decorator  — declarative page registration on any extension
```

## Page Registration

### The `@expose_web_page` Decorator

Any extension method can declare itself as a web page provider:

```python
@expose_web_page(
    page_id="map_viewer",
    title="Map Viewer",
    icon="fa-map",
    description="Interactive map viewer.",
    required_roles=[],          # empty = public
    section="main",             # "main", "admin", "tools"
    priority=10,                # lower = higher in nav
)
async def provide_map_viewer(self, request: Request):
    return HTMLResponse(...)
```

The decorator attaches a `_web_page_config` dict to the method. During startup, `WebModule.scan_and_register_providers(instance)` discovers all decorated methods on an extension instance and registers them in the central page registry.

### Discovery Flow

```
1. Extension.__init__() or configure_app()
   → calls web.scan_and_register_providers(self)
2. WebModule iterates methods with _web_page_config attribute
3. Each page is registered with dedup guard (page_id uniqueness)
4. GET /web/config/pages returns filtered page list based on user roles
5. SPA shell renders navigation from the page list
```

### Role Filtering

The `/web/config/pages` endpoint filters pages by the authenticated user's roles:
- Pages with `required_roles=[]` are visible to everyone
- Pages with `required_roles=["sysadmin"]` are only in the response for sysadmin users
- The SPA shell renders navigation items only for pages present in the response

### Static Asset Registration

Extensions can also register static file directories:

```python
@expose_static(prefix="/maps/static")
def provide_static(self):
    return os.path.join(os.path.dirname(__file__), "static")
```

## Shared UI Components

### Context Selector (`context-selector.js`)

A cascaded dropdown component shared across 5+ extensions for selecting Catalog → Collection → Asset:

```html
<ds-context-selector
    id="ctx"
    show-assets="true"
    on-change="handleContextChange">
</ds-context-selector>
```

The component:
- Fetches catalogs from `/catalogs/` on mount
- Fetches collections from `/catalogs/{id}/collections` on catalog select
- Fetches assets from `/features/catalogs/{id}/collections/{id}/items` on collection select
- Emits change events with `{catalog_id, collection_id, asset_id}`

Used by: Data Explorer, STAC Browser, Map Viewer, Admin Panel, Features Inspector.

### Language Selector (`<ds-language-selector>`)

A Web Component for switching the UI language:

```html
<ds-language-selector></ds-language-selector>
```

Reads available languages from `window.__DS_LANGUAGES__` and persists selection in `localStorage`. All multilingual content (titles, descriptions) is stored as JSONB `{"en": "...", "it": "..."}` and the selected language key is passed as `?lang=` parameter.

## Dashboard Pages

| Page ID | Extension | Section | Roles | Description |
|---------|-----------|---------|-------|-------------|
| `home` | web | main | — | Landing page |
| `dashboard` | web | main | — | System dashboard with stats |
| `docs_viewer` | web | main | — | API documentation browser |
| `demo_manager` | web | admin | sysadmin | Demo data provisioning |
| `map_viewer` | maps | main | — | Interactive tile map |
| `stac_browser` | stac | main | — | STAC catalog browser |
| `data_explorer` | features | main | — | Feature data explorer |
| `configs_editor` | configs | admin | sysadmin | Configuration management |
| `admin_panel` | admin | admin | sysadmin | User/key management |
| `migrations_panel` | admin | admin | sysadmin | Database migrations dashboard |
| `logs_viewer` | logs | admin | sysadmin | Access log viewer |

## Files

| Path | Purpose |
|------|---------|
| `src/dynastore/modules/web/web_module.py` | WebModule — central page registry |
| `src/dynastore/extensions/web/decorators.py` | @expose_web_page, @expose_static decorators |
| `src/dynastore/extensions/web/web.py` | WebService — SPA shell, routes, demo endpoints |
| `src/dynastore/extensions/web/static/` | Shared JS/CSS (context-selector.js, etc.) |
