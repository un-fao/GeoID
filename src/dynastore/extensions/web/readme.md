# Web Extension

## Overview

The Web Extension provides a comprehensive, modern web interface for DynaStore, serving as the primary portal for exploration, documentation, and system monitoring.

## Features

### 🌐 **Multi-Page SPA (Single Page Application)**
A complete web application with intuitive navigation and deep linking support:
- **Home**: Hero section with platform overview and quick stats
- **Overview**: Data hierarchy, core value propositions, and platform capabilities
- **Architecture**: The Three Pillars pattern explained with visual guides
- **Features**: Comprehensive overview of all implemented OGC and geospatial extensions
- **Deployment**: Installation methods, Docker Compose environments, and core services
- **Docs**: Integrated documentation portal with sidebar navigation

### 📚 **Integrated Documentation Portal**
Dynamically discovers and renders all `readme.md` files from across the project:
- **Sidebar Navigation**: Organized by category (Modules, Extensions, Tasks)
- **Content Rendering**: Full Markdown support with syntax highlighting
- **Deep Linking**: Direct URL access to specific documentation pages (`#docs:module-id`)
- **Search & Filter**: Easy navigation through documentation hierarchy
- **Responsive Layout**: Works on desktop, tablet, and mobile devices

### 📊 **Live Dashboard** (When Backend Supports It)
Real-time system monitoring:
- **System Stats**: Total requests, success rate, average latency, active tasks
- **Live Logs**: Stream system logs with filtering by level
- **Task Monitor**: Track active background tasks with progress bars

### 🎨 **Modern Design**
- Dark theme optimized for extended reading
- Glassmorphism design system for depth and visual hierarchy
- Smooth animations and transitions
- Responsive grid layouts
- Accessibility-first approach

## Main Endpoint

```
GET /web/
```

Serves the main SPA application.

## File Structure

```
src/dynastore/extensions/web/
├── web.py                    # FastAPI extension implementation
├── readme.md                 # This file
├── requirements.txt          # Dependencies (FastAPI, uvicorn, etc.)
└── static/
    ├── index.html           # Main SPA entry point
    ├── docs.html            # Alternative docs-only page
    ├── custom.js            # Navigation, docs loading, dashboard logic
    ├── styles.css           # Dark theme CSS + prose styles
    ├── swagger_*.css/js     # OpenAPI Swagger UI customization
    ├── README_home.md       # Homepage markdown content
    └── _index.html          # Fallback page
```

## Configuration

The web extension requires no special configuration. It automatically:
- Discovers documentation files from the backend
- Loads system statistics from monitoring endpoints
- Renders content dynamically based on available data

## Backend Endpoints Used

The frontend communicates with the backend to fetch content:

### Documentation
```
GET /web/docs-manifest              # Get list of available docs
GET /web/docs-content/{id}          # Get specific doc content (HTML)
```

### Dashboard (per-catalog, gated by `TenantScopeMiddleware`)
```
GET /web/dashboard/                                              # Catalog picker (anonymous-allowed)
GET /web/dashboard/catalogs/{catalog_id}/                        # Per-catalog HTML shell
GET /web/dashboard/catalogs/{catalog_id}/processes/              # Per-catalog process executor shell
GET /web/dashboard/catalogs/{catalog_id}/stats                   # Per-catalog statistics
GET /web/dashboard/catalogs/{catalog_id}/logs?limit=50&level=INFO  # Per-catalog logs
GET /web/dashboard/catalogs/{catalog_id}/events                  # Per-catalog events
GET /web/dashboard/catalogs/{catalog_id}/tasks                   # Per-catalog background tasks
GET /web/dashboard/catalogs/{catalog_id}/ogc-compliance          # Per-catalog OGC conformance
GET /web/dashboard/catalogs/{catalog_id}/collections/{collection_id}/stats
GET /web/dashboard/catalogs/{catalog_id}/collections/{collection_id}/logs
GET /web/dashboard/catalogs/{catalog_id}/collections/{collection_id}/events
```

`{catalog_id}=_system_` is the synthetic platform-scope (sysadmin-only). Catalog admins
get 401/403 on catalogs they don't own — the gate runs in `TenantScopeMiddleware`
before the handler, so the route bodies carry no authz code.

## Development

### Editing Pages

Each main page (Overview, Architecture, Features, Deployment) is a React-free component in `index.html`. Simply edit the corresponding `<div id="section-{name}">` element.

### Adding New Pages

1. Create a new section in `index.html`:
   ```html
   <div id="section-mynewpage" class="hidden-section fade-in">
       <!-- Your content -->
   </div>
   ```

2. Add a navigation button:
   ```html
   <button onclick="switchTab('mynewpage')" class="nav-btn ...">My New Page</button>
   ```

3. Styled content automatically inherits from `styles.css`

### Customizing Styles

All CSS is in `static/styles.css`. Key theme colors are in the Tailwind config within HTML files:
- Primary: `#3b82f6` (Blue)
- Accent: `#0ea5e9` (Cyan)
- Success: `#10b981` (Emerald)
- Warning: `#f59e0b` (Amber)
- Danger: `#ef4444` (Red)

### Markdown Content Rendering

Documentation is automatically rendered from markdown files. The backend should provide:
1. A manifest listing available docs with IDs
2. HTML-converted content for each doc

## Browser Support

- Modern browsers (Chrome, Firefox, Safari, Edge)
- Mobile responsive design
- No JavaScript framework dependencies (vanilla JS only)

## Performance

- Single CSS/JS file loading for main pages
- Lazy loading of documentation content
- No external API calls during navigation
- Smooth animations with hardware acceleration

## Security

- Content Security Policy ready (no inline scripts in production)
- CORS-compliant for remote content loading
- Markdown sanitization handled server-side
- XSS protection for user-generated content

## Future Enhancements

- [ ] Dark/Light theme toggle
- [ ] Documentation search with client-side indexing
- [ ] API interactive explorer (Swagger UI integration)
- [ ] Performance metrics visualization
- [ ] Multi-language support
- [ ] PWA capabilities (offline documentation)
- [ ] Advanced dashboard with charts and graphs

## Technical Details

### Navigation Flow
```
User clicks nav button → switchTab() called → Section shown with fade animation → 
If docs, initDocs() called → Manifest fetched → Content loaded dynamically
```

### Documentation Loading
```
User opens /docs or clicks Docs tab → initDocs() → /web/docs-manifest fetched →
Sidebar rendered → User clicks doc → loadDocContent(id) → /web/docs-content/{id} fetched →
HTML injected into #docs-content
```

## Maintenance

The web extension is static-first. Most updates require only editing HTML/CSS, not backend changes.

Regular maintenance tasks:
- Update documentation links if module names change
- Refresh hero section imagery/stats quarterly
- Test responsive design on new device sizes
- Review browser console for deprecation warnings

## License

Apache 2.0 - Same as parent project
