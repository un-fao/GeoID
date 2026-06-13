# Per-extension web sections: STAC, Features, Assets

**Date:** 2026-06-09
**Status:** Design approved — ready for implementation plan
**Scope:** STAC, OGC Features, Assets web UI sections. **Out of scope:** Configs UI and Admin UI (owned by a parallel refactor — do not edit those files).

## Problem

The web UI concentrates extension-specific functionality inside a single Admin page. Two consequences:

1. **Coupling to the auth instance.** The Admin page is where things like the Configs UI live, so an instance that does not ship IAM/Auth (e.g. a catalog-only instance on `localhost:80`) cannot reach that functionality at all.
2. **No per-extension separation of concerns.** STAC, Features, and Assets expose rich APIs but contribute little or no dedicated UI; users drive them through generic Admin screens rather than purpose-built, simplified interfaces.

The goal: give each main extension its own web section, built on existing protocols, multilingual, so functionality follows the extension onto whatever instance loads it — without losing the Admin/IAM/Auth experience that already works well.

## Key architectural finding

A per-extension web-contribution pattern **already exists and is widely used** (maps, tiles, iam, logs, notebooks, stats, geoid, auth, and a STAC browser page all use it). No new framework is required.

- `@expose_web_page(page_id, title, icon, description, audience_policy_id=..., is_embed=..., section=..., priority=...)` and `@expose_static(virtual_path, owner=, description=)` decorators
  (`packages/extensions/web/.../web/decorators.py`).
- Capability protocols `WebPageContributor.get_web_pages()` / `StaticAssetProvider.get_static_assets()`
  (`packages/core/.../models/protocols/web_ui.py`). Each extension implements these by calling
  `collect_web_pages(self)` / `collect_static_assets(self)`
  (`packages/core/.../extensions/tools/web_collect.py`).
- `WebModule` (`packages/core/.../modules/web/web_module.py`) discovers contributors during its lifespan
  (`_discover_contributors`), builds the navigation from `get_web_pages_config(language)` — **only for pages
  whose extension is loaded** — and renders page content via `get_web_page_content(page_id, request, language)`,
  introspecting each handler for `request` / `language` kwargs.

**Why this solves the core problem for free:** because the nav is assembled from loaded extensions only and
visibility is policy-filtered, moving functionality into the owning extension means it appears on exactly the
instances that load that extension. A catalog instance with no IAM shows STAC/Features/Assets sections with no
Admin present; the auth instance keeps Admin/IAM untouched.

A page handler returns HTML (a full content document, or an `is_embed=True` fragment injected into a parent page
such as `home`). Static CSS/JS is served per-extension under the `@expose_static` prefix. Reference
implementations: STAC `provide_stac_browser` serving `stac_browser.html` + `@expose_static("stac")`
(`packages/extensions/stac/.../stac_service.py:507`), and maps `provide_map_viewer` serving `map_viewer.html`
(`packages/extensions/maps/.../maps_service.py:227`).

## Design decisions

- **Rendering:** vanilla JS + static HTML, no build step (consistent with `stac_browser.html`, `map_viewer.html`).
- **Reuse:** reuse the **existing always-on shared layer** at `web/static/common/` rather than create a new toolkit.
  > **Deviation from the initial "shared toolkit in `extensions/tools`" decision, with rationale:** there is no
  > installable `extensions/tools` package that can serve static assets (the shared `tools` code lives in
  > `packages/core/.../extensions/tools/` and is Python-only). The real shared home is the `web` extension's
  > `static/common/` directory, which is `always_on` (present on every instance, including the no-IAM catalog on
  > `localhost:80`) and is already imported by the maps page and the admin pages. Reusing it satisfies the original
  > intent (one shared, reused location available on the catalog instance) without inventing a parallel toolkit.
  > **Constraint:** the parallel Admin/Configs refactor also depends on `common/` modules, so we **only add new files**
  > to `common/` and treat the existing shared modules as read-only.
- **Scope:** browse/navigate **plus** the named write actions — asset upload into a selected catalog/collection,
  create collection/catalog, create feature. No bulk-edit/delete-everything surface in v1 (delete is per-resource,
  policy-guarded).
- **Languages:** v1 ships en/fr/es (matches the existing language clamp); the i18n layer is structured so the UN-6
  set (en/fr/es/ar/zh/ru) drops in by adding dictionaries, no code change.
- **STAC:** enhance the existing `stac_browser` page; do not create a parallel one.

## Components

### 1. Shared web toolkit (foundation — lands first)

Host: `web` extension `static/common/` (served under the always-on `static` prefix; URL convention is the existing
`../common/<module>.js` relative import used by `admin/*.js` and the maps page). **Reuse what exists; add only the
two genuinely missing pieces.**

Already present and reused **read-only** (do not edit — the Admin refactor depends on them):

- `common/api.js` — `getJSON` / `postJSON` / `patchJSON` / `deleteJSON` / `fetchCatalogOptions`. The API client.
- `common/context-bar.js` — `mountContextBar`. The catalog/collection picker.
- `common/schema-form.js`, `common/schema-list.js`, `common/config-tabs.js`, `common/url.js`, `common/download.js`.
- `common/admin.css` — shared base styles.
- `components/language_selector.js` / `.css` — the existing language-switch UI.

New additive files (no collision with the Admin refactor):

- `common/i18n.js` — small string-dictionary helper: per-language dicts keyed by language code, reads the active
  language (from the language selector / `?language=`), `en` fallback, public `t(key)` helper. (Genuine gap — no JS
  string-i18n helper exists today.)
- `common/leaflet-map.js` — thin Leaflet (1.9.4, the version maps loads from unpkg) initialiser shared by the Features
  map and the STAC map preview, so neither inlines its own map bootstrap. (Maps currently inlines Leaflet; we do not
  touch maps — this is a new shared helper.)

The asset-upload flow helper is **assets-only**, so it stays in the assets section's own static dir (promote to
`common/` only if a second section needs it).

### 2. STAC section — enhance `stac_browser`

- Browse: catalogs → collections → items via `GET /stac/catalogs`, `/stac/catalogs/{cat}/collections`,
  `/stac/catalogs/{cat}/collections/{coll}/items`, `POST /stac/search`. Map preview + item detail.
- Write: simplified **Create catalog** / **Create collection** forms calling the STAC create routes. Buttons hidden
  client-side when policy disallows; the API enforces via `IamMiddleware` regardless.
- `language=` threaded to every call so localized titles/descriptions render.

### 3. Features section — new `features_browser`

- Browse collections/items as GeoJSON on a map (reuse the map library already vendored by maps/tiles — do **not**
  add a new map dependency). Simplified query controls: bbox (draw on map), datetime, limit.
- Write: **Create feature** form (draw geometry + key/value properties) → `POST
  /features/catalogs/{cat}/collections/{coll}/items`. Bulk ingest deferred.

### 4. Assets section — new `assets_manager`

- Shared catalog/collection picker → list/search assets (`GET /assets/catalogs/{cat}` and the collection variant;
  `POST /assets-search`).
- **Upload (headline action):** pick catalog (and optionally collection) → request an `UploadTicket` via
  `POST /assets/catalogs/{cat}/upload` or `POST /assets/catalogs/{cat}/collections/{coll}/upload` → PUT/POST the file
  to the returned URL+method → show the registered asset.
- Register virtual asset (external href) form. Per-asset delete, policy-guarded.

## Multilanguage

- Page titles/descriptions: localized dicts in `@expose_web_page` (en/fr/es).
- In-page strings: `i18n.js` dictionaries with `en` fallback.
- API calls pass `language=` so server-localized metadata matches the UI chrome.

## Authorization & separation of concerns

- Each page declares `audience_policy_id` (operator-rebindable via REST) rather than hardcoded role strings — the
  preferred path per the web-policy convention.
- Defense in depth: the UI hides actions the principal cannot perform; `IamMiddleware` enforces server-side
  regardless of the UI.
- No-IAM instances: anonymous policies apply, so the sections are visible and usable per the deployment config — the
  explicit goal of working with a catalog on `localhost:80` without Admin/IAM.
- **Admin/Configs untouched.** Where a generic widget (picker, table, upload) overlaps with something currently in
  Admin, it is reimplemented in the shared toolkit and consumed here; the Admin/Configs files are left to the parallel
  refactor. Overlap is recorded here rather than resolved by editing their code.

## Testing

Per section:

- Registration smoke test: the page appears in `WebModule.get_web_pages_config(...)` and its static files resolve via
  the static provider.
- Handler test: the page handler returns 200 with HTML content.

JS is vanilla/no-build, so there is no JS test harness; page logic stays thin and correctness rides on the
already-tested JSON APIs.

## Build sequence

1. **Phase 1 (serial):** shared toolkit under `tools` — foundation everything else imports.
2. **Phase 2 (parallel, 3-way):** STAC, Features, Assets sections — independent extension directories, no shared
   mutable state → suitable for parallel implementation agents.

## Resolved facts (verified in tree)

Concrete routes the sections call (handler names from each extension's route table):

- **STAC** (`/stac`): `GET /catalogs` (`list_stac_catalogs`), `GET /catalogs/{cat}/collections`
  (`list_stac_collections`), `POST /catalogs` (`create_stac_catalog`), `POST /catalogs/{cat}/collections`
  (`create_stac_collection`), `POST /catalogs/{cat}/collections/{coll}/items` (`add_stac_item`),
  `POST /catalogs/{cat}/search` (`search_items_post`).
- **Features** (`/features`): `POST /catalogs` (`create_catalog`), `GET /catalogs/{cat}/collections`
  (`list_collections_in_catalog`), `POST /catalogs/{cat}/collections` (`create_collection`),
  `GET /catalogs/{cat}/collections/{coll}/items` (`get_items`), `POST /catalogs/{cat}/collections/{coll}/items`
  (create feature).
- **Assets** (`/assets`): `GET /catalogs/{cat}`, `POST /assets-search`, `POST /catalogs/{cat}/upload` and
  `POST /catalogs/{cat}/collections/{coll}/upload` (returns an `UploadTicket` with URL + method), virtual-asset
  register, per-asset `DELETE`.

- **Map library:** Leaflet **1.9.4** loaded from unpkg (the version the maps page already uses, with SRI hashes).
- **Toolkit host:** the always-on `web/static/common/` layer (see Component 1). No `extensions/tools` static host
  exists; that decision is superseded with rationale above.
