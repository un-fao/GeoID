# Per-extension Web Sections Implementation Plan

> **For agentic workers:** Implement task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Work on the
> feature branch for this change. Run Python from `.venv/bin/python` with `PYTHONPATH` covering `packages/*/src`
> **and** `packages/extensions/*/src`.

**Goal:** Give STAC, OGC Features, and Assets their own multilingual web sections, built on the existing
`@expose_web_page` / `WebPageContributor` pattern and the always-on `web/static/common/` shared layer, so each section
follows its extension onto whatever instance loads it (no Admin/IAM dependency).

**Architecture:** No core changes. Each extension service gains a page handler (`@expose_web_page`) returning a static
HTML page served via its `@expose_static` prefix; pages reuse `common/api.js` (API client) + `common/context-bar.js`
(catalog/collection picker) and two new additive shared helpers (`common/i18n.js`, `common/leaflet-map.js`). All data
flows through the existing JSON APIs with `language=` threaded for localization.

**Tech Stack:** Python 3.12 / FastAPI services; vanilla ES-module JS (no build step); Leaflet 1.9.4 (unpkg CDN);
pytest.

**Design doc:** `docs/design/2026-06-09-per-extension-web-sections.md`.

**Hard constraints:**
- Do **not** edit Configs UI or Admin UI files (`web/static/admin/*`, configs services) — a parallel refactor owns them.
- Do **not** edit existing `web/static/common/*` modules — only **add** new files there.
- Do **not** add AI-context paths/skill/agent names to any tracked file or commit message.
- No `Co-Authored-By` / AI attribution in commits.

---

## File Structure

| File | Responsibility |
|---|---|
| `packages/extensions/web/.../static/common/i18n.js` | NEW shared string-dictionary i18n helper (`t(key)`, lang detection, `en` fallback) |
| `packages/extensions/web/.../static/common/leaflet-map.js` | NEW shared Leaflet initialiser (basemap + helpers) used by Features & STAC |
| `packages/extensions/stac/.../static/stac_browser.html` | EXISTING page — extend with create-catalog/collection forms + i18n |
| `packages/extensions/stac/.../static/stac_browser.js` | NEW page logic (browse + create), imports shared helpers |
| `packages/extensions/features/.../static/features_browser.html` | NEW Features page shell |
| `packages/extensions/features/.../static/features_browser.js` | NEW Features page logic (map browse + create feature) |
| `packages/extensions/features/.../features_service.py` | ADD `@expose_static("features")` + `@expose_web_page` handler + `get_web_pages`/`get_static_assets` |
| `packages/extensions/assets/.../static/assets_manager.html` | NEW Assets page shell |
| `packages/extensions/assets/.../static/assets_manager.js` | NEW Assets page logic (list/search + upload + virtual + delete) |
| `packages/extensions/assets/.../static/upload.js` | NEW assets-only upload helper (UploadTicket → PUT/POST) |
| `packages/extensions/assets/.../assets_service.py` | ADD `@expose_static("assets")` + `@expose_web_page` handler + `get_web_pages`/`get_static_assets` |
| `tests/.../web/test_web_sections_registration.py` | NEW registration + handler tests for all three sections |

---

## Phase 0 — Shared foundation (serial, lands first)

These two files are auto-served by the existing web `static` provider (it walks `static/**`), so **no Python change**
is needed for hosting; pages import them via `../common/<file>.js`.

### Task 0.1: Shared i18n helper

**Files:**
- Create: `packages/extensions/web/src/dynastore/extensions/web/static/common/i18n.js`

- [ ] **Step 1: Write the helper**

```javascript
// common/i18n.js — minimal string-dictionary i18n shared across extension web sections.
// Dictionaries are keyed by language code; missing keys fall back to English, then to the key itself.
// Active language resolution order: explicit setLang() > URL ?language= > <html lang> > "en".

const SUPPORTED = ["en", "fr", "es"]; // v1; add codes here to extend (e.g. ar, zh, ru)
let _lang = null;
let _dict = {};

export function detectLang() {
  const url = new URLSearchParams(window.location.search).get("language");
  if (url && SUPPORTED.includes(url)) return url;
  const htmlLang = (document.documentElement.lang || "").slice(0, 2);
  if (SUPPORTED.includes(htmlLang)) return htmlLang;
  return "en";
}

export function setLang(lang) {
  _lang = SUPPORTED.includes(lang) ? lang : "en";
}

export function lang() {
  if (!_lang) _lang = detectLang();
  return _lang;
}

// register({en:{key:val}, fr:{...}}) merges a section's dictionaries.
export function register(dicts) {
  for (const code of Object.keys(dicts)) {
    _dict[code] = Object.assign({}, _dict[code] || {}, dicts[code]);
  }
}

export function t(key, vars) {
  const code = lang();
  let s = (_dict[code] && _dict[code][key]) ?? (_dict.en && _dict.en[key]) ?? key;
  if (vars) for (const k of Object.keys(vars)) s = s.replace(new RegExp(`\\{${k}\\}`, "g"), vars[k]);
  return s;
}

export { SUPPORTED };
```

- [ ] **Step 2: Sanity-check syntax**

Run: `cd packages/extensions/web/src/dynastore/extensions/web/static && node --check common/i18n.js`
Expected: no output (exit 0). If `node` unavailable, skip — syntax is verified at runtime by the handler test loading the page.

- [ ] **Step 3: Commit**

```bash
git add packages/extensions/web/src/dynastore/extensions/web/static/common/i18n.js
git commit -m "feat(web): add shared i18n string helper for extension sections"
```

### Task 0.2: Shared Leaflet map helper

**Files:**
- Create: `packages/extensions/web/src/dynastore/extensions/web/static/common/leaflet-map.js`

- [ ] **Step 1: Write the helper** (Leaflet global `L` is loaded by the page's `<script>` tag, as in `maps/static/map_viewer.html`)

```javascript
// common/leaflet-map.js — shared Leaflet initialiser. The page must load Leaflet 1.9.4 (unpkg) before importing this.

const BASEMAP = "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png";
const ATTRIB = '&copy; OpenStreetMap, &copy; CARTO';

// initMap(elId) -> Leaflet map centred on the world.
export function initMap(elId) {
  const map = L.map(elId, { worldCopyJump: true }).setView([20, 0], 2);
  L.tileLayer(BASEMAP, { attribution: ATTRIB, subdomains: "abcd", maxZoom: 19 }).addTo(map);
  return map;
}

// showGeoJSON(map, layerRef, geojson) -> replaces layerRef.current with a new GeoJSON layer, fits bounds.
export function showGeoJSON(map, layerRef, geojson) {
  if (layerRef.current) { map.removeLayer(layerRef.current); layerRef.current = null; }
  if (!geojson) return;
  const layer = L.geoJSON(geojson);
  layer.addTo(map);
  layerRef.current = layer;
  try { const b = layer.getBounds(); if (b.isValid()) map.fitBounds(b, { maxZoom: 12 }); } catch (e) { /* empty geometry */ }
}

// bboxFromMap(map) -> "minx,miny,maxx,maxy" for the current view, for OGC bbox= params.
export function bboxFromMap(map) {
  const b = map.getBounds();
  return [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()].map((n) => n.toFixed(6)).join(",");
}
```

- [ ] **Step 2: Sanity-check syntax**

Run: `cd packages/extensions/web/src/dynastore/extensions/web/static && node --check common/leaflet-map.js`
Expected: exit 0 (or skip if no node).

- [ ] **Step 3: Commit**

```bash
git add packages/extensions/web/src/dynastore/extensions/web/static/common/leaflet-map.js
git commit -m "feat(web): add shared Leaflet map helper for extension sections"
```

---

## Phase 1 — STAC section (enhance existing `stac_browser`)

STAC already has `@expose_web_page(page_id="stac_browser", ...)` (`stac_service.py:507`) serving `stac_browser.html`,
and `@expose_static("stac")` (`stac_service.py:374`). We extend the page; the Python registration is unchanged.

### Task 1.1: STAC browser page logic + create forms

**Files:**
- Create: `packages/extensions/stac/src/dynastore/extensions/stac/static/stac_browser.js`
- Modify: `packages/extensions/stac/src/dynastore/extensions/stac/static/stac_browser.html`

- [ ] **Step 1:** Add `stac_browser.js` (ES module). It MUST, using only the shared helpers + STAC JSON API:
  - `import { getJSON, postJSON } from "../common/api.js";`
  - `import { register, t, lang } from "../common/i18n.js";`
  - `import { initMap, showGeoJSON } from "../common/leaflet-map.js";`
  - `register({...})` with en/fr/es dicts for every UI label used.
  - Browse flow: `getJSON('/stac/catalogs?language='+lang())` → list catalogs; on select,
    `getJSON('/stac/catalogs/{cat}/collections?language='+lang())` → list collections; on select,
    `getJSON('/stac/catalogs/{cat}/collections/{coll}/items?language='+lang())` → item table; on item select, show
    item geometry via `showGeoJSON` and a property panel.
  - Create flow (buttons hidden unless allowed — see Step 3): **Create catalog** form → `postJSON('/stac/catalogs', body)`;
    **Create collection** form → `postJSON('/stac/catalogs/{cat}/collections', body)`. On 2xx refresh the relevant list;
    on 401/403 show a permission notice via `t('err.forbidden')`.
  - All fetches go through `getJSON`/`postJSON` so error handling is uniform.

- [ ] **Step 2:** Edit `stac_browser.html`:
  - Keep the existing `{{VERSION}}` token (substituted server-side by `_serve_page_template`).
  - Add Leaflet `<link>`+`<script>` (1.9.4 unpkg with SRI, copy exactly from `maps/static/map_viewer.html:1-2`).
  - Add containers: catalog list, collection list, item table, `#stac-map`, item detail panel, and the two create
    forms (initially `hidden`).
  - `<link rel="stylesheet" href="../static/common/admin.css">` for base styling (read-only reuse).
  - End with `<script type="module" src="../static/stac/stac_browser.js"></script>`.

- [ ] **Step 3:** Gate create buttons client-side: call `getJSON('/auth/me')` if present; if it 404s or returns no
  write role, keep create forms hidden. (The API enforces regardless; this is cosmetic.) If `/auth/me` is unavailable
  on a no-IAM instance, treat as anonymous and follow the deployment default (show forms; the API still authorizes).

- [ ] **Step 4: Verify the page loads** (handler test added in Task 4.1 covers HTTP 200 + HTML). Manual check optional.

- [ ] **Step 5: Commit**

```bash
git add packages/extensions/stac/src/dynastore/extensions/stac/static/stac_browser.js \
        packages/extensions/stac/src/dynastore/extensions/stac/static/stac_browser.html
git commit -m "feat(stac): browse + create catalogs/collections in the STAC browser web section"
```

---

## Phase 2 — Features section (new `features_browser`)

`features_service.py` has no web page today. Add the contribution. Reference the STAC service for the exact
`get_web_pages`/`get_static_assets`/`_serve_page_template` shape (`stac_service.py:374,507,516`).

### Task 2.1: Features service web contribution

**Files:**
- Modify: `packages/extensions/features/src/dynastore/extensions/features/features_service.py`

- [ ] **Step 1: Add imports + methods to the service class.** Place near the other handlers:

```python
# at top of file (with the other imports)
import os
from dynastore.extensions.web.decorators import expose_web_page, expose_static
from dynastore.extensions.tools.web_collect import collect_web_pages, collect_static_assets
from fastapi import Request, Response
from dynastore._version import VERSION


# inside the Features service class:
def get_web_pages(self):
    return collect_web_pages(self)

def get_static_assets(self):
    return collect_static_assets(self)

@expose_static("features")
def provide_static_files(self) -> list[str]:
    static_dir = os.path.join(os.path.dirname(__file__), "static")
    files = []
    for root, _, filenames in os.walk(static_dir):
        for filename in filenames:
            files.append(os.path.join(root, filename))
    return files

@expose_web_page(
    page_id="features_browser",
    title={"en": "Features", "fr": "Entités", "es": "Entidades"},
    icon="fa-draw-polygon",
    description={
        "en": "Browse and create vector features on a map.",
        "fr": "Explorer et créer des entités vectorielles sur une carte.",
        "es": "Explorar y crear entidades vectoriales en un mapa.",
    },
)
async def provide_features_browser(self, request: Request):
    return await self._serve_page_template("features_browser.html")

async def _serve_page_template(self, filename: str):
    file_path = os.path.join(os.path.dirname(__file__), "static", filename)
    if not os.path.exists(file_path):
        return Response(content=f"Template {filename} not found", status_code=404)
    with open(file_path, "r", encoding="utf-8") as f:
        return Response(content=f.read().replace("{{VERSION}}", VERSION), media_type="text/html")
```

> If the class already defines any of these (`get_web_pages`, `_serve_page_template`, an `os` import), reuse the
> existing one instead of duplicating. Verify with `grep -n "get_web_pages\|_serve_page_template\|^import os" features_service.py`.

- [ ] **Step 2: Commit**

```bash
git add packages/extensions/features/src/dynastore/extensions/features/features_service.py
git commit -m "feat(features): register Features web section page + static provider"
```

### Task 2.2: Features page (map browse + create feature)

**Files:**
- Create: `packages/extensions/features/.../static/features_browser.html`
- Create: `packages/extensions/features/.../static/features_browser.js`

- [ ] **Step 1:** `features_browser.html` — same shell pattern as `stac_browser.html` (Leaflet link/script,
  `{{VERSION}}`, `admin.css`), with a `#features-map`, a catalog/collection picker mount, an items list, a
  "Create feature" panel, and `<script type="module" src="../static/features/features_browser.js"></script>`.

- [ ] **Step 2:** `features_browser.js` MUST:
  - `import { getJSON, postJSON } from "../common/api.js";`
  - `import { mountContextBar } from "../common/context-bar.js";` (catalog/collection picker — reuse, do not rebuild)
  - `import { register, t, lang } from "../common/i18n.js";`
  - `import { initMap, showGeoJSON, bboxFromMap } from "../common/leaflet-map.js";`
  - Browse: on catalog+collection selection, `getJSON('/features/catalogs/{cat}/collections/{coll}/items?limit=50&bbox='+bboxFromMap(map)+'&language='+lang())`,
    render features via `showGeoJSON` and a list.
  - Create feature: a form to draw/enter a point (lon/lat) + key/value properties →
    `postJSON('/features/catalogs/{cat}/collections/{coll}/items', {type:'Feature', geometry:{...}, properties:{...}})`.
    On 2xx refresh the items; on 401/403 show `t('err.forbidden')`.

- [ ] **Step 3: Commit**

```bash
git add packages/extensions/features/src/dynastore/extensions/features/static/features_browser.html \
        packages/extensions/features/src/dynastore/extensions/features/static/features_browser.js
git commit -m "feat(features): map-based browse and create-feature web page"
```

---

## Phase 3 — Assets section (new `assets_manager`)

`assets_service.py` has no web page today. Mirror the Features contribution shape.

### Task 3.1: Assets service web contribution

**Files:**
- Modify: `packages/extensions/assets/src/dynastore/extensions/assets/assets_service.py`

- [ ] **Step 1:** Add the same five members as Task 2.1 Step 1, with `@expose_static("assets")` and:

```python
@expose_web_page(
    page_id="assets_manager",
    title={"en": "Assets", "fr": "Ressources", "es": "Recursos"},
    icon="fa-folder-open",
    description={
        "en": "Upload, browse, and manage catalog and collection assets.",
        "fr": "Téléverser, explorer et gérer les ressources.",
        "es": "Cargar, explorar y gestionar recursos.",
    },
)
async def provide_assets_manager(self, request: Request):
    return await self._serve_page_template("assets_manager.html")
```

(plus `get_web_pages`, `get_static_assets`, `provide_static_files` for prefix `"assets"`, `_serve_page_template`,
and the imports — reuse any that already exist; verify with grep as in Task 2.1.)

- [ ] **Step 2: Commit**

```bash
git add packages/extensions/assets/src/dynastore/extensions/assets/assets_service.py
git commit -m "feat(assets): register Assets web section page + static provider"
```

### Task 3.2: Assets page (list/search + upload + virtual + delete)

**Files:**
- Create: `packages/extensions/assets/.../static/assets_manager.html`
- Create: `packages/extensions/assets/.../static/assets_manager.js`
- Create: `packages/extensions/assets/.../static/upload.js`

- [ ] **Step 1:** `upload.js` — assets-only helper:

```javascript
// upload.js — request an UploadTicket then deliver the file to the backend-specified URL/method.
import { postJSON } from "../common/api.js";

// uploadAsset(cat, coll, file) -> registered-asset JSON (or throws).
export async function uploadAsset(cat, coll, file) {
  const base = coll
    ? `/assets/catalogs/${cat}/collections/${coll}/upload`
    : `/assets/catalogs/${cat}/upload`;
  const ticket = await postJSON(base, { filename: file.name, content_type: file.type || "application/octet-stream" });
  // ticket: { url, method, headers? } — PUT for GCS/S3, POST (multipart) for local.
  const method = (ticket.method || "PUT").toUpperCase();
  let res;
  if (method === "POST") {
    const fd = new FormData();
    fd.append("file", file, file.name);
    res = await fetch(ticket.url, { method, body: fd, headers: ticket.headers || {} });
  } else {
    res = await fetch(ticket.url, { method, body: file, headers: ticket.headers || {} });
  }
  if (!res.ok) throw new Error(`upload failed: ${res.status}`);
  return ticket;
}
```

> Verify the `UploadTicket` field names (`url`, `method`, `headers`, request body keys) against the upload route in
> `assets_service.py` (~line 462-499) before finalizing; adjust the keys to match the real model.

- [ ] **Step 2:** `assets_manager.html` — shell pattern (no Leaflet needed), `admin.css`, context-bar mount, an
  asset table, an upload form (`<input type="file">` + upload button), a virtual-asset form, and
  `<script type="module" src="../static/assets/assets_manager.js"></script>`.

- [ ] **Step 3:** `assets_manager.js` MUST:
  - `import { getJSON, postJSON, deleteJSON } from "../common/api.js";`
  - `import { mountContextBar } from "../common/context-bar.js";`
  - `import { register, t, lang } from "../common/i18n.js";`
  - `import { uploadAsset } from "../static/assets/upload.js";` (or relative `./upload.js`)
  - List/search: on catalog (+optional collection) selection, `getJSON('/assets/catalogs/{cat}?language='+lang())`
    (or the collection variant); render an asset table.
  - Upload: on submit, `await uploadAsset(cat, coll, fileInput.files[0])`, then refresh the table; show `t('upload.ok')`.
  - Virtual asset: form → `postJSON('/assets/catalogs/{cat}/virtual-assets', body)`.
  - Delete: per-row → `deleteJSON('/assets/catalogs/{cat}/assets/{id}')`, refresh; 401/403 → `t('err.forbidden')`.

- [ ] **Step 4: Commit**

```bash
git add packages/extensions/assets/src/dynastore/extensions/assets/static/assets_manager.html \
        packages/extensions/assets/src/dynastore/extensions/assets/static/assets_manager.js \
        packages/extensions/assets/src/dynastore/extensions/assets/static/upload.js
git commit -m "feat(assets): upload/browse/manage web page for the Assets section"
```

---

## Phase 4 — Tests (TDD-verifiable surface)

The JS UIs have no harness; verification rides on the JSON APIs. The Python-testable surface is: each section's page
**registers** (appears in `WebModule.get_web_pages_config`) and its handler **returns 200 HTML**.

### Task 4.1: Registration + handler tests

**Files:**
- Create: `tests/extensions/web/test_web_sections_registration.py` (mirror the path of existing web tests — verify the
  real test dir with `find tests -path '*web*' -name 'test_*.py' | head`)

- [ ] **Step 1: Write the failing tests**

```python
import pytest
from dynastore.modules.web.web_module import WebModule


def _register(contributor, web_module):
    for spec in contributor.get_web_pages():
        web_module.register_web_page(spec.to_config(), spec.handler)


@pytest.mark.parametrize(
    "import_path, cls_name, page_id, prefix",
    [
        ("dynastore.extensions.features.features_service", "OGCFeaturesService", "features_browser", "features"),
        ("dynastore.extensions.assets.assets_service", "AssetsService", "assets_manager", "assets"),
        ("dynastore.extensions.stac.stac_service", "STACService", "stac_browser", "stac"),
    ],
)
def test_section_page_registers(import_path, cls_name, page_id, prefix):
    import importlib
    mod = importlib.import_module(import_path)
    cls = getattr(mod, cls_name)
    svc = cls.__new__(cls)  # avoid full __init__; we only exercise the decorator-collection methods
    pages = {s.page_id for s in svc.get_web_pages()}
    assert page_id in pages
    prefixes = {a.prefix.strip("/") for a in svc.get_static_assets()}
    assert prefix in prefixes
```

> Confirm the exact class names with
> `grep -n "^class .*Service" packages/extensions/{features,assets,stac}/src/dynastore/extensions/*/*_service.py`
> and fix the parametrize tuples. If `cls.__new__` trips on attribute access inside `get_web_pages`, instantiate via
> the project's existing service-construction test fixture instead (look at how other web-page tests build a service).

- [ ] **Step 2: Run, expect failures for features/assets (pages not yet registered if run before Phases 2-3)**

Run:
```bash
.venv/bin/python -m pytest tests/extensions/web/test_web_sections_registration.py -p no:randomly -n0 -v
```
Expected before Phases 2-3: features/assets FAIL (no `get_web_pages`); STAC PASS. After Phases 2-3: all PASS.

- [ ] **Step 3: After Phases 1-3 complete, run again to green**

Run the same command. Expected: 3 passed.

- [ ] **Step 4: Handler returns HTML** — add to the same file:

```python
import asyncio
import inspect


@pytest.mark.parametrize(
    "import_path, cls_name, handler_name",
    [
        ("dynastore.extensions.features.features_service", "OGCFeaturesService", "provide_features_browser"),
        ("dynastore.extensions.assets.assets_service", "AssetsService", "provide_assets_manager"),
        ("dynastore.extensions.stac.stac_service", "STACService", "provide_stac_browser"),
    ],
)
def test_section_handler_returns_html(import_path, cls_name, handler_name):
    import importlib
    mod = importlib.import_module(import_path)
    svc = getattr(mod, cls_name).__new__(getattr(mod, cls_name))
    handler = getattr(svc, handler_name)
    result = handler(request=None) if "request" in inspect.signature(handler).parameters else handler()
    if inspect.isawaitable(result):
        result = asyncio.get_event_loop().run_until_complete(result)
    body = result.body.decode() if hasattr(result, "body") else str(result)
    assert result.status_code == 200
    assert "<" in body and "{{VERSION}}" not in body  # template token substituted
```

- [ ] **Step 5: Run full file green**

Run:
```bash
.venv/bin/python -m pytest tests/extensions/web/test_web_sections_registration.py -p no:randomly -n0 -v
```
Expected: all passed.

- [ ] **Step 6: Commit**

```bash
git add tests/extensions/web/test_web_sections_registration.py
git commit -m "test(web): registration + handler tests for STAC/Features/Assets web sections"
```

---

## Phase 5 — Integration verification & sweep

- [ ] **Step 1: Three-repo sync sweep.** Grep `dynastore/` and `fao-aip-catalog/` for assumptions broken by the new
  static prefixes / page ids. These are additive web pages with no SCOPE/Dockerfile/import changes, so expect clean;
  record the result.

- [ ] **Step 2: Lint the Python edits.**

Run: `uv tool run ruff check packages/extensions/features packages/extensions/assets packages/extensions/stac`
Fix anything reported (use a zsh array for the file list — unquoted `$FILES` does not word-split in zsh).

- [ ] **Step 3: Full targeted test run** (baseline-diff if anything fails to separate env gaps from real breakage):

Run:
```bash
PYTHONPATH="$(pwd)/packages/core/src:$(pwd)/packages/extensions/web/src:$(pwd)/packages/extensions/stac/src:$(pwd)/packages/extensions/features/src:$(pwd)/packages/extensions/assets/src" \
  .venv/bin/python -m pytest tests/extensions/web/test_web_sections_registration.py -p no:randomly -n0 -v
```
Expected: all passed.

- [ ] **Step 4:** Open the PR (base `main`) summarizing the three sections, the shared-toolkit reuse, and the
  Admin/Configs out-of-scope boundary. No AI attribution in the body.

---

## Self-Review (author checklist — completed)

- **Spec coverage:** toolkit reuse (Phase 0) ✓; STAC browse+create (Phase 1) ✓; Features browse+create (Phase 2) ✓;
  Assets upload/list/virtual/delete (Phase 3) ✓; multilingual via i18n.js + localized page titles + `language=` ✓;
  policy/separation via existing middleware + `audience` (pages anonymous-visible by default; tighten via
  `audience_policy_id` if a policy id is provided) ✓; tests (Phase 4) ✓.
- **Placeholder scan:** none — code provided for helpers, Python members, tests; UI markup specified against named
  reference files with exact API calls and imports.
- **Type/name consistency:** shared helper exports (`getJSON/postJSON/deleteJSON`, `mountContextBar`, `register/t/lang`,
  `initMap/showGeoJSON/bboxFromMap`, `uploadAsset`) are referenced identically across tasks.
- **Open verifications flagged inline:** service class names, `UploadTicket` field names, the real test directory, and
  whether `cls.__new__` suffices vs. a construction fixture — each task says exactly what to grep/confirm.
