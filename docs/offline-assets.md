# Offline / Air-Gapped Asset Bundling

The web UI and JupyterLite kernel must run without outbound internet (on-prem
docker compose, air-gapped Cloud Run). Two build steps vendor every remote
asset into the repository so the runtime image is self-contained.

## 1. Web Fonts — `docker/scripts/vendor-web-fonts.sh`

Downloads FontAwesome 6.5.0 + Google Fonts (Inter, Fira Code, Montserrat) into
`src/dynastore/extensions/web/static/vendor/` and emits a single `vendor.css`
aggregator that HTML pages include in place of the old CDN `<link>` tags.

```bash
./docker/scripts/vendor-web-fonts.sh
```

Output layout:

```
src/dynastore/extensions/web/static/vendor/
├── vendor.css                 # aggregator (HTML pages point here)
├── fontawesome/
│   ├── all.min.css
│   └── webfonts/              # .woff2 + .ttf (brands/regular/solid/v4compat)
├── fonts/                     # Google Fonts woff2 (relative-rewritten)
├── inter.css
├── fira-code.css
└── montserrat.css
```

All HTML pages reference `<link rel="stylesheet" href="/web/static/vendor/vendor.css"/>`.
`swagger_common.css` imports `montserrat.css` directly.

The script uses a modern-browser User-Agent so Google Fonts serves `woff2`
instead of `ttf`; URLs inside each CSS are rewritten to `./fonts/<name>.woff2`.

## 2. JupyterLite — `docker/scripts/build-jupyterlite.sh` + Dockerfile STAGE 0

Pyodide kernel + piplite wheels are pre-built into
`src/dynastore/extensions/notebooks/static/lite/`. Requirements come from
`src/dynastore/extensions/notebooks/jupyterlite/requirements.txt`.

### Local build (dev)

```bash
./docker/scripts/build-jupyterlite.sh
```

Preserves `bridge.js` across rebuilds and injects the `<script>` tag into
`lab/index.html` after build.

### Docker build

Gated by `BUILD_JUPYTERLITE=true|false` (default **false**). Set to `true`
only for services that serve `/web/pages/notebooks`; workers and Cloud Run
Jobs keep it `false` to skip the ~250 MB Pyodide stage.

The `.github/actions/setup-env` action in `dynastore/` derives the flag from
the matrix type — `service` → `true`, everything else → `false` — so the
guarantee is enforced at CI build time (unit-tested in
`dynastore/.github/scripts/test_build_env.py`).

### Offline piplite

`jupyter-lite.json` sets `disablePyPIFallback: true`, and
`jupyter_lite_config.json` pins `PipliteAddon.piplite_urls: []`. Wheels are
fed via the `--piplite-wheels=<path>` CLI flag (one per `.whl`) so the built
site never contacts PyPI.

## Adding a new font or JS library

1. Extend `docker/scripts/vendor-web-fonts.sh` (another `fetch_google_font` call, or
   add a new CDN download alongside FontAwesome).
2. Append the CSS to the `vendor.css` aggregator.
3. Re-run the script and commit the regenerated `vendor/` directory.
