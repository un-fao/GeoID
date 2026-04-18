#!/usr/bin/env bash
# Build JupyterLite assets locally (without Docker) and inject bridge.js.
# Useful when running the API from source and you want the Run button on
# /web/pages/notebooks to work.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
LITE_CONFIG="${REPO_DIR}/src/dynastore/extensions/notebooks/jupyterlite"
LITE_OUT="${REPO_DIR}/src/dynastore/extensions/notebooks/static/lite"

if ! command -v jupyter >/dev/null 2>&1; then
    echo "Installing jupyterlite-core + pyodide-kernel into current env…"
    pip install jupyterlite-core jupyterlite-pyodide-kernel
fi

WHEELS_DIR="${LITE_CONFIG}/wheels"
mkdir -p "${WHEELS_DIR}"
if [ -s "${LITE_CONFIG}/requirements.txt" ]; then
    echo "Pre-downloading piplite wheels into ${WHEELS_DIR}…"
    pip download --no-deps --only-binary=:all: \
        --dest "${WHEELS_DIR}" -r "${LITE_CONFIG}/requirements.txt" \
        || echo "WARN: wheel pre-download partial; packages will fall back to piplite CDN"
fi

# ogc-dimensions: not on PyPI — build from sibling checkout if present.
OGC_DIM_SRC="${REPO_DIR}/../ogc-dimensions/reference-implementation"
if [ -d "${OGC_DIM_SRC}" ]; then
    echo "Building ogc-dimensions wheel from ${OGC_DIM_SRC}…"
    rm -f "${WHEELS_DIR}"/ogc_dimensions-*.whl
    pip wheel --no-deps --wheel-dir "${WHEELS_DIR}" "${OGC_DIM_SRC}" \
        || echo "WARN: ogc-dimensions wheel build failed; notebooks using it will error in-browser"
fi

echo "Building JupyterLite into ${LITE_OUT}…"
# Preserve bridge.js across rebuilds.
cp "${LITE_OUT}/bridge.js" "${LITE_OUT}/../bridge.js.bak" 2>/dev/null || true

WHEEL_ARGS=()
for w in "${WHEELS_DIR}"/*.whl; do
    # piplite resolves relative paths against its own cwd; always pass absolute.
    [ -e "$w" ] && WHEEL_ARGS+=("--piplite-wheels=$(cd "$(dirname "$w")" && pwd)/$(basename "$w")")
done
jupyter lite build --lite-dir "${LITE_CONFIG}" --output-dir "${LITE_OUT}" ${WHEEL_ARGS[@]+"${WHEEL_ARGS[@]}"}

mv "${LITE_OUT}/../bridge.js.bak" "${LITE_OUT}/bridge.js" 2>/dev/null || true

if [ -f "${LITE_OUT}/lab/index.html" ] && ! grep -q "bridge.js" "${LITE_OUT}/lab/index.html"; then
    sed -i.bak 's#</head>#<script src="../bridge.js"></script></head>#' "${LITE_OUT}/lab/index.html"
    rm -f "${LITE_OUT}/lab/index.html.bak"
fi

echo "Done. Restart the service to pick up the new assets."
