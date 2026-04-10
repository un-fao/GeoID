#!/usr/bin/env bash
# Build JupyterLite static assets into the extension's static/lite/ directory.
#
# Prerequisites:
#   pip install jupyterlite-core jupyterlite-pyodide-kernel
#
# Usage:
#   bash src/dynastore/extensions/notebooks/jupyterlite/build.sh
#
# The output is written to static/lite/ which is served by the
# NotebooksExtension via @expose_static("lite") at:
#   /web/extension-static/lite/
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
STATIC_DIR="$(dirname "$SCRIPT_DIR")/static"
OUTPUT_DIR="$STATIC_DIR/lite"

# Preserve bridge.js (written manually, not part of the JupyterLite build)
BRIDGE_BACKUP=""
if [ -f "$OUTPUT_DIR/bridge.js" ]; then
    BRIDGE_BACKUP="$(mktemp)"
    cp "$OUTPUT_DIR/bridge.js" "$BRIDGE_BACKUP"
fi

echo "Building JupyterLite..."
echo "  Config dir: $SCRIPT_DIR"
echo "  Output dir: $OUTPUT_DIR"

jupyter lite build \
    --lite-dir "$SCRIPT_DIR" \
    --output-dir "$OUTPUT_DIR" \
    --contents ""

# Restore bridge.js
if [ -n "$BRIDGE_BACKUP" ] && [ -f "$BRIDGE_BACKUP" ]; then
    cp "$BRIDGE_BACKUP" "$OUTPUT_DIR/bridge.js"
    rm -f "$BRIDGE_BACKUP"
    echo "Restored bridge.js"
fi

echo "JupyterLite build complete: $OUTPUT_DIR"
echo "Serve at: /web/extension-static/lite/"
