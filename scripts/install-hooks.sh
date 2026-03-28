#!/bin/bash
# Install local git hooks from scripts/hooks/ into .git/hooks/
# Run once after cloning: ./scripts/install-hooks.sh
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
HOOKS_SRC="${REPO_ROOT}/scripts/hooks"
HOOKS_DST="${REPO_ROOT}/.git/hooks"

if [ ! -d "$HOOKS_SRC" ]; then
    echo "ERROR: hooks source directory not found: $HOOKS_SRC" >&2
    exit 1
fi

for hook in "$HOOKS_SRC"/*; do
    hook_name="$(basename "$hook")"
    target="${HOOKS_DST}/${hook_name}"

    # Back up existing hook if it's not already a symlink to our hooks
    if [ -f "$target" ] && [ ! -L "$target" ]; then
        echo "Backing up existing ${hook_name} -> ${hook_name}.bak"
        mv "$target" "${target}.bak"
    fi

    ln -sf "$hook" "$target"
    chmod +x "$hook"
    echo "Installed: ${hook_name}"
done

echo "Git hooks installed successfully."
