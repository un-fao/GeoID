#!/bin/bash
# Install git hooks from scripts/hooks/ into .git/hooks/
set -euo pipefail
REPO_ROOT="$(git rev-parse --show-toplevel)"
ln -sf "$REPO_ROOT/scripts/hooks/pre-commit" "$REPO_ROOT/.git/hooks/pre-commit"
chmod +x "$REPO_ROOT/scripts/hooks/pre-commit"
echo "Hooks installed."
