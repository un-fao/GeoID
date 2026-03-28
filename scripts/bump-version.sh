#!/bin/bash
# Bump the semantic version in the VERSION file.
#
# Usage:
#   ./scripts/bump-version.sh          # patch +1 (auto-rolls minor at 99)
#   ./scripts/bump-version.sh major    # x+1.0.0
set -euo pipefail

VERSION_FILE="$(git rev-parse --show-toplevel 2>/dev/null || dirname "$(cd "$(dirname "$0")/.." && pwd)")/VERSION"

if [ ! -f "$VERSION_FILE" ]; then
    echo "ERROR: VERSION file not found at $VERSION_FILE" >&2
    exit 1
fi

CURRENT=$(cat "$VERSION_FILE")
IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT"

case "${1:-}" in
    major)
        MAJOR=$((MAJOR + 1))
        MINOR=0
        PATCH=0
        ;;
    "")
        # Default: patch +1, auto-roll minor when patch reaches 99
        PATCH=$((PATCH + 1))
        if [ "$PATCH" -gt 99 ]; then
            MINOR=$((MINOR + 1))
            PATCH=0
        fi
        ;;
    *)
        echo "Usage: $0 [major]" >&2
        echo "  No args = patch +1 (auto-rolls minor at 99)" >&2
        echo "  major   = x+1.0.0" >&2
        exit 1
        ;;
esac

NEW_VERSION="${MAJOR}.${MINOR}.${PATCH}"
printf '%s' "$NEW_VERSION" > "$VERSION_FILE"
echo "Version bumped: ${CURRENT} -> ${NEW_VERSION}"
