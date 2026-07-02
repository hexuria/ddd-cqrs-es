#!/usr/bin/env bash
set -euo pipefail

if ! command -v jq >/dev/null 2>&1; then
  echo "Error: jq is required (install with your package manager)." >&2
  exit 1
fi

DOCS_JSON="docs/docs.json"

nav_pages=$(mktemp)
fs_pages=$(mktemp)
cleanup() {
  rm -f "$nav_pages" "$fs_pages"
}
trap cleanup EXIT

jq -r '.navigation.groups[].pages[]' "$DOCS_JSON" | sort | grep -v '^README$' > "$nav_pages"
find docs -type f -name '*.md' \
  | grep -v '/README\.md$' \
  | sed 's#^docs/##' \
  | sed 's#\.md$##' \
  | sort > "$fs_pages"

if ! diff -u "$nav_pages" "$fs_pages" > /tmp/verify-docs.diff; then
  echo "Documentation navigation mismatch detected."
  echo "Pages present in docs/ folder but missing in docs/docs.json:" >&2
  comm -23 "$fs_pages" "$nav_pages" >&2
  echo "Pages in docs/docs.json but not present on disk:" >&2
  comm -13 "$fs_pages" "$nav_pages" >&2
  echo "" >&2
  echo "See /tmp/verify-docs.diff for exact diff." >&2
  exit 1
fi

echo "docs.json navigation and docs/**/*.md are aligned."
