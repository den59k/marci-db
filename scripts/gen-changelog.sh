#!/usr/bin/env bash
#
# Generate a Markdown changelog from conventional-commit messages in a git range.
#
# Usage:
#   scripts/gen-changelog.sh [<range>]
#
#   <range>  any git revision range, e.g. v0.2.2..HEAD or v0.0.6..v0.2.3.
#            Defaults to "<last-tag>..HEAD", or the full history if there is no tag.
#
# Output goes to stdout. Commits are grouped by their conventional-commit type
# (feat:, fix:, refactor:, ...); anything that doesn't match falls under
# "Other Changes". Merge commits are skipped.
set -euo pipefail

range="${1:-}"
if [ -z "$range" ]; then
  last_tag="$(git describe --tags --abbrev=0 2>/dev/null || true)"
  if [ -n "$last_tag" ]; then range="${last_tag}..HEAD"; else range="HEAD"; fi
fi

# All known conventional-commit types, used to detect the "Other Changes" bucket.
known_re='^(feat|fix|perf|refactor|docs|test|build|ci|chore|style|revert)(\(.+\))?!?:'

emit_section() {
  local type="$1" title="$2"
  local lines
  lines="$(git log "$range" --no-merges --pretty=format:'%s|%h' \
    | grep -E "^${type}(\(.+\))?!?:" || true)"
  [ -z "$lines" ] && return 0
  printf '### %s\n\n' "$title"
  while IFS='|' read -r subject hash; do
    [ -z "$subject" ] && continue
    # strip the "type(scope)!: " prefix, keep the human-readable message
    local msg
    msg="$(printf '%s' "$subject" | sed -E "s/^${type}(\(.+\))?!?:[[:space:]]*//")"
    printf -- '- %s (%s)\n' "$msg" "$hash"
  done <<< "$lines"
  printf '\n'
}

emit_section "feat"     "Features"
emit_section "fix"      "Bug Fixes"
emit_section "perf"     "Performance"
emit_section "refactor" "Refactoring"
emit_section "docs"     "Documentation"
emit_section "test"     "Tests"
emit_section "build"    "Build System"
emit_section "ci"       "CI"
emit_section "chore"    "Chores"
emit_section "style"    "Styles"
emit_section "revert"   "Reverts"

# Everything that isn't a recognised conventional commit.
others="$(git log "$range" --no-merges --pretty=format:'%s|%h' \
  | grep -Ev "$known_re" || true)"
if [ -n "$others" ]; then
  printf '### Other Changes\n\n'
  while IFS='|' read -r subject hash; do
    [ -z "$subject" ] && continue
    printf -- '- %s (%s)\n' "$subject" "$hash"
  done <<< "$others"
  printf '\n'
fi
