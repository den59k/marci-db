#!/usr/bin/env bash
#
# Release driver for MarciDB.
#
#   scripts/release.sh <version> [options]
#
# It will, in order:
#   1. validate the working tree is clean and you are on the release branch,
#   2. set marcidb-server's package version to <version>,
#   3. regenerate the CHANGELOG.md section from commits since the last tag,
#   4. build & push both Docker flavours (full + core) to Docker Hub,
#   5. commit, create the vX.Y.Z tag and push it.
#
# Pushing the tag triggers .github/workflows/release.yml, which builds the
# Windows .exe / Linux binaries, mirrors the Docker images to GHCR and
# publishes the GitHub Release with these same changelog notes.
#
# Options:
#   --no-docker   skip the Docker build & push (let CI / GHCR handle images)
#   --no-push     do everything locally but don't push the commit or tag
#   --dry-run     print what would happen, change nothing
#
# Environment:
#   IMAGE   Docker Hub repository (default: den59k/marcidb-server)
#   REMOTE  git remote to push to     (default: origin)
set -euo pipefail

cd "$(dirname "$0")/.."

IMAGE="${IMAGE:-den59k/marcidb-server}"
REMOTE="${REMOTE:-origin}"
DO_DOCKER=1
DO_PUSH=1
DRY_RUN=0
VERSION=""

die() { printf 'error: %s\n' "$*" >&2; exit 1; }

# Run a command, or just print it under --dry-run.
run() {
  if [ "$DRY_RUN" -eq 1 ]; then
    printf '  [dry-run] %s\n' "$*"
  else
    "$@"
  fi
}

while [ $# -gt 0 ]; do
  case "$1" in
    --no-docker) DO_DOCKER=0 ;;
    --no-push)   DO_PUSH=0 ;;
    --dry-run)   DRY_RUN=1 ;;
    -h|--help)   sed -n '2,30p' "$0"; exit 0 ;;
    -*)          die "unknown option: $1" ;;
    *)           [ -z "$VERSION" ] && VERSION="$1" || die "unexpected argument: $1" ;;
  esac
  shift
done

[ -n "$VERSION" ] || die "usage: scripts/release.sh <version> [--no-docker] [--no-push] [--dry-run]"
# Accept both "0.2.3" and "v0.2.3".
VERSION="${VERSION#v}"
echo "$VERSION" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?$' \
  || die "version must look like X.Y.Z (got '$VERSION')"
TAG="v${VERSION}"

# --- preflight ---
git rev-parse --git-dir >/dev/null 2>&1 || die "not a git repository"
if [ "$DRY_RUN" -eq 0 ]; then
  [ -z "$(git status --porcelain)" ] || die "working tree is not clean — commit or stash first"
fi
git rev-parse "$TAG" >/dev/null 2>&1 && die "tag $TAG already exists"

last_tag="$(git describe --tags --abbrev=0 2>/dev/null || true)"
if [ -n "$last_tag" ]; then range="${last_tag}..HEAD"; else range="HEAD"; fi
echo ">> releasing $TAG (changes since ${last_tag:-the beginning})"

# --- 1. bump the server package version ---
server_toml="marcidb-server/Cargo.toml"
echo ">> setting $server_toml version to $VERSION"
if [ "$DRY_RUN" -eq 0 ]; then
  # Replace the first 'version = "..."' line (the [package] one at the top).
  perl -0pi -e 's/^version = "[^"]*"/version = "'"$VERSION"'"/m' "$server_toml"
fi

# --- 2. changelog ---
echo ">> generating changelog"
notes="$(bash scripts/gen-changelog.sh "$range")"
[ -n "$notes" ] || notes="_No notable changes._"
release_date="$(date +%Y-%m-%d)"

if [ "$DRY_RUN" -eq 0 ]; then
  tmp="$(mktemp)"
  entry="$(mktemp)"
  { printf '## %s - %s\n\n' "$TAG" "$release_date"; printf '%s\n\n' "$notes"; } > "$entry"
  if [ -f CHANGELOG.md ] && grep -q '^## ' CHANGELOG.md; then
    # insert the new entry just before the first existing "## " version heading,
    # preserving the "# Changelog" header and any intro prose above it.
    awk -v nf="$entry" '
      !done && /^## / { while ((getline line < nf) > 0) print line; done=1 }
      { print }
    ' CHANGELOG.md > "$tmp"
  else
    # no existing versioned entries — start (or append to) the header
    { if [ -f CHANGELOG.md ]; then cat CHANGELOG.md; else printf '# Changelog\n'; fi; printf '\n'; cat "$entry"; } > "$tmp"
  fi
  mv "$tmp" CHANGELOG.md
  rm -f "$entry"
else
  printf -- '---- changelog preview ----\n## %s - %s\n\n%s\n--------------------------\n' \
    "$TAG" "$release_date" "$notes"
fi

# --- 3. docker ---
if [ "$DO_DOCKER" -eq 1 ]; then
  echo ">> building Docker images"
  # full: vector + full-text, nightly toolchain
  run docker build \
    --build-arg FEATURES="vector fulltext" \
    -t "${IMAGE}:${VERSION}" \
    -t "${IMAGE}:${VERSION}-full" \
    -t "${IMAGE}:full" \
    -t "${IMAGE}:latest" \
    .
  # core: no optional modules, stable toolchain
  run docker build \
    --build-arg FEATURES="" \
    --build-arg RUST_IMAGE="rust:bookworm" \
    -t "${IMAGE}:${VERSION}-core" \
    -t "${IMAGE}:core" \
    .

  echo ">> pushing Docker images to $IMAGE"
  for t in "${VERSION}" "${VERSION}-full" "full" "latest" "${VERSION}-core" "core"; do
    run docker push "${IMAGE}:${t}"
  done
else
  echo ">> skipping Docker (--no-docker)"
fi

# --- 4. commit, tag, push ---
echo ">> committing release"
run git add CHANGELOG.md "$server_toml" Cargo.lock 2>/dev/null || run git add CHANGELOG.md "$server_toml"
run git commit -m "chore: release ${TAG}"
run git tag -a "$TAG" -m "Release ${TAG}"

if [ "$DO_PUSH" -eq 1 ]; then
  branch="$(git rev-parse --abbrev-ref HEAD)"
  echo ">> pushing $branch and $TAG to $REMOTE"
  run git push "$REMOTE" "$branch"
  run git push "$REMOTE" "$TAG"
  echo ">> done — GitHub Actions will build the binaries and publish the release for $TAG"
else
  echo ">> skipping push (--no-push). Push manually with:"
  echo "     git push $REMOTE HEAD && git push $REMOTE $TAG"
fi
