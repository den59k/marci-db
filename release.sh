#!/usr/bin/env bash
# Prepares a release: stamps ONE version across the Cargo workspace + both npm packages (lockstep — "same
# version == compatible"), commits, tags, and — with --push — pushes. The push triggers the Release workflow
# (.github/workflows/release.yml), which builds every platform and publishes the npm packages + GitHub
# Release. All building/testing/publishing happens in CI; this script only stamps and tags.
#
#   ./release.sh <version> [--push] [--no-verify]
#
#   --push        also push the commit + tag (triggers the release CI). Without it, you push manually.
#   --no-verify   skip the local `cargo check` pre-flight.
set -euo pipefail

cd "$(dirname "$0")"

VERSION="${1:-}"
PUSH=0
VERIFY=1
shift || true
while [ $# -gt 0 ]; do
  case "$1" in
    --push)      PUSH=1; shift ;;
    --no-verify) VERIFY=0; shift ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

if ! echo "$VERSION" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?$'; then
  echo "Usage: ./release.sh <version> [--push] [--no-verify]   (version must be semver, e.g. 0.6.0)" >&2
  exit 1
fi

# Refuse on a dirty tree so the release commit contains only the version stamp.
if [ -n "$(git status --porcelain)" ]; then
  echo "Working tree is not clean — commit or stash your changes first." >&2
  exit 1
fi

echo "▶ Stamping version $VERSION"
# Cargo workspace: the only quoted `version = "…"` in the root manifest (members use version.workspace=true).
perl -i -pe 's/^version = ".*"/version = "'"$VERSION"'"/' Cargo.toml
# Both npm packages (rewrite via node to keep valid JSON); embedded also pins its client peerDep in lockstep.
node -e '
  const fs = require("fs");
  const v = process.argv[1];
  for (const f of ["packages/marcidb-client/package.json", "packages/marcidb-embedded/package.json"]) {
    const p = JSON.parse(fs.readFileSync(f, "utf8"));
    p.version = v;
    if (p.peerDependencies && p.peerDependencies["marcidb-client"]) p.peerDependencies["marcidb-client"] = "^" + v;
    fs.writeFileSync(f, JSON.stringify(p, null, 2) + "\n");
  }
' "$VERSION"

if [ "$VERIFY" = 1 ]; then
  echo "▶ Pre-flight: cargo check --workspace"
  cargo check --workspace
fi

echo "▶ Committing + tagging v$VERSION"
git add Cargo.toml packages/marcidb-client/package.json packages/marcidb-embedded/package.json
git commit -m "release: v$VERSION"
git tag "v$VERSION"

if [ "$PUSH" = 1 ]; then
  echo "▶ Pushing (triggers the release CI → npm publish + GitHub Release)"
  git push origin HEAD
  git push origin "v$VERSION"
  echo "✔ Pushed v$VERSION. Watch the Release workflow on GitHub."
else
  echo "✔ Committed + tagged v$VERSION locally. Push to release:"
  echo "    git push origin HEAD && git push origin v$VERSION"
fi
