#!/usr/bin/env bash
#
# Cut a new rtpigate release.
#
#   ./deploy/release.sh X.Y.Z
#
# Bumps the crate version, runs the pre-flight gate, commits, and pushes an
# annotated tag `vX.Y.Z`. The push triggers .github/workflows/release.yml, which
# cross-compiles for arm64 and amd64, builds a .deb for each, and attaches them
# (plus SHA256SUMS) to a GitHub Release.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

MANIFEST="Cargo.toml"

die() { echo "error: $*" >&2; exit 1; }
note() { echo ">> $*"; }

VERSION="${1:-}"
[ -n "$VERSION" ] || die "usage: $0 X.Y.Z"
echo "$VERSION" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$' || die "version must be X.Y.Z (got '$VERSION')"

TAG="v${VERSION}"

# --- Pre-flight ---
[ "$(git rev-parse --abbrev-ref HEAD)" = "main" ] || die "must be on 'main'"
[ -z "$(git status --porcelain)" ] || die "working tree is dirty; commit or stash first"
git rev-parse "$TAG" >/dev/null 2>&1 && die "tag $TAG already exists"

note "pre-flight checks (fmt, clippy, test)"
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test

# --- Bump version (first 'version = "..."' line, i.e. the [package] one) ---
note "bumping $MANIFEST to $VERSION"
sed -i "0,/^version = \".*\"/s//version = \"$VERSION\"/" "$MANIFEST"
grep -q "^version = \"$VERSION\"" "$MANIFEST" || die "version bump failed"
# Refresh Cargo.lock to reflect the new crate version.
cargo update -p rtpigate

# --- Commit, tag, push ---
note "committing and tagging $TAG"
git add "$MANIFEST" Cargo.lock
git commit -m "Release rtpigate $TAG"
git tag -a "$TAG" -m "rtpigate $TAG"

note "pushing main and $TAG"
git push origin main
git push origin "$TAG"

echo
note "pushed $TAG — GitHub Actions will build and publish the release."
if command -v gh >/dev/null 2>&1; then
    echo "   Watch:  gh run watch --exit-status"
    echo "   Release: gh release view $TAG --web"
fi
