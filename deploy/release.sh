#!/bin/bash
set -e

# Build and publish a release to GitHub
# Usage: ./deploy/release.sh [version]
# Example: ./deploy/release.sh 0.1.0

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

# Get version from argument or Cargo.toml
VERSION="${1:-$(grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)"/\1/')}"
TAG="v${VERSION}"

echo "Building release $TAG"

# Ensure working tree is clean
if [ -n "$(git status --porcelain)" ]; then
    echo "Error: working tree is not clean. Commit or stash changes first."
    exit 1
fi

# Build release binary
echo "Building release binary..."
cargo build --release

# Build .deb package
echo "Building .deb package..."
cargo deb --no-build

# Find the built .deb
DEB_FILE=$(ls -1 target/debian/rtpigate_*.deb 2>/dev/null | head -1)
if [ -z "$DEB_FILE" ]; then
    echo "Error: .deb file not found"
    exit 1
fi

echo "Built: $DEB_FILE"

# Tag the release
echo "Tagging $TAG..."
git tag -a "$TAG" -m "Release $TAG"
git push origin "$TAG"

# Create GitHub release with the .deb attached
echo "Creating GitHub release..."
gh release create "$TAG" \
    "$DEB_FILE" \
    --title "rtpigate $TAG" \
    --notes "$(cat <<EOF
## rtpigate $TAG

### Installation

Download the .deb package and install:
\`\`\`bash
sudo dpkg -i $(basename "$DEB_FILE")
\`\`\`

Then configure and start:
\`\`\`bash
sudo nano /etc/rtpigate/config.toml
sudo systemctl enable --now rtpigate
\`\`\`

### View logs
\`\`\`bash
journalctl -u rtpigate -f
\`\`\`

### Reload config without restart
\`\`\`bash
sudo systemctl reload rtpigate
\`\`\`
EOF
)"

echo ""
echo "Release $TAG published!"
echo "URL: https://github.com/Deatojef/rtpigate/releases/tag/$TAG"
