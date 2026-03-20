#!/bin/bash
set -e

# rtpigate installation script
# Run as root or with sudo

INSTALL_BIN="/usr/local/bin/rtpigate"
INSTALL_CONFIG="/etc/rtpigate"
INSTALL_FRONTEND="/usr/local/share/rtpigate/frontend"
SERVICE_FILE="/etc/systemd/system/rtpigate.service"
SERVICE_USER="rtpigate"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "Installing rtpigate from $PROJECT_DIR"

# Create system user if it doesn't exist
if ! id "$SERVICE_USER" &>/dev/null; then
    echo "Creating system user: $SERVICE_USER"
    useradd --system --no-create-home --shell /usr/sbin/nologin "$SERVICE_USER"
else
    echo "System user $SERVICE_USER already exists"
fi

# Build release binary
echo "Building release binary..."
cd "$PROJECT_DIR"
cargo build --release

# Install binary
echo "Installing binary to $INSTALL_BIN"
install -m 755 "$PROJECT_DIR/target/release/rtpigate" "$INSTALL_BIN"

# Install config directory and default config
echo "Installing config to $INSTALL_CONFIG"
mkdir -p "$INSTALL_CONFIG"
if [ ! -f "$INSTALL_CONFIG/config.toml" ]; then
    install -m 640 -o root -g "$SERVICE_USER" "$PROJECT_DIR/config.toml" "$INSTALL_CONFIG/config.toml"
    echo "  Installed default config.toml (edit before starting)"
else
    echo "  Config already exists, not overwriting"
    # Ensure existing config is readable by service user
    chgrp "$SERVICE_USER" "$INSTALL_CONFIG/config.toml"
    chmod 640 "$INSTALL_CONFIG/config.toml"
fi
chown root:"$SERVICE_USER" "$INSTALL_CONFIG"

# Install frontend assets
echo "Installing frontend to $INSTALL_FRONTEND"
mkdir -p "$INSTALL_FRONTEND/assets"
cp "$PROJECT_DIR/frontend/index.html" "$INSTALL_FRONTEND/"
cp "$PROJECT_DIR/frontend/assets/style.css" "$INSTALL_FRONTEND/assets/"
cp "$PROJECT_DIR/frontend/assets/app.js" "$INSTALL_FRONTEND/assets/"
cp -r "$PROJECT_DIR/frontend/assets/aprssymbols" "$INSTALL_FRONTEND/assets/"

# Ensure config.toml points to the installed frontend path
if ! grep -q '\[http\]' "$INSTALL_CONFIG/config.toml" 2>/dev/null; then
    echo "" >> "$INSTALL_CONFIG/config.toml"
    echo "[http]" >> "$INSTALL_CONFIG/config.toml"
    echo "frontend = \"$INSTALL_FRONTEND\"" >> "$INSTALL_CONFIG/config.toml"
    echo "  Added [http] frontend path to config"
fi

# Install systemd service
echo "Installing systemd service"
install -m 644 "$PROJECT_DIR/deploy/rtpigate.service" "$SERVICE_FILE"
systemctl daemon-reload

echo ""
echo "Installation complete!"
echo ""
echo "Next steps:"
echo "  1. Edit config:    sudo nano $INSTALL_CONFIG/config.toml"
echo "  2. Enable service: sudo systemctl enable rtpigate"
echo "  3. Start service:  sudo systemctl start rtpigate"
echo "  4. View logs:      journalctl -u rtpigate -f"
echo "  5. Reload config:  sudo systemctl reload rtpigate"
