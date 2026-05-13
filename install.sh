#!/usr/bin/env sh
# ZenithDB installer for macOS and Linux.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/Polarityinc/zenith/main/install.sh | sh
#
# Environment overrides:
#   VERSION       release tag to install            (default: latest)
#   INSTALL_DIR   binary install directory          (default: $HOME/.local/bin)
#   REPO          source repo owner/name            (default: Polarityinc/zenith)

set -eu

REPO="${REPO:-Polarityinc/zenith}"
INSTALL_DIR="${INSTALL_DIR:-$HOME/.local/bin}"
VERSION="${VERSION:-latest}"

uname_s="$(uname -s)"
uname_m="$(uname -m)"

case "$uname_s" in
  Darwin) OS=darwin ;;
  Linux)  OS=linux ;;
  *) echo "error: unsupported OS '$uname_s'. Supported: macOS, Linux." >&2; exit 1 ;;
esac

case "$uname_m" in
  arm64|aarch64) ARCH=arm64 ;;
  x86_64|amd64)  ARCH=amd64 ;;
  *) echo "error: unsupported arch '$uname_m'. Supported: arm64, amd64." >&2; exit 1 ;;
esac

for tool in curl tar uname; do
  command -v "$tool" >/dev/null 2>&1 || { echo "error: '$tool' is required" >&2; exit 1; }
done

if [ "$VERSION" = "latest" ]; then
  VERSION="$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" 2>/dev/null \
    | grep -E '"tag_name"' | head -n1 | sed -E 's/.*"tag_name": *"([^"]+)".*/\1/')" || true
  if [ -z "${VERSION:-}" ]; then
    cat >&2 <<EOF
error: no published releases found at github.com/${REPO}/releases.

ZenithDB is alpha; published binaries may not exist yet. Build from source:

  git clone https://github.com/${REPO}.git
  cd zenith
  cargo build --release -p zen_cli
  ./target/release/zen serve --config examples/zenithdb.dev.toml
EOF
    exit 1
  fi
fi

TARBALL="zen-${VERSION}-${OS}-${ARCH}.tar.gz"
URL="https://github.com/${REPO}/releases/download/${VERSION}/${TARBALL}"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo "downloading ${TARBALL} ..."
if ! curl -fsSL "$URL" -o "$TMP/$TARBALL"; then
  echo "error: failed to download $URL" >&2
  echo "build from source instead: cargo install --git https://github.com/${REPO}.git zen_cli" >&2
  exit 1
fi

tar -xzf "$TMP/$TARBALL" -C "$TMP"
if [ ! -f "$TMP/zen" ]; then
  echo "error: tarball missing 'zen' binary" >&2
  exit 1
fi

mkdir -p "$INSTALL_DIR"
mv "$TMP/zen" "$INSTALL_DIR/zen"
chmod +x "$INSTALL_DIR/zen"

echo
echo "installed: $INSTALL_DIR/zen ($VERSION)"
case ":$PATH:" in
  *":$INSTALL_DIR:"*) ;;
  *) echo "note: add $INSTALL_DIR to your PATH (e.g. 'export PATH=\"$INSTALL_DIR:\$PATH\"')" ;;
esac
echo
echo "start a local server:"
echo "  zen serve --config examples/zenithdb.dev.toml"
