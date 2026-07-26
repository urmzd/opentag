#!/bin/sh
# install.sh — Installs opentag from GitHub releases.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/urmzd/opentag/main/install.sh | sh
#
# Environment variables:
#   OPENTAG_VERSION     — version to install (default: latest)
#   OPENTAG_INSTALL_DIR — installation directory (default: $HOME/.local/bin)
#   OPENTAG_SHA256      — optional SHA256 checksum to verify against

set -eu

REPO="urmzd/opentag"
BINARY="opentag"
VERSION="${OPENTAG_VERSION:-latest}"
INSTALL_DIR="${OPENTAG_INSTALL_DIR:-$HOME/.local/bin}"

os=$(uname -s | tr '[:upper:]' '[:lower:]')
arch=$(uname -m)
case "$os" in
  linux | darwin) ;;
  *)
    echo "error: unsupported OS: $os (download manually from https://github.com/$REPO/releases)" >&2
    exit 1
    ;;
esac
case "$arch" in
  x86_64 | amd64) arch="amd64" ;;
  aarch64 | arm64) arch="arm64" ;;
  *)
    echo "error: unsupported architecture: $arch" >&2
    exit 1
    ;;
esac

asset="${BINARY}-${os}-${arch}"
if [ "$VERSION" = "latest" ]; then
  url="https://github.com/$REPO/releases/latest/download/$asset"
else
  case "$VERSION" in
    v*) ;;
    *) VERSION="v$VERSION" ;;
  esac
  url="https://github.com/$REPO/releases/download/$VERSION/$asset"
fi

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

echo "downloading $url" >&2
curl -fsSL "$url" -o "$tmp/$BINARY"

if [ -n "${OPENTAG_SHA256:-}" ]; then
  echo "${OPENTAG_SHA256}  $tmp/$BINARY" | (sha256sum -c - 2>/dev/null || shasum -a 256 -c -) >&2
fi

chmod +x "$tmp/$BINARY"
mkdir -p "$INSTALL_DIR"
mv "$tmp/$BINARY" "$INSTALL_DIR/$BINARY"

echo "installed $BINARY to $INSTALL_DIR/$BINARY" >&2
case ":$PATH:" in
  *":$INSTALL_DIR:"*) ;;
  *) echo "note: $INSTALL_DIR is not on your PATH" >&2 ;;
esac
