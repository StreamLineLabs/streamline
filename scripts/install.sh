#!/usr/bin/env bash
#
# Streamline install script
# Downloads the correct release binary and installs it locally.
#
# Usage: ./scripts/install.sh [--version <version>] [--dir <install-dir>]
#

set -euo pipefail

REPO="josedab/streamline"
VERSION="${STREAMLINE_VERSION:-latest}"
INSTALL_DIR="${INSTALL_DIR:-$HOME/.local/bin}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --version|-v)
            VERSION="$2"
            shift 2
            ;;
        --dir|-d)
            INSTALL_DIR="$2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: ./scripts/install.sh [--version <version>] [--dir <install-dir>]"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

if [[ "$VERSION" == "latest" ]]; then
    VERSION=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
        | grep -E '"tag_name":' \
        | head -1 \
        | sed -E 's/.*"v?([^"]+)".*/\1/')
fi

if [[ -z "$VERSION" ]]; then
    echo "Unable to determine latest version."
    exit 1
fi

TAG="v${VERSION}"

OS="$(uname -s)"
ARCH="$(uname -m)"

case "$OS" in
    Darwin)
        OS_NAME="apple-darwin"
        ;;
    Linux)
        OS_NAME="unknown-linux-gnu"
        ;;
    *)
        echo "Unsupported OS: $OS"
        exit 1
        ;;
esac

case "$ARCH" in
    x86_64|amd64)
        ARCH_NAME="x86_64"
        ;;
    arm64|aarch64)
        ARCH_NAME="aarch64"
        ;;
    *)
        echo "Unsupported architecture: $ARCH"
        exit 1
        ;;
esac

ASSET="streamline-${VERSION}-${ARCH_NAME}-${OS_NAME}.tar.gz"
URL="https://github.com/${REPO}/releases/download/${TAG}/${ASSET}"

TMP_DIR="$(mktemp -d)"
cleanup() {
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

echo "Downloading ${URL}"
curl -fsSL "$URL" -o "${TMP_DIR}/${ASSET}"

if curl -fsSL "${URL}.sha256" -o "${TMP_DIR}/${ASSET}.sha256"; then
    if command -v sha256sum >/dev/null 2>&1; then
        EXPECTED=$(cut -d ' ' -f 1 "${TMP_DIR}/${ASSET}.sha256")
        ACTUAL=$(sha256sum "${TMP_DIR}/${ASSET}" | cut -d ' ' -f 1)
    elif command -v shasum >/dev/null 2>&1; then
        EXPECTED=$(cut -d ' ' -f 1 "${TMP_DIR}/${ASSET}.sha256")
        ACTUAL=$(shasum -a 256 "${TMP_DIR}/${ASSET}" | cut -d ' ' -f 1)
    fi

    if [[ -n "${EXPECTED:-}" && -n "${ACTUAL:-}" && "$EXPECTED" != "$ACTUAL" ]]; then
        echo "Checksum verification failed."
        exit 1
    fi
fi

tar -xzf "${TMP_DIR}/${ASSET}" -C "$TMP_DIR"

mkdir -p "$INSTALL_DIR"
install -m 0755 "${TMP_DIR}/streamline" "${INSTALL_DIR}/streamline"
install -m 0755 "${TMP_DIR}/streamline-cli" "${INSTALL_DIR}/streamline-cli"

echo "Installed Streamline to ${INSTALL_DIR}"
echo "Add ${INSTALL_DIR} to your PATH if needed."
