#!/usr/bin/env bash
# Install libjemalloc development package for the current platform.
set -euo pipefail

check_already_installed() {
    if pkg-config --libs jemalloc >/dev/null 2>&1; then
        echo "jemalloc is already installed ($(pkg-config --modversion jemalloc))."
        exit 0
    fi
}

install_debian() {
    echo "Detected Debian/Ubuntu — installing libjemalloc-dev..."
    sudo apt-get update -qq
    sudo apt-get install -y libjemalloc-dev
}

install_rhel() {
    echo "Detected RHEL/CentOS/Fedora — installing jemalloc-devel..."
    if command -v dnf >/dev/null 2>&1; then
        sudo dnf install -y jemalloc-devel
    else
        sudo yum install -y jemalloc-devel
    fi
}

install_arch() {
    echo "Detected Arch Linux — installing jemalloc..."
    sudo pacman -Sy --noconfirm jemalloc
}

install_macos() {
    echo "Detected macOS — installing jemalloc via Homebrew..."
    if ! command -v brew >/dev/null 2>&1; then
        echo "Homebrew not found. Install it from https://brew.sh and re-run this script." >&2
        exit 1
    fi
    brew install jemalloc
}

check_already_installed

case "$(uname -s)" in
    Linux)
        if [ -f /etc/debian_version ]; then
            install_debian
        elif [ -f /etc/redhat-release ]; then
            install_rhel
        elif [ -f /etc/arch-release ]; then
            install_arch
        else
            echo "Unsupported Linux distribution. Install libjemalloc-dev manually." >&2
            exit 1
        fi
        ;;
    Darwin)
        install_macos
        ;;
    *)
        echo "Unsupported OS: $(uname -s)" >&2
        exit 1
        ;;
esac

echo ""
echo "Verifying installation..."
if pkg-config --libs jemalloc >/dev/null 2>&1; then
    echo "jemalloc $(pkg-config --modversion jemalloc) installed successfully."
else
    echo "Installation may have succeeded but pkg-config cannot find jemalloc." >&2
    echo "You may need to set PKG_CONFIG_PATH manually." >&2
    exit 1
fi
