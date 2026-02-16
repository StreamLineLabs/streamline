#!/usr/bin/env bash
#
# Streamline Developer Setup Script
#
# This script sets up a complete development environment for Streamline.
# Run this after cloning the repository.
#
# Usage: ./scripts/dev-setup.sh [--skip-build] [--skip-hooks] [--full] [--minimal]
#

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Parse arguments
SKIP_BUILD=false
SKIP_HOOKS=false
FULL_SETUP=false

for arg in "$@"; do
    case $arg in
        --skip-build)
            SKIP_BUILD=true
            shift
            ;;
        --skip-hooks)
            SKIP_HOOKS=true
            shift
            ;;
        --full)
            FULL_SETUP=true
            shift
            ;;
        --minimal)
            FULL_SETUP=false
            shift
            ;;
        --help|-h)
            echo "Usage: ./scripts/dev-setup.sh [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --skip-build    Skip building the project"
            echo "  --skip-hooks    Skip setting up git hooks"
            echo "  --full          Install optional development tools"
            echo "  --minimal       Skip optional development tools (default)"
            echo "  --help, -h      Show this help message"
            exit 0
            ;;
    esac
done

echo -e "${BLUE}"
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║              Streamline Developer Setup                      ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo -e "${NC}"

# Check if we're in the right directory
if [[ ! -f "Cargo.toml" ]] || ! grep -q "streamline" Cargo.toml 2>/dev/null; then
    echo -e "${RED}Error: Please run this script from the streamline repository root${NC}"
    exit 1
fi

# Step 1: Check Rust installation
echo -e "${BLUE}[1/7] Checking Rust installation...${NC}"
if command -v rustc &> /dev/null; then
    RUST_VERSION=$(rustc --version)
    echo -e "${GREEN}✓ Rust is installed: $RUST_VERSION${NC}"
else
    echo -e "${YELLOW}Rust not found. Installing via rustup...${NC}"
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source "$HOME/.cargo/env"
    echo -e "${GREEN}✓ Rust installed successfully${NC}"
fi

# Step 2: Install required Rust version from toolchain file
echo -e "${BLUE}[2/7] Setting up Rust toolchain...${NC}"
if [[ -f "rust-toolchain.toml" ]]; then
    rustup show active-toolchain
    echo -e "${GREEN}✓ Toolchain configured from rust-toolchain.toml${NC}"
else
    rustup default stable
    rustup component add rustfmt clippy rust-src rust-analyzer
    echo -e "${GREEN}✓ Toolchain configured${NC}"
fi

# Step 3: Install development tools
if [[ "$FULL_SETUP" == "true" ]]; then
    echo -e "${BLUE}[3/7] Installing development tools...${NC}"

    TOOLS=(
        "cargo-watch:watch"
        "cargo-audit:audit"
        "cargo-deny:deny"
        "cargo-machete:machete"
        "cargo-nextest:nextest"
    )

    for tool_spec in "${TOOLS[@]}"; do
        IFS=':' read -r crate cmd <<< "$tool_spec"
        if cargo "$cmd" --version &> /dev/null 2>&1; then
            echo -e "  ${GREEN}✓${NC} $crate already installed"
        else
            echo -e "  ${YELLOW}Installing $crate...${NC}"
            cargo install "$crate" --quiet || echo -e "  ${YELLOW}⚠ Failed to install $crate (optional)${NC}"
        fi
    done
else
    echo -e "${BLUE}[3/7] Skipping optional tools (use --full to install)${NC}"
fi

# Step 4: Set up pre-commit hooks
if [[ "$SKIP_HOOKS" == "false" ]]; then
    echo -e "${BLUE}[4/7] Setting up git hooks...${NC}"
    if command -v pre-commit &> /dev/null; then
        pre-commit install
        echo -e "${GREEN}✓ Pre-commit hooks installed${NC}"
    elif [[ -f ".pre-commit-config.yaml" ]]; then
        echo -e "${YELLOW}pre-commit not found. Install with: pip install pre-commit${NC}"
        echo -e "${YELLOW}Then run: pre-commit install${NC}"
    fi
else
    echo -e "${BLUE}[4/7] Skipping git hooks setup (--skip-hooks)${NC}"
fi

# Step 5: Build the project
if [[ "$SKIP_BUILD" == "false" ]]; then
    echo -e "${BLUE}[5/7] Building the project (lite edition)...${NC}"
    cargo build
    echo -e "${GREEN}✓ Build successful${NC}"

    echo -e "${BLUE}[6/7] Running tests...${NC}"
    cargo test --lib -- --test-threads=4
    echo -e "${GREEN}✓ Tests passed${NC}"
else
    echo -e "${BLUE}[5/7] Skipping build (--skip-build)${NC}"
    echo -e "${BLUE}[6/7] Skipping tests (--skip-build)${NC}"
fi

# Step 7: Create data directory
echo -e "${BLUE}[7/7] Setting up local directories...${NC}"
mkdir -p data
mkdir -p ~/.streamline
echo -e "${GREEN}✓ Directories created${NC}"

# Print summary
echo ""
echo -e "${GREEN}"
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║                    Setup Complete!                           ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo -e "${NC}"

echo -e "${BLUE}Quick Start Commands:${NC}"
echo ""
echo "  Start server (playground mode):"
echo -e "    ${YELLOW}cargo run -- --playground${NC}"
echo ""
echo "  Start server (production mode):"
echo -e "    ${YELLOW}cargo run${NC}"
echo ""
echo "  Run CLI:"
echo -e "    ${YELLOW}cargo run --bin streamline-cli -- topics list${NC}"
echo ""
echo "  Run TUI dashboard:"
echo -e "    ${YELLOW}cargo run --bin streamline-cli -- top${NC}"
echo ""
echo "  Watch mode (auto-rebuild on changes):"
echo -e "    ${YELLOW}cargo watch -x 'check --all-features'${NC}"
echo ""
echo "  Run full validation:"
echo -e "    ${YELLOW}make validate${NC}"
echo ""
echo -e "${BLUE}Useful Cargo Aliases:${NC}"
echo "  cargo c       - Quick check with all features"
echo "  cargo lint    - Run clippy with strict warnings"
echo "  cargo t       - Run all tests"
echo "  cargo play    - Run server in playground mode"
echo "  cargo top     - Run TUI dashboard"
echo "  cargo docs    - Generate and open documentation"
echo ""
echo -e "${BLUE}Documentation:${NC}"
echo "  README.md           - Project overview"
echo "  CONTRIBUTING.md     - Contribution guidelines"
echo "  CLAUDE.md           - AI assistant context"
echo "  docs/               - Detailed documentation"
echo ""
echo -e "${GREEN}Happy coding! 🚀${NC}"
