# Streamline Development Container
# Use this if you need custom tooling beyond the base image

FROM mcr.microsoft.com/devcontainers/rust:1-1-bullseye

# Install additional system dependencies
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    libprotobuf-dev \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Rust development tools
RUN cargo install cargo-watch cargo-audit cargo-deny cargo-nextest cargo-machete

# Install pre-commit
RUN pip3 install pre-commit

# Set up shell customizations
RUN echo 'alias ll="ls -la"' >> /home/vscode/.zshrc \
    && echo 'alias c="cargo check --all-features"' >> /home/vscode/.zshrc \
    && echo 'alias t="cargo test"' >> /home/vscode/.zshrc \
    && echo 'alias run="cargo run --"' >> /home/vscode/.zshrc

USER vscode
