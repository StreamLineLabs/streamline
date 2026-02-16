FROM gitpod/workspace-rust

USER gitpod
RUN rustup default stable && \
    rustup component add clippy rustfmt
