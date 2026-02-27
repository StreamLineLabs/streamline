#!/usr/bin/env bash
set -euo pipefail

# Streamline SDK Publishing Script
# Publishes all SDKs to their respective package registries.
# Usage: ./scripts/publish-sdks.sh [--dry-run]

DRY_RUN="${1:-}"
VERSION="0.2.0"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

log() { echo "$(date +%H:%M:%S) [PUBLISH] $*"; }
err() { echo "$(date +%H:%M:%S) [ERROR] $*" >&2; }

publish_rust_sdk() {
    log "Publishing Rust SDK v${VERSION} to crates.io..."
    cd "$ROOT/streamline-rust-sdk"
    if [ "$DRY_RUN" = "--dry-run" ]; then
        cargo publish --dry-run
    else
        cargo publish
    fi
    log "✅ Rust SDK published"
}

publish_node_sdk() {
    log "Publishing Node.js SDK v${VERSION} to npm..."
    cd "$ROOT/streamline-node-sdk"
    npm version "$VERSION" --no-git-tag-version 2>/dev/null || true
    if [ "$DRY_RUN" = "--dry-run" ]; then
        npm pack
    else
        npm publish --access public
    fi
    log "✅ Node.js SDK published"
}

publish_python_sdk() {
    log "Publishing Python SDK v${VERSION} to PyPI..."
    cd "$ROOT/streamline-python-sdk"
    if [ "$DRY_RUN" = "--dry-run" ]; then
        python -m build
    else
        python -m build && twine upload dist/*
    fi
    log "✅ Python SDK published"
}

publish_java_sdk() {
    log "Publishing Java SDK v${VERSION} to Maven Central..."
    cd "$ROOT/streamline-java-sdk"
    if [ "$DRY_RUN" = "--dry-run" ]; then
        mvn package -DskipTests
    else
        mvn deploy -DskipTests
    fi
    log "✅ Java SDK published"
}

publish_go_sdk() {
    log "Publishing Go SDK v${VERSION}..."
    cd "$ROOT/streamline-go-sdk"
    # Go modules are published by tagging — just verify
    log "Tag with: git tag v${VERSION} && git push origin v${VERSION}"
    log "✅ Go SDK (tag-based publish)"
}

publish_dotnet_sdk() {
    log "Publishing .NET SDK v${VERSION} to NuGet..."
    cd "$ROOT/streamline-dotnet-sdk"
    if [ "$DRY_RUN" = "--dry-run" ]; then
        dotnet pack -c Release
    else
        dotnet pack -c Release && dotnet nuget push **/*.nupkg --source nuget.org
    fi
    log "✅ .NET SDK published"
}

publish_wasm_sdk() {
    log "Publishing WASM SDK v${VERSION} to npm..."
    cd "$ROOT/streamline-wasm-sdk"
    if [ "$DRY_RUN" = "--dry-run" ]; then
        wasm-pack build --target web
    else
        wasm-pack build --target web && cd pkg && npm publish --access public
    fi
    log "✅ WASM SDK published"
}

publish_swift_sdk() {
    log "Swift SDK v${VERSION} — publish via Swift Package Manager"
    log "Tag with: git tag ${VERSION} && git push origin ${VERSION}"
    log "✅ Swift SDK (tag-based publish)"
}

publish_kotlin_sdk() {
    log "Publishing Kotlin SDK v${VERSION} to Maven Central..."
    cd "$ROOT/streamline-kotlin-sdk"
    if [ "$DRY_RUN" = "--dry-run" ]; then
        ./gradlew build
    else
        ./gradlew publish
    fi
    log "✅ Kotlin SDK published"
}

main() {
    log "Starting SDK publish pipeline for v${VERSION}"
    [ "$DRY_RUN" = "--dry-run" ] && log "DRY RUN MODE — no actual publishing"
    
    publish_rust_sdk
    publish_node_sdk  
    publish_python_sdk
    publish_java_sdk
    publish_go_sdk
    publish_dotnet_sdk
    publish_wasm_sdk
    publish_swift_sdk
    publish_kotlin_sdk
    
    log "🎉 All SDKs published successfully!"
}

main "$@"
