#!/usr/bin/env python3
"""Generate a cross-SDK API compatibility matrix.

Scans each SDK repository to identify supported features and produces a
markdown comparison table suitable for inclusion in documentation.

Usage:
    python3 scripts/generate-sdk-matrix.py [--output FILE]
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path


# SDK definitions: (directory, display_name, language, config_file)
SDKS = [
    ("streamline-java-sdk", "Java", "Java 17+", "pom.xml"),
    ("streamline-python-sdk", "Python", "Python 3.9+", "pyproject.toml"),
    ("streamline-go-sdk", "Go", "Go 1.22+", "go.mod"),
    ("streamline-node-sdk", "Node.js", "TypeScript", "package.json"),
    ("streamline-rust-sdk", "Rust", "Rust 1.80+", "Cargo.toml"),
    ("streamline-dotnet-sdk", ".NET", "C# .NET 8+", "Streamline.sln"),
    ("streamline-wasm-sdk", "WASM", "Rust/JS", "package.json"),
]

# Features to check for in each SDK
FEATURE_PATTERNS = {
    "Producer": [r"[Pp]roducer", r"produce", r"send.*message"],
    "Consumer": [r"[Cc]onsumer", r"consume", r"subscribe"],
    "Admin Client": [r"[Aa]dmin", r"create.*topic", r"delete.*topic"],
    "Consumer Groups": [r"group", r"consumer.*group", r"offset.*commit"],
    "TLS/SSL": [r"[Tt][Ll][Ss]", r"[Ss][Ss][Ll]", r"ssl_context", r"security\.protocol"],
    "SASL Auth": [r"[Ss][Aa][Ss][Ll]", r"sasl_mechanism", r"SaslConfig"],
    "Compression": [r"compress", r"lz4", r"zstd", r"snappy", r"gzip"],
    "Connection Pooling": [r"pool", r"[Cc]onnection.*[Pp]ool"],
    "Auto Reconnect": [r"reconnect", r"retry", r"backoff"],
    "OpenTelemetry": [r"opentelemetry", r"otel", r"tracing", r"telemetry"],
    "TestContainers": [r"testcontainer", r"test.*container"],
    "Embedded Mode": [r"embedded", r"in.process", r"EmbeddedStreamline"],
    "Async/Streaming": [r"async", r"stream", r"channel", r"IAsyncEnumerable"],
    "Benchmarks": [r"benchmark", r"bench"],
}


def find_org_root():
    """Find the StreamlineLabs org root directory."""
    script_dir = Path(__file__).resolve().parent
    # Script is in streamline/scripts/, org root is two levels up
    org_root = script_dir.parent.parent
    if not (org_root / "streamline").is_dir():
        # Try current directory
        org_root = Path.cwd()
        if not (org_root / "streamline").is_dir():
            print("Error: Cannot find StreamlineLabs org root", file=sys.stderr)
            sys.exit(1)
    return org_root


def get_version(sdk_dir, config_file):
    """Extract version from SDK config file."""
    config_path = sdk_dir / config_file
    if not config_path.exists():
        return "?"

    content = config_path.read_text()

    if config_file == "package.json":
        try:
            data = json.loads(content)
            return data.get("version", "?")
        except json.JSONDecodeError:
            return "?"
    elif config_file == "Cargo.toml":
        match = re.search(r'^version\s*=\s*"([^"]+)"', content, re.MULTILINE)
        return match.group(1) if match else "?"
    elif config_file == "pom.xml":
        match = re.search(r"<version>([^<]+)</version>", content)
        return match.group(1) if match else "?"
    elif config_file == "pyproject.toml":
        match = re.search(r'^version\s*=\s*"([^"]+)"', content, re.MULTILINE)
        return match.group(1) if match else "?"
    elif config_file == "go.mod":
        return "module"
    elif config_file.endswith(".sln"):
        return "?"

    return "?"


def check_feature(sdk_dir, patterns):
    """Check if an SDK supports a feature by scanning source files."""
    src_extensions = {".java", ".py", ".go", ".ts", ".js", ".rs", ".cs"}

    for root, _dirs, files in os.walk(sdk_dir):
        # Skip test directories for primary feature detection (but include for TestContainers)
        rel = os.path.relpath(root, sdk_dir)
        if any(skip in rel for skip in ["node_modules", "target", "dist", "bin", ".git"]):
            continue

        for f in files:
            ext = os.path.splitext(f)[1]
            if ext not in src_extensions:
                continue

            try:
                content = open(os.path.join(root, f), encoding="utf-8", errors="ignore").read()
            except OSError:
                continue

            for pattern in patterns:
                if re.search(pattern, content):
                    return True
    return False


def count_test_files(sdk_dir):
    """Count test files in an SDK."""
    count = 0
    test_patterns = [r"_test\.", r"test_", r"Test\.", r"\.test\.", r"\.spec\."]
    for root, _dirs, files in os.walk(sdk_dir):
        if ".git" in root or "node_modules" in root or "target" in root:
            continue
        for f in files:
            for pat in test_patterns:
                if re.search(pat, f):
                    count += 1
                    break
    return count


def has_ci(sdk_dir):
    """Check if SDK has CI configuration."""
    return (sdk_dir / ".github" / "workflows" / "ci.yml").exists()


def has_codeql(sdk_dir):
    """Check if SDK has CodeQL security scanning."""
    return (sdk_dir / ".github" / "workflows" / "codeql.yml").exists()


def generate_matrix(org_root):
    """Generate the full compatibility matrix."""
    results = []

    for sdk_dirname, display_name, language, config_file in SDKS:
        sdk_dir = org_root / sdk_dirname
        if not sdk_dir.is_dir():
            continue

        version = get_version(sdk_dir, config_file)
        features = {}
        for feature_name, patterns in FEATURE_PATTERNS.items():
            features[feature_name] = check_feature(sdk_dir, patterns)

        results.append({
            "name": display_name,
            "dir": sdk_dirname,
            "language": language,
            "version": version,
            "features": features,
            "test_count": count_test_files(sdk_dir),
            "has_ci": has_ci(sdk_dir),
            "has_codeql": has_codeql(sdk_dir),
        })

    return results


def render_markdown(results):
    """Render results as a markdown table."""
    lines = []
    lines.append("# Cross-SDK API Compatibility Matrix")
    lines.append("")
    lines.append(f"*Auto-generated by `scripts/generate-sdk-matrix.py`*")
    lines.append("")

    # Overview table
    lines.append("## SDK Overview")
    lines.append("")
    lines.append("| SDK | Language | Version | Tests | CI | Security |")
    lines.append("|-----|----------|---------|-------|----|----------|")
    for r in results:
        ci = "✅" if r["has_ci"] else "❌"
        codeql = "✅" if r["has_codeql"] else "❌"
        lines.append(f"| **{r['name']}** | {r['language']} | {r['version']} | {r['test_count']} files | {ci} | {codeql} |")
    lines.append("")

    # Feature matrix
    lines.append("## Feature Support")
    lines.append("")
    header = "| Feature | " + " | ".join(r["name"] for r in results) + " |"
    separator = "|---------|" + "|".join("---" for _ in results) + "|"
    lines.append(header)
    lines.append(separator)

    for feature_name in FEATURE_PATTERNS:
        row = f"| **{feature_name}** |"
        for r in results:
            supported = r["features"].get(feature_name, False)
            row += " ✅ |" if supported else " ❌ |"
        lines.append(row)

    lines.append("")
    lines.append("## Legend")
    lines.append("")
    lines.append("- ✅ Feature detected in source code")
    lines.append("- ❌ Feature not detected (may be planned or use different naming)")
    lines.append("")
    lines.append("*Detection is based on pattern matching in source files. Some features may be")
    lines.append("present but use naming conventions not covered by the scanner.*")

    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description="Generate cross-SDK compatibility matrix")
    parser.add_argument("--output", "-o", help="Output file path (default: stdout)")
    parser.add_argument("--json", action="store_true", help="Output as JSON instead of markdown")
    args = parser.parse_args()

    org_root = find_org_root()
    results = generate_matrix(org_root)

    if args.json:
        output = json.dumps(results, indent=2) + "\n"
    else:
        output = render_markdown(results)

    if args.output:
        Path(args.output).write_text(output)
        print(f"Matrix written to {args.output}", file=sys.stderr)
    else:
        print(output, end="")


if __name__ == "__main__":
    main()
