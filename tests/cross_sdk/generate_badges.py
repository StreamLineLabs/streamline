#!/usr/bin/env python3
"""
Generate SDK conformance certification badges and matrix.

Reads conformance results from CI artifacts and produces:
1. Per-SDK badge JSON (shields.io endpoint format)
2. Markdown compatibility matrix for docs site

Usage:
  python3 generate_badges.py --results-dir ./results --output-dir ./badges
"""

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path


def generate_badge(sdk_name: str, status: str, output_dir: Path) -> None:
    """Generate a shields.io endpoint badge JSON."""
    if status == "success":
        color = "brightgreen"
        message = "certified"
    elif status == "failure":
        color = "red"
        message = "failing"
    else:
        color = "yellow"
        message = "unknown"

    badge = {
        "schemaVersion": 1,
        "label": f"{sdk_name}",
        "message": message,
        "color": color,
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    badge_file = output_dir / f"{sdk_name.replace('/', '-')}.json"
    badge_file.write_text(json.dumps(badge, indent=2))
    print(f"  Generated badge: {badge_file}")


def generate_matrix(results: dict, output_path: Path) -> None:
    """Generate a Markdown compatibility matrix."""
    lines = [
        "# SDK Conformance Matrix",
        "",
        f"Last updated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        "| SDK | Status | Required Tests | Optional Tests |",
        "|-----|--------|---------------|---------------|",
    ]

    for sdk, info in sorted(results.items()):
        status = info.get("status", "unknown")
        icon = "✅" if status == "success" else "❌" if status == "failure" else "⚠️"
        required = info.get("required_passed", "?")
        optional = info.get("optional_passed", "?")
        lines.append(f"| {sdk} | {icon} {status} | {required}/46 | {optional}/4 |")

    lines.extend([
        "",
        "## Spec Version",
        "",
        "Conformance tests follow [CONFORMANCE_SPEC.md v1.0.0]"
        "(https://github.com/streamlinelabs/streamline/blob/main/tests/cross_sdk/CONFORMANCE_SPEC.md).",
        "",
        "## Badge URLs",
        "",
        "Use these in SDK READMEs:",
        "```markdown",
    ])

    for sdk in sorted(results.keys()):
        badge_name = sdk.replace("/", "-")
        lines.append(
            f"[![Conformance](https://img.shields.io/endpoint?"
            f"url=https://streamlinelabs.github.io/streamline/badges/{badge_name}.json)]"
            f"(https://streamlinelabs.dev/docs/sdks/conformance)"
        )

    lines.append("```")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines))
    print(f"  Generated matrix: {output_path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-dir", default=".", help="Directory with conformance-*.json files")
    parser.add_argument("--output-dir", default="./badges", help="Output directory for badges")
    args = parser.parse_args()

    results_dir = Path(args.results_dir)
    output_dir = Path(args.output_dir)

    results = {}
    for f in sorted(results_dir.glob("conformance-*.json")):
        try:
            data = json.loads(f.read_text())
            sdk = data.get("sdk", f.stem)
            status = data.get("status", "unknown")
            results[sdk] = {
                "status": status,
                "required_passed": data.get("required_passed", "?"),
                "optional_passed": data.get("optional_passed", "?"),
            }
            generate_badge(sdk, status, output_dir)
        except (json.JSONDecodeError, KeyError) as e:
            print(f"  Warning: Could not parse {f}: {e}")

    if not results:
        # Generate placeholder badges for all SDKs
        sdks = [
            "streamline-java-sdk",
            "streamline-python-sdk",
            "streamline-go-sdk",
            "streamline-node-sdk",
            "streamline-rust-sdk",
            "streamline-dotnet-sdk",
        ]
        for sdk in sdks:
            results[sdk] = {"status": "pending", "required_passed": "0", "optional_passed": "0"}
            generate_badge(sdk, "pending", output_dir)

    generate_matrix(results, output_dir / "CONFORMANCE_MATRIX.md")
    print(f"\nDone! Generated {len(results)} badges.")


if __name__ == "__main__":
    main()
