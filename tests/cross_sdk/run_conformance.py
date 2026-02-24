#!/usr/bin/env python3
"""
Conformance Test Runner — Executes tests and produces JSON report.

Wraps pytest to run test_conformance.py and generates a structured
JSON report compatible with the badge generator.

Usage:
  python3 run_conformance.py --sdk python --output results.json
  python3 run_conformance.py --sdk python --output results.json --server localhost:9092
"""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


def run_pytest_conformance(sdk_dir: str, server: str) -> dict:
    """Run conformance tests and parse results."""
    env = os.environ.copy()
    env["STREAMLINE_BOOTSTRAP"] = server
    env["STREAMLINE_HTTP_URL"] = f"http://{server.split(':')[0]}:9094"

    test_file = Path(sdk_dir) / "tests" / "test_conformance.py"
    if not test_file.exists():
        return {
            "sdk": f"streamline-python-sdk",
            "status": "skipped",
            "message": f"test_conformance.py not found at {test_file}",
            "results": {"total": 0, "passed": 0, "failed": 0, "skipped": 0},
            "details": [],
        }

    cmd = [
        sys.executable, "-m", "pytest",
        str(test_file),
        "-v", "--tb=short", "--timeout=30",
        f"--junitxml={sdk_dir}/conformance-report.xml",
        "-q",
    ]

    start = time.time()
    result = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=300)
    duration = time.time() - start

    # Parse output
    passed = failed = skipped = 0
    details = []

    for line in result.stdout.split("\n"):
        line = line.strip()
        if "PASSED" in line:
            test_name = line.split("::")[1].split(" ")[0] if "::" in line else line
            details.append({"id": extract_id(test_name), "name": test_name, "status": "passed"})
            passed += 1
        elif "FAILED" in line:
            test_name = line.split("::")[1].split(" ")[0] if "::" in line else line
            details.append({"id": extract_id(test_name), "name": test_name, "status": "failed"})
            failed += 1
        elif "SKIPPED" in line:
            skipped += 1

    total = passed + failed + skipped

    # Determine overall status
    if failed == 0 and passed > 0:
        status = "success"
    elif failed > 0:
        status = "failure"
    else:
        status = "unknown"

    return {
        "sdk": "streamline-python-sdk",
        "version": "0.2.0",
        "server_version": "0.2.0",
        "status": status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "duration_secs": round(duration, 2),
        "results": {
            "total": total,
            "passed": passed,
            "failed": failed,
            "skipped": skipped,
        },
        "required_passed": passed,
        "optional_passed": 0,
        "details": details,
    }


def extract_id(test_name: str) -> str:
    """Extract conformance ID (C01, T01, etc.) from test name."""
    for prefix in ["test_C", "test_T", "test_P", "test_N", "test_G", "test_E"]:
        if prefix in test_name:
            idx = test_name.index(prefix) + 5
            if idx + 2 <= len(test_name):
                return test_name[idx - 3:idx + 2].replace("test_", "")
    return test_name[:5]


def main():
    parser = argparse.ArgumentParser(description="Run SDK conformance tests")
    parser.add_argument("--sdk", required=True, choices=["python", "java", "go", "node", "rust", "dotnet"])
    parser.add_argument("--output", default="conformance-results.json")
    parser.add_argument("--server", default="localhost:9092")
    parser.add_argument("--sdk-dir", help="Override SDK directory path")
    args = parser.parse_args()

    # Determine SDK directory
    script_dir = Path(__file__).parent
    org_dir = script_dir.parent.parent.parent  # streamline/tests/cross_sdk -> org root

    sdk_dirs = {
        "python": org_dir / "streamline-python-sdk",
        "java": org_dir / "streamline-java-sdk",
        "go": org_dir / "streamline-go-sdk",
        "node": org_dir / "streamline-node-sdk",
        "rust": org_dir / "streamline-rust-sdk",
        "dotnet": org_dir / "streamline-dotnet-sdk",
    }

    sdk_dir = args.sdk_dir or str(sdk_dirs.get(args.sdk, "."))

    print(f"Running {args.sdk} conformance tests against {args.server}")
    print(f"SDK directory: {sdk_dir}")
    print()

    if args.sdk == "python":
        report = run_pytest_conformance(sdk_dir, args.server)
    else:
        report = {
            "sdk": f"streamline-{args.sdk}-sdk",
            "status": "not_implemented",
            "message": f"Conformance runner for {args.sdk} not yet implemented. Use the reference Python tests as a template.",
            "results": {"total": 0, "passed": 0, "failed": 0, "skipped": 0},
        }

    # Write report
    output_path = Path(args.output)
    output_path.write_text(json.dumps(report, indent=2))
    print(f"\nReport written to: {output_path}")
    print(f"Status: {report['status']}")
    print(f"Results: {report['results']}")

    # Exit with appropriate code
    sys.exit(0 if report["status"] in ("success", "not_implemented", "skipped") else 1)


if __name__ == "__main__":
    main()
