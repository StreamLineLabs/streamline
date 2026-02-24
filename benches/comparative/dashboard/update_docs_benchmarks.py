#!/usr/bin/env python3
"""
Update the docs site benchmark page with data from CI results.

Reads comparative-results.json (from run_comparative.sh) and patches
the BENCHMARK_DATA constant in benchmarks.tsx.

Usage:
  python3 update_docs_benchmarks.py \
    --input benchmark-results/comparative-results.json \
    --output ../streamline-docs/src/pages/benchmarks.tsx
"""

import argparse
import json
import re
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to comparative-results.json")
    parser.add_argument("--output", required=True, help="Path to benchmarks.tsx")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        print(f"Input not found: {input_path}", file=sys.stderr)
        sys.exit(1)
    if not output_path.exists():
        print(f"Output not found: {output_path}", file=sys.stderr)
        sys.exit(1)

    with open(input_path) as f:
        data = json.load(f)

    results = data.get("results", [])
    if not results:
        print("No results found in input", file=sys.stderr)
        sys.exit(1)

    # Build TypeScript array entries
    ts_entries = []
    for r in results:
        ts_entries.append(
            f"  {{ target: '{r['target']}', benchmark_type: '{r['benchmark_type']}', "
            f"throughput_msgs_sec: {r.get('throughput_msgs_sec', 0)}, "
            f"throughput_mb_sec: {r.get('throughput_mb_sec', 0)}, "
            f"latency_p50_ms: {r.get('latency_p50_ms', 0)}, "
            f"latency_p99_ms: {r.get('latency_p99_ms', 0)}, "
            f"startup_ms: {r.get('startup_ms', 0)} }},"
        )

    new_data_block = "const BENCHMARK_DATA: BenchmarkResult[] = [\n" + "\n".join(ts_entries) + "\n];"

    # Read the current benchmarks.tsx
    content = output_path.read_text()

    # Replace the BENCHMARK_DATA block
    pattern = r"const BENCHMARK_DATA: BenchmarkResult\[\] = \[.*?\];"
    if re.search(pattern, content, re.DOTALL):
        updated = re.sub(pattern, new_data_block, content, flags=re.DOTALL)
        output_path.write_text(updated)
        print(f"Updated {output_path} with {len(results)} benchmark results")
    else:
        print("Could not find BENCHMARK_DATA block in benchmarks.tsx", file=sys.stderr)
        sys.exit(1)

    # Also update the metadata comment
    metadata = data.get("metadata", {})
    version = metadata.get("version", "unknown")
    timestamp = metadata.get("timestamp", "unknown")
    print(f"  Version: {version}")
    print(f"  Timestamp: {timestamp}")
    print(f"  Results: {len(results)}")


if __name__ == "__main__":
    main()
