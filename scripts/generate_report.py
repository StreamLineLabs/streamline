#!/usr/bin/env python3
"""
Benchmark Report Generator

Generates a markdown report from benchmark JSON results.

Usage:
    python3 generate_report.py --input-dir ./benchmark-results --output BENCHMARK_REPORT.md
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate markdown benchmark report from JSON results"
    )
    parser.add_argument(
        "--input-dir",
        type=str,
        required=True,
        help="Directory containing benchmark JSON files",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="BENCHMARK_REPORT.md",
        help="Output markdown file path",
    )
    parser.add_argument(
        "--title",
        type=str,
        default="Streamline Benchmark Results",
        help="Report title",
    )
    parser.add_argument(
        "--compare",
        action="store_true",
        help="Generate comparison tables between targets",
    )
    return parser.parse_args()


def load_results(input_dir: str) -> List[Dict[str, Any]]:
    """Load all benchmark results from JSON files."""
    results = []
    input_path = Path(input_dir)

    if not input_path.exists():
        print(f"Warning: Input directory does not exist: {input_dir}", file=sys.stderr)
        return results

    for json_file in input_path.glob("*.json"):
        try:
            with open(json_file, "r") as f:
                data = json.load(f)
                if isinstance(data, list):
                    results.extend(data)
                else:
                    results.append(data)
        except (json.JSONDecodeError, IOError) as e:
            print(f"Warning: Failed to load {json_file}: {e}", file=sys.stderr)

    return results


def group_by_benchmark_type(results: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Group results by benchmark type."""
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for result in results:
        benchmark_type = result.get("benchmark_type", "unknown")
        if benchmark_type not in grouped:
            grouped[benchmark_type] = []
        grouped[benchmark_type].append(result)
    return grouped


def group_by_target(results: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Group results by target system."""
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for result in results:
        target = result.get("target", "unknown")
        if target not in grouped:
            grouped[target] = []
        grouped[target].append(result)
    return grouped


def format_number(value: float, precision: int = 2) -> str:
    """Format a number with comma separators."""
    if value >= 1_000_000:
        return f"{value / 1_000_000:.{precision}f}M"
    elif value >= 1_000:
        return f"{value / 1_000:.{precision}f}K"
    else:
        return f"{value:.{precision}f}"


def generate_throughput_table(results: List[Dict[str, Any]], title: str) -> str:
    """Generate a markdown table for throughput results."""
    if not results:
        return ""

    lines = [
        f"### {title}\n",
        "| System | Messages/sec | MB/sec | p50 Latency | p99 Latency | p99.9 Latency |",
        "|--------|--------------|--------|-------------|-------------|---------------|",
    ]

    for result in sorted(results, key=lambda x: x.get("target", "")):
        target = result.get("target", "unknown")
        msgs_sec = format_number(result.get("throughput_msgs_sec", 0))
        mb_sec = f"{result.get('throughput_mb_sec', 0):.2f}"
        p50 = f"{result.get('latency_p50_ms', 0):.2f} ms"
        p99 = f"{result.get('latency_p99_ms', 0):.2f} ms"
        p999 = f"{result.get('latency_p999_ms', 0):.2f} ms"

        lines.append(f"| {target} | {msgs_sec} | {mb_sec} | {p50} | {p99} | {p999} |")

    lines.append("")
    return "\n".join(lines)


def generate_startup_table(results: List[Dict[str, Any]]) -> str:
    """Generate a markdown table for startup time results."""
    if not results:
        return ""

    lines = [
        "### Startup Time\n",
        "| System | Cold Start | Warm Start | Time to First Request |",
        "|--------|------------|------------|----------------------|",
    ]

    for result in sorted(results, key=lambda x: x.get("target", "")):
        target = result.get("target", "unknown")
        cold_start = f"{result.get('cold_start_ms', 0):.0f} ms"
        warm_start = f"{result.get('warm_start_ms', 0):.0f} ms"
        ttfr = f"{result.get('time_to_first_request_ms', 0):.0f} ms"

        lines.append(f"| {target} | {cold_start} | {warm_start} | {ttfr} |")

    lines.append("")
    return "\n".join(lines)


def generate_memory_table(results: List[Dict[str, Any]]) -> str:
    """Generate a markdown table for memory usage results."""
    if not results:
        return ""

    lines = [
        "### Memory Usage\n",
        "| System | Idle | 100K msg/s | 500K msg/s |",
        "|--------|------|------------|------------|",
    ]

    for result in sorted(results, key=lambda x: x.get("target", "")):
        target = result.get("target", "unknown")
        idle = f"{result.get('memory_idle_mb', 0):.0f} MB"
        m100k = f"{result.get('memory_100k_mb', 0):.0f} MB"
        m500k = f"{result.get('memory_500k_mb', 0):.0f} MB"

        lines.append(f"| {target} | {idle} | {m100k} | {m500k} |")

    lines.append("")
    return "\n".join(lines)


def generate_config_section(results: List[Dict[str, Any]]) -> str:
    """Generate the configuration section."""
    if not results:
        return ""

    # Get config from first result
    config = results[0].get("config", {})

    lines = [
        "## Test Configuration\n",
        "| Parameter | Value |",
        "|-----------|-------|",
        f"| Message Size | {config.get('message_size', 'N/A')} bytes |",
        f"| Batch Size | {config.get('batch_size', 'N/A')} |",
        f"| Partitions | {config.get('num_partitions', 'N/A')} |",
        f"| Duration | {config.get('duration_secs', 'N/A')} seconds |",
        f"| Warmup | {config.get('warmup_secs', 'N/A')} seconds |",
        f"| Producers | {config.get('num_producers', 'N/A')} |",
        f"| Consumers | {config.get('num_consumers', 'N/A')} |",
        "",
    ]

    return "\n".join(lines)


def generate_environment_section() -> str:
    """Generate the environment section."""
    import platform

    lines = [
        "## Test Environment\n",
        "| Component | Value |",
        "|-----------|-------|",
        f"| OS | {platform.system()} {platform.release()} |",
        f"| Architecture | {platform.machine()} |",
        f"| Python | {platform.python_version()} |",
        f"| Date | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} |",
        "",
    ]

    return "\n".join(lines)


def generate_report(
    results: List[Dict[str, Any]],
    title: str,
    compare: bool = False
) -> str:
    """Generate the full markdown report."""
    lines = [
        f"# {title}\n",
        f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n",
    ]

    if not results:
        lines.append("\n**No benchmark results found.**\n")
        return "\n".join(lines)

    # Group by benchmark type
    by_type = group_by_benchmark_type(results)

    # Table of Contents
    lines.extend([
        "## Table of Contents\n",
        "- [Summary](#summary)",
        "- [Producer Throughput](#producer-throughput)",
        "- [Consumer Throughput](#consumer-throughput)",
        "- [End-to-End Latency](#end-to-end-latency)",
        "- [Startup Time](#startup-time)",
        "- [Memory Usage](#memory-usage)",
        "- [Test Configuration](#test-configuration)",
        "- [Test Environment](#test-environment)",
        "- [Methodology](#methodology)",
        "",
    ])

    # Summary section
    lines.extend([
        "## Summary\n",
        "This report compares the performance of Streamline against Apache Kafka and Redpanda.",
        "",
        "**Key Findings:**",
        "",
    ])

    # Add findings based on results
    if "producer" in by_type:
        streamline_results = [r for r in by_type["producer"] if r.get("target") == "streamline"]
        if streamline_results:
            msgs_sec = streamline_results[0].get("throughput_msgs_sec", 0)
            lines.append(f"- **Producer Throughput**: Streamline achieves {format_number(msgs_sec)} messages/second")

    if "startup" in by_type:
        streamline_results = [r for r in by_type["startup"] if r.get("target") == "streamline"]
        if streamline_results:
            startup_ms = streamline_results[0].get("cold_start_ms", 0)
            lines.append(f"- **Startup Time**: Streamline starts in {startup_ms:.0f}ms")

    lines.append("")

    # Producer Throughput
    lines.append("## Producer Throughput\n")
    if "producer" in by_type:
        lines.append(generate_throughput_table(by_type["producer"], "Producer Benchmark"))
    else:
        lines.append("*No producer benchmark results available.*\n")

    # Consumer Throughput
    lines.append("## Consumer Throughput\n")
    if "consumer" in by_type:
        lines.append(generate_throughput_table(by_type["consumer"], "Consumer Benchmark"))
    else:
        lines.append("*No consumer benchmark results available.*\n")

    # End-to-End Latency
    lines.append("## End-to-End Latency\n")
    if "e2e" in by_type:
        lines.append(generate_throughput_table(by_type["e2e"], "E2E Latency Benchmark"))
    else:
        lines.append("*No end-to-end latency benchmark results available.*\n")

    # Startup Time
    lines.append("## Startup Time\n")
    if "startup" in by_type:
        lines.append(generate_startup_table(by_type["startup"]))
    else:
        lines.append("*No startup time benchmark results available.*\n")

    # Memory Usage
    lines.append("## Memory Usage\n")
    if "memory" in by_type:
        lines.append(generate_memory_table(by_type["memory"]))
    else:
        lines.append("*No memory usage benchmark results available.*\n")

    # Configuration
    lines.append(generate_config_section(results))

    # Environment
    lines.append(generate_environment_section())

    # Methodology
    lines.extend([
        "## Methodology\n",
        "### Benchmark Procedure\n",
        "1. Each system is started fresh before benchmarks",
        "2. A warmup period allows the system to reach steady state",
        "3. Measurements are taken over the configured duration",
        "4. Results are averaged across multiple runs where applicable",
        "",
        "### Metrics Collected\n",
        "- **Throughput**: Messages and bytes processed per second",
        "- **Latency**: Time from send to acknowledgment (p50, p99, p99.9)",
        "- **Startup Time**: Time from process start to ready state",
        "- **Memory**: RSS memory usage under various load levels",
        "",
        "### Fair Comparison Notes\n",
        "- All systems run with default configurations unless noted",
        "- Kafka and Redpanda run in Docker containers",
        "- Streamline runs as a native binary",
        "- Network round-trip is localhost (minimal)",
        "",
    ])

    return "\n".join(lines)


def main():
    """Main entry point."""
    args = parse_args()

    # Load results
    results = load_results(args.input_dir)

    if not results:
        print("No benchmark results found. Generating empty report.", file=sys.stderr)

    # Generate report
    report = generate_report(results, args.title, args.compare)

    # Write output
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        f.write(report)

    print(f"Report generated: {args.output}")


if __name__ == "__main__":
    main()
