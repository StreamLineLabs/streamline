#!/usr/bin/env python3
"""
OpenMessaging Benchmark Results Processor

Parses OMB JSON results and generates comparison reports in multiple formats.

Usage:
    python process_results.py results/*.json --output report.md
    python process_results.py results/*.json --format html --output report.html
    python process_results.py results/*.json --format csv --output results.csv
    python process_results.py results/*.json --chart throughput.png
    python process_results.py results/*.json --heatmap heatmap.png
    python process_results.py results/*.json --efficiency --cpu-cores 2 --memory-gb 2
"""

import argparse
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

# Optional dependencies for charts
try:
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker
    import matplotlib.colors as mcolors
    import numpy as np
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

# Default resource limits (matching docker-compose.benchmarks.yml)
DEFAULT_CPU_CORES = 2.0
DEFAULT_MEMORY_GB = 2.0


@dataclass
class BenchmarkResult:
    """Parsed benchmark result from OMB JSON output."""
    driver: str
    workload: str
    timestamp: str

    # Throughput metrics
    publish_rate_msg_sec: float
    publish_rate_mb_sec: float
    consume_rate_msg_sec: float
    consume_rate_mb_sec: float

    # Latency metrics (milliseconds)
    publish_latency_avg: float
    publish_latency_p50: float
    publish_latency_p75: float
    publish_latency_p95: float
    publish_latency_p99: float
    publish_latency_p999: float
    publish_latency_max: float

    # End-to-end latency
    e2e_latency_avg: float
    e2e_latency_p50: float
    e2e_latency_p75: float
    e2e_latency_p95: float
    e2e_latency_p99: float
    e2e_latency_p999: float
    e2e_latency_max: float

    # Aggregated stats
    total_messages_sent: int
    total_messages_received: int
    test_duration_sec: float

    # Resource efficiency (calculated)
    msgs_per_cpu_core: float = 0.0
    msgs_per_gb_memory: float = 0.0
    mb_per_cpu_core: float = 0.0


def calculate_efficiency(result: 'BenchmarkResult', cpu_cores: float, memory_gb: float) -> 'BenchmarkResult':
    """Calculate resource efficiency metrics for a result."""
    result.msgs_per_cpu_core = result.publish_rate_msg_sec / cpu_cores if cpu_cores > 0 else 0
    result.msgs_per_gb_memory = result.publish_rate_msg_sec / memory_gb if memory_gb > 0 else 0
    result.mb_per_cpu_core = result.publish_rate_mb_sec / cpu_cores if cpu_cores > 0 else 0
    return result


def parse_result_file(filepath: Path) -> Optional[BenchmarkResult]:
    """Parse a single OMB JSON result file."""
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)

        # Extract driver name from filename or data
        driver = data.get('driver', filepath.stem.split('-')[0])
        workload = data.get('workload', 'unknown')

        # Handle different OMB result formats
        agg = data.get('aggregatedPublishLatencyQuantiles', data.get('publishLatencyQuantiles', {}))
        e2e = data.get('aggregatedEndToEndLatencyQuantiles', data.get('endToEndLatencyQuantiles', {}))

        return BenchmarkResult(
            driver=driver,
            workload=workload,
            timestamp=data.get('timestamp', datetime.now().isoformat()),

            publish_rate_msg_sec=data.get('publishRate', 0),
            publish_rate_mb_sec=data.get('publishThroughput', 0) / (1024 * 1024) if data.get('publishThroughput') else 0,
            consume_rate_msg_sec=data.get('consumeRate', 0),
            consume_rate_mb_sec=data.get('consumeThroughput', 0) / (1024 * 1024) if data.get('consumeThroughput') else 0,

            publish_latency_avg=data.get('aggregatedPublishLatencyAvg', data.get('publishLatencyAvg', 0)),
            publish_latency_p50=agg.get('50', agg.get('0.5', 0)),
            publish_latency_p75=agg.get('75', agg.get('0.75', 0)),
            publish_latency_p95=agg.get('95', agg.get('0.95', 0)),
            publish_latency_p99=agg.get('99', agg.get('0.99', 0)),
            publish_latency_p999=agg.get('99.9', agg.get('0.999', 0)),
            publish_latency_max=data.get('aggregatedPublishLatencyMax', data.get('publishLatencyMax', 0)),

            e2e_latency_avg=data.get('aggregatedEndToEndLatencyAvg', data.get('endToEndLatencyAvg', 0)),
            e2e_latency_p50=e2e.get('50', e2e.get('0.5', 0)),
            e2e_latency_p75=e2e.get('75', e2e.get('0.75', 0)),
            e2e_latency_p95=e2e.get('95', e2e.get('0.95', 0)),
            e2e_latency_p99=e2e.get('99', e2e.get('0.99', 0)),
            e2e_latency_p999=e2e.get('99.9', e2e.get('0.999', 0)),
            e2e_latency_max=data.get('aggregatedEndToEndLatencyMax', data.get('endToEndLatencyMax', 0)),

            total_messages_sent=data.get('totalMessagesSent', 0),
            total_messages_received=data.get('totalMessagesReceived', 0),
            test_duration_sec=data.get('testDurationSec', 0),
        )
    except Exception as e:
        print(f"Warning: Failed to parse {filepath}: {e}", file=sys.stderr)
        return None


def format_number(n: float, decimals: int = 2) -> str:
    """Format number with thousands separator."""
    if n >= 1_000_000:
        return f"{n/1_000_000:.{decimals}f}M"
    elif n >= 1_000:
        return f"{n/1_000:.{decimals}f}K"
    else:
        return f"{n:.{decimals}f}"


def format_latency(ms: float) -> str:
    """Format latency in appropriate units."""
    if ms >= 1000:
        return f"{ms/1000:.2f}s"
    elif ms >= 1:
        return f"{ms:.2f}ms"
    else:
        return f"{ms*1000:.1f}us"


def generate_markdown_report(results: List[BenchmarkResult], title: str = "Benchmark Results",
                             include_efficiency: bool = False) -> str:
    """Generate a Markdown comparison report."""
    if not results:
        return "# No results to display\n"

    lines = [
        f"# {title}",
        "",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "## Summary",
        "",
    ]

    # Group by workload
    workloads: Dict[str, List[BenchmarkResult]] = {}
    for r in results:
        workloads.setdefault(r.workload, []).append(r)

    for workload, workload_results in workloads.items():
        lines.extend([
            f"### Workload: {workload}",
            "",
            "#### Throughput Comparison",
            "",
            "| System | Publish Rate | Publish Throughput | Consume Rate | Consume Throughput |",
            "|--------|-------------|-------------------|--------------|-------------------|",
        ])

        # Sort by publish rate descending
        sorted_results = sorted(workload_results, key=lambda x: x.publish_rate_msg_sec, reverse=True)

        for r in sorted_results:
            lines.append(
                f"| {r.driver} | {format_number(r.publish_rate_msg_sec)} msg/s | "
                f"{r.publish_rate_mb_sec:.2f} MB/s | {format_number(r.consume_rate_msg_sec)} msg/s | "
                f"{r.consume_rate_mb_sec:.2f} MB/s |"
            )

        lines.extend([
            "",
            "#### Publish Latency (ms)",
            "",
            "| System | Avg | P50 | P75 | P95 | P99 | P99.9 | Max |",
            "|--------|-----|-----|-----|-----|-----|-------|-----|",
        ])

        # Sort by p99 latency ascending (lower is better)
        sorted_results = sorted(workload_results, key=lambda x: x.publish_latency_p99)

        for r in sorted_results:
            lines.append(
                f"| {r.driver} | {format_latency(r.publish_latency_avg)} | "
                f"{format_latency(r.publish_latency_p50)} | {format_latency(r.publish_latency_p75)} | "
                f"{format_latency(r.publish_latency_p95)} | {format_latency(r.publish_latency_p99)} | "
                f"{format_latency(r.publish_latency_p999)} | {format_latency(r.publish_latency_max)} |"
            )

        lines.extend([
            "",
            "#### End-to-End Latency (ms)",
            "",
            "| System | Avg | P50 | P75 | P95 | P99 | P99.9 | Max |",
            "|--------|-----|-----|-----|-----|-----|-------|-----|",
        ])

        # Sort by e2e p99 latency ascending
        sorted_results = sorted(workload_results, key=lambda x: x.e2e_latency_p99)

        for r in sorted_results:
            lines.append(
                f"| {r.driver} | {format_latency(r.e2e_latency_avg)} | "
                f"{format_latency(r.e2e_latency_p50)} | {format_latency(r.e2e_latency_p75)} | "
                f"{format_latency(r.e2e_latency_p95)} | {format_latency(r.e2e_latency_p99)} | "
                f"{format_latency(r.e2e_latency_p999)} | {format_latency(r.e2e_latency_max)} |"
            )

        # Resource efficiency table
        if include_efficiency:
            lines.extend([
                "",
                "#### Resource Efficiency",
                "",
                "| System | Msgs/CPU Core | Msgs/GB Memory | MB/s per CPU |",
                "|--------|---------------|----------------|--------------|",
            ])

            # Sort by msgs per CPU descending (higher is better)
            sorted_results = sorted(workload_results, key=lambda x: x.msgs_per_cpu_core, reverse=True)

            for r in sorted_results:
                lines.append(
                    f"| {r.driver} | {format_number(r.msgs_per_cpu_core)} | "
                    f"{format_number(r.msgs_per_gb_memory)} | {r.mb_per_cpu_core:.2f} |"
                )

        lines.append("")

    # Add winner summary
    lines.extend([
        "## Performance Leaders",
        "",
    ])

    all_results = results
    if all_results:
        # Throughput winner
        throughput_winner = max(all_results, key=lambda x: x.publish_rate_msg_sec)
        lines.append(f"- **Highest Throughput**: {throughput_winner.driver} ({format_number(throughput_winner.publish_rate_msg_sec)} msg/s)")

        # Latency winner
        latency_winner = min(all_results, key=lambda x: x.publish_latency_p99)
        lines.append(f"- **Lowest P99 Latency**: {latency_winner.driver} ({format_latency(latency_winner.publish_latency_p99)})")

        # E2E winner
        e2e_winner = min(all_results, key=lambda x: x.e2e_latency_p99)
        lines.append(f"- **Lowest E2E P99 Latency**: {e2e_winner.driver} ({format_latency(e2e_winner.e2e_latency_p99)})")

        # Efficiency winner
        if include_efficiency:
            efficiency_winner = max(all_results, key=lambda x: x.msgs_per_cpu_core)
            lines.append(f"- **Most CPU Efficient**: {efficiency_winner.driver} ({format_number(efficiency_winner.msgs_per_cpu_core)} msg/s/core)")

    lines.append("")
    return "\n".join(lines)


def generate_csv_report(results: List[BenchmarkResult], include_efficiency: bool = False) -> str:
    """Generate a CSV report."""
    headers = [
        "driver", "workload", "timestamp",
        "publish_rate_msg_sec", "publish_rate_mb_sec",
        "consume_rate_msg_sec", "consume_rate_mb_sec",
        "publish_latency_avg_ms", "publish_latency_p50_ms", "publish_latency_p75_ms",
        "publish_latency_p95_ms", "publish_latency_p99_ms", "publish_latency_p999_ms",
        "publish_latency_max_ms",
        "e2e_latency_avg_ms", "e2e_latency_p50_ms", "e2e_latency_p75_ms",
        "e2e_latency_p95_ms", "e2e_latency_p99_ms", "e2e_latency_p999_ms",
        "e2e_latency_max_ms",
        "total_messages_sent", "total_messages_received", "test_duration_sec"
    ]

    if include_efficiency:
        headers.extend(["msgs_per_cpu_core", "msgs_per_gb_memory", "mb_per_cpu_core"])

    lines = [",".join(headers)]

    for r in results:
        row = [
            r.driver, r.workload, r.timestamp,
            str(r.publish_rate_msg_sec), str(r.publish_rate_mb_sec),
            str(r.consume_rate_msg_sec), str(r.consume_rate_mb_sec),
            str(r.publish_latency_avg), str(r.publish_latency_p50), str(r.publish_latency_p75),
            str(r.publish_latency_p95), str(r.publish_latency_p99), str(r.publish_latency_p999),
            str(r.publish_latency_max),
            str(r.e2e_latency_avg), str(r.e2e_latency_p50), str(r.e2e_latency_p75),
            str(r.e2e_latency_p95), str(r.e2e_latency_p99), str(r.e2e_latency_p999),
            str(r.e2e_latency_max),
            str(r.total_messages_sent), str(r.total_messages_received), str(r.test_duration_sec)
        ]
        if include_efficiency:
            row.extend([str(r.msgs_per_cpu_core), str(r.msgs_per_gb_memory), str(r.mb_per_cpu_core)])
        lines.append(",".join(row))

    return "\n".join(lines)


def generate_html_report(results: List[BenchmarkResult], title: str = "Benchmark Results",
                         include_efficiency: bool = False) -> str:
    """Generate an HTML comparison report."""
    if not results:
        return "<html><body><h1>No results to display</h1></body></html>"

    # Group by workload
    workloads: Dict[str, List[BenchmarkResult]] = {}
    for r in results:
        workloads.setdefault(r.workload, []).append(r)

    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>{title}</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 40px; background: #f5f5f5; }}
        h1 {{ color: #333; }}
        h2 {{ color: #555; margin-top: 30px; }}
        h3 {{ color: #666; }}
        table {{ border-collapse: collapse; width: 100%; margin: 20px 0; background: white; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
        th, td {{ border: 1px solid #ddd; padding: 12px; text-align: right; }}
        th {{ background: #4a90d9; color: white; text-align: center; }}
        td:first-child {{ text-align: left; font-weight: bold; }}
        tr:nth-child(even) {{ background: #f9f9f9; }}
        tr:hover {{ background: #f0f7ff; }}
        .winner {{ background: #d4edda !important; }}
        .summary {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin: 20px 0; }}
        .metric {{ display: inline-block; margin: 10px 20px; text-align: center; }}
        .metric-value {{ font-size: 24px; font-weight: bold; color: #4a90d9; }}
        .metric-label {{ font-size: 12px; color: #888; }}
        .timestamp {{ color: #888; font-size: 14px; }}
    </style>
</head>
<body>
    <h1>{title}</h1>
    <p class="timestamp">Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
"""

    for workload, workload_results in workloads.items():
        html += f"<h2>Workload: {workload}</h2>"

        # Throughput table
        html += """
    <h3>Throughput Comparison</h3>
    <table>
        <tr>
            <th>System</th>
            <th>Publish Rate</th>
            <th>Publish Throughput</th>
            <th>Consume Rate</th>
            <th>Consume Throughput</th>
        </tr>
"""
        sorted_results = sorted(workload_results, key=lambda x: x.publish_rate_msg_sec, reverse=True)
        max_rate = sorted_results[0].publish_rate_msg_sec if sorted_results else 0

        for r in sorted_results:
            winner_class = ' class="winner"' if r.publish_rate_msg_sec == max_rate else ''
            html += f"""        <tr{winner_class}>
            <td>{r.driver}</td>
            <td>{format_number(r.publish_rate_msg_sec)} msg/s</td>
            <td>{r.publish_rate_mb_sec:.2f} MB/s</td>
            <td>{format_number(r.consume_rate_msg_sec)} msg/s</td>
            <td>{r.consume_rate_mb_sec:.2f} MB/s</td>
        </tr>
"""
        html += "    </table>\n"

        # Latency table
        html += """
    <h3>Publish Latency</h3>
    <table>
        <tr>
            <th>System</th>
            <th>Avg</th>
            <th>P50</th>
            <th>P75</th>
            <th>P95</th>
            <th>P99</th>
            <th>P99.9</th>
            <th>Max</th>
        </tr>
"""
        sorted_results = sorted(workload_results, key=lambda x: x.publish_latency_p99)
        min_latency = sorted_results[0].publish_latency_p99 if sorted_results else float('inf')

        for r in sorted_results:
            winner_class = ' class="winner"' if r.publish_latency_p99 == min_latency else ''
            html += f"""        <tr{winner_class}>
            <td>{r.driver}</td>
            <td>{format_latency(r.publish_latency_avg)}</td>
            <td>{format_latency(r.publish_latency_p50)}</td>
            <td>{format_latency(r.publish_latency_p75)}</td>
            <td>{format_latency(r.publish_latency_p95)}</td>
            <td>{format_latency(r.publish_latency_p99)}</td>
            <td>{format_latency(r.publish_latency_p999)}</td>
            <td>{format_latency(r.publish_latency_max)}</td>
        </tr>
"""
        html += "    </table>\n"

        # E2E Latency table
        html += """
    <h3>End-to-End Latency</h3>
    <table>
        <tr>
            <th>System</th>
            <th>Avg</th>
            <th>P50</th>
            <th>P75</th>
            <th>P95</th>
            <th>P99</th>
            <th>P99.9</th>
            <th>Max</th>
        </tr>
"""
        sorted_results = sorted(workload_results, key=lambda x: x.e2e_latency_p99)
        min_e2e = sorted_results[0].e2e_latency_p99 if sorted_results else float('inf')

        for r in sorted_results:
            winner_class = ' class="winner"' if r.e2e_latency_p99 == min_e2e else ''
            html += f"""        <tr{winner_class}>
            <td>{r.driver}</td>
            <td>{format_latency(r.e2e_latency_avg)}</td>
            <td>{format_latency(r.e2e_latency_p50)}</td>
            <td>{format_latency(r.e2e_latency_p75)}</td>
            <td>{format_latency(r.e2e_latency_p95)}</td>
            <td>{format_latency(r.e2e_latency_p99)}</td>
            <td>{format_latency(r.e2e_latency_p999)}</td>
            <td>{format_latency(r.e2e_latency_max)}</td>
        </tr>
"""
        html += "    </table>\n"

        # Resource efficiency table
        if include_efficiency:
            html += """
    <h3>Resource Efficiency</h3>
    <table>
        <tr>
            <th>System</th>
            <th>Msgs/CPU Core</th>
            <th>Msgs/GB Memory</th>
            <th>MB/s per CPU</th>
        </tr>
"""
            sorted_results = sorted(workload_results, key=lambda x: x.msgs_per_cpu_core, reverse=True)
            max_efficiency = sorted_results[0].msgs_per_cpu_core if sorted_results else 0

            for r in sorted_results:
                winner_class = ' class="winner"' if r.msgs_per_cpu_core == max_efficiency else ''
                html += f"""        <tr{winner_class}>
            <td>{r.driver}</td>
            <td>{format_number(r.msgs_per_cpu_core)}</td>
            <td>{format_number(r.msgs_per_gb_memory)}</td>
            <td>{r.mb_per_cpu_core:.2f}</td>
        </tr>
"""
            html += "    </table>\n"

    # Summary section
    if results:
        throughput_winner = max(results, key=lambda x: x.publish_rate_msg_sec)
        latency_winner = min(results, key=lambda x: x.publish_latency_p99)
        e2e_winner = min(results, key=lambda x: x.e2e_latency_p99)

        html += f"""
    <h2>Performance Leaders</h2>
    <div class="summary">
        <div class="metric">
            <div class="metric-value">{throughput_winner.driver}</div>
            <div class="metric-label">Highest Throughput ({format_number(throughput_winner.publish_rate_msg_sec)} msg/s)</div>
        </div>
        <div class="metric">
            <div class="metric-value">{latency_winner.driver}</div>
            <div class="metric-label">Lowest P99 Latency ({format_latency(latency_winner.publish_latency_p99)})</div>
        </div>
        <div class="metric">
            <div class="metric-value">{e2e_winner.driver}</div>
            <div class="metric-label">Lowest E2E P99 ({format_latency(e2e_winner.e2e_latency_p99)})</div>
        </div>"""

        if include_efficiency:
            efficiency_winner = max(results, key=lambda x: x.msgs_per_cpu_core)
            html += f"""
        <div class="metric">
            <div class="metric-value">{efficiency_winner.driver}</div>
            <div class="metric-label">Most CPU Efficient ({format_number(efficiency_winner.msgs_per_cpu_core)} msg/s/core)</div>
        </div>"""

        html += """
    </div>
"""

    html += """
</body>
</html>
"""
    return html


def generate_charts(results: List[BenchmarkResult], output_prefix: str):
    """Generate comparison charts (requires matplotlib)."""
    if not HAS_MATPLOTLIB:
        print("Warning: matplotlib not installed. Skipping chart generation.", file=sys.stderr)
        print("Install with: pip install matplotlib", file=sys.stderr)
        return

    if not results:
        return

    # Group by workload
    workloads: Dict[str, List[BenchmarkResult]] = {}
    for r in results:
        workloads.setdefault(r.workload, []).append(r)

    for workload, workload_results in workloads.items():
        drivers = [r.driver for r in workload_results]

        # Throughput chart
        fig, ax = plt.subplots(figsize=(10, 6))
        rates = [r.publish_rate_msg_sec for r in workload_results]
        bars = ax.bar(drivers, rates, color='#4a90d9')
        ax.set_ylabel('Messages/sec')
        ax.set_title(f'Publish Throughput - {workload}')
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: format_number(x)))

        # Add value labels on bars
        for bar, rate in zip(bars, rates):
            height = bar.get_height()
            ax.annotate(format_number(rate),
                       xy=(bar.get_x() + bar.get_width() / 2, height),
                       xytext=(0, 3),
                       textcoords="offset points",
                       ha='center', va='bottom', fontsize=9)

        plt.tight_layout()
        plt.savefig(f"{output_prefix}-throughput-{workload}.png", dpi=150)
        plt.close()

        # Latency chart (P50, P99, P99.9)
        fig, ax = plt.subplots(figsize=(12, 6))
        x = range(len(drivers))
        width = 0.25

        p50 = [r.publish_latency_p50 for r in workload_results]
        p99 = [r.publish_latency_p99 for r in workload_results]
        p999 = [r.publish_latency_p999 for r in workload_results]

        bars1 = ax.bar([i - width for i in x], p50, width, label='P50', color='#28a745')
        bars2 = ax.bar(x, p99, width, label='P99', color='#ffc107')
        bars3 = ax.bar([i + width for i in x], p999, width, label='P99.9', color='#dc3545')

        ax.set_ylabel('Latency (ms)')
        ax.set_title(f'Publish Latency Percentiles - {workload}')
        ax.set_xticks(x)
        ax.set_xticklabels(drivers)
        ax.legend()
        ax.set_yscale('log')

        plt.tight_layout()
        plt.savefig(f"{output_prefix}-latency-{workload}.png", dpi=150)
        plt.close()

        print(f"Generated charts for workload: {workload}")


def generate_latency_heatmap(results: List[BenchmarkResult], output_path: str):
    """Generate a latency percentile heatmap comparing all systems."""
    if not HAS_MATPLOTLIB:
        print("Warning: matplotlib not installed. Skipping heatmap generation.", file=sys.stderr)
        return

    if not results:
        return

    # Group by workload
    workloads: Dict[str, List[BenchmarkResult]] = {}
    for r in results:
        workloads.setdefault(r.workload, []).append(r)

    for workload, workload_results in workloads.items():
        drivers = [r.driver for r in workload_results]
        percentiles = ['P50', 'P75', 'P95', 'P99', 'P99.9', 'Max']

        # Build data matrix
        data = []
        for r in workload_results:
            data.append([
                r.publish_latency_p50,
                r.publish_latency_p75,
                r.publish_latency_p95,
                r.publish_latency_p99,
                r.publish_latency_p999,
                r.publish_latency_max,
            ])

        data = np.array(data)

        # Create heatmap
        fig, ax = plt.subplots(figsize=(12, max(6, len(drivers) * 0.8)))

        # Use log scale for better visualization of latency differences
        data_log = np.log10(data + 0.001)  # Add small value to avoid log(0)

        # Custom colormap: green (low) -> yellow -> red (high)
        cmap = plt.cm.RdYlGn_r

        im = ax.imshow(data_log, cmap=cmap, aspect='auto')

        # Labels
        ax.set_xticks(np.arange(len(percentiles)))
        ax.set_yticks(np.arange(len(drivers)))
        ax.set_xticklabels(percentiles)
        ax.set_yticklabels(drivers)

        # Rotate x labels
        plt.setp(ax.get_xticklabels(), rotation=45, ha="right", rotation_mode="anchor")

        # Add value annotations
        for i in range(len(drivers)):
            for j in range(len(percentiles)):
                value = data[i, j]
                text = format_latency(value)
                # Choose text color based on background
                text_color = 'white' if data_log[i, j] > np.median(data_log) else 'black'
                ax.text(j, i, text, ha="center", va="center", color=text_color, fontsize=9)

        ax.set_title(f'Publish Latency Heatmap - {workload}\n(Green = Lower = Better)')

        # Add colorbar
        cbar = ax.figure.colorbar(im, ax=ax)
        cbar.ax.set_ylabel('Latency (log scale)', rotation=-90, va="bottom")

        plt.tight_layout()
        plt.savefig(f"{output_path}-heatmap-{workload}.png", dpi=150, bbox_inches='tight')
        plt.close()

        print(f"Generated heatmap for workload: {workload}")


def generate_efficiency_chart(results: List[BenchmarkResult], output_path: str):
    """Generate resource efficiency comparison charts."""
    if not HAS_MATPLOTLIB:
        print("Warning: matplotlib not installed. Skipping efficiency chart.", file=sys.stderr)
        return

    if not results:
        return

    # Group by workload
    workloads: Dict[str, List[BenchmarkResult]] = {}
    for r in results:
        workloads.setdefault(r.workload, []).append(r)

    for workload, workload_results in workloads.items():
        drivers = [r.driver for r in workload_results]

        fig, axes = plt.subplots(1, 3, figsize=(15, 5))

        # Messages per CPU core
        ax1 = axes[0]
        values = [r.msgs_per_cpu_core for r in workload_results]
        bars = ax1.bar(drivers, values, color='#4a90d9')
        ax1.set_ylabel('Messages/sec per CPU core')
        ax1.set_title('CPU Efficiency')
        ax1.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: format_number(x)))
        for bar, val in zip(bars, values):
            ax1.annotate(format_number(val),
                        xy=(bar.get_x() + bar.get_width() / 2, bar.get_height()),
                        xytext=(0, 3), textcoords="offset points",
                        ha='center', va='bottom', fontsize=9)

        # Messages per GB memory
        ax2 = axes[1]
        values = [r.msgs_per_gb_memory for r in workload_results]
        bars = ax2.bar(drivers, values, color='#28a745')
        ax2.set_ylabel('Messages/sec per GB memory')
        ax2.set_title('Memory Efficiency')
        ax2.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: format_number(x)))
        for bar, val in zip(bars, values):
            ax2.annotate(format_number(val),
                        xy=(bar.get_x() + bar.get_width() / 2, bar.get_height()),
                        xytext=(0, 3), textcoords="offset points",
                        ha='center', va='bottom', fontsize=9)

        # MB/s per CPU core
        ax3 = axes[2]
        values = [r.mb_per_cpu_core for r in workload_results]
        bars = ax3.bar(drivers, values, color='#ffc107')
        ax3.set_ylabel('MB/sec per CPU core')
        ax3.set_title('Throughput per CPU')
        for bar, val in zip(bars, values):
            ax3.annotate(f'{val:.1f}',
                        xy=(bar.get_x() + bar.get_width() / 2, bar.get_height()),
                        xytext=(0, 3), textcoords="offset points",
                        ha='center', va='bottom', fontsize=9)

        plt.suptitle(f'Resource Efficiency - {workload}', fontsize=14)
        plt.tight_layout()
        plt.savefig(f"{output_path}-efficiency-{workload}.png", dpi=150, bbox_inches='tight')
        plt.close()

        print(f"Generated efficiency chart for workload: {workload}")


def main():
    parser = argparse.ArgumentParser(
        description="Process OpenMessaging Benchmark results",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Generate markdown report
    python process_results.py results/*.json --output report.md

    # Generate HTML report
    python process_results.py results/*.json --format html --output report.html

    # Generate CSV for spreadsheet
    python process_results.py results/*.json --format csv --output results.csv

    # Generate charts (requires matplotlib)
    python process_results.py results/*.json --chart charts/benchmark

    # All at once
    python process_results.py results/*.json --format html --output report.html --chart charts/benchmark
        """
    )

    parser.add_argument('files', nargs='+', help='OMB JSON result files')
    parser.add_argument('--output', '-o', help='Output file path')
    parser.add_argument('--format', '-f', choices=['markdown', 'html', 'csv'], default='markdown',
                        help='Output format (default: markdown)')
    parser.add_argument('--title', '-t', default='Streamline Benchmark Results',
                        help='Report title')
    parser.add_argument('--chart', '-c', help='Generate charts with given prefix (requires matplotlib)')
    parser.add_argument('--heatmap', help='Generate latency heatmap with given prefix (requires matplotlib)')
    parser.add_argument('--efficiency', action='store_true',
                        help='Include resource efficiency metrics in report')
    parser.add_argument('--cpu-cores', type=float, default=DEFAULT_CPU_CORES,
                        help=f'CPU cores per system for efficiency calculation (default: {DEFAULT_CPU_CORES})')
    parser.add_argument('--memory-gb', type=float, default=DEFAULT_MEMORY_GB,
                        help=f'Memory GB per system for efficiency calculation (default: {DEFAULT_MEMORY_GB})')

    args = parser.parse_args()

    # Parse all result files
    results = []
    for filepath in args.files:
        path = Path(filepath)
        if path.exists():
            result = parse_result_file(path)
            if result:
                results.append(result)
        else:
            print(f"Warning: File not found: {filepath}", file=sys.stderr)

    if not results:
        print("No valid results found.", file=sys.stderr)
        sys.exit(1)

    print(f"Parsed {len(results)} result file(s)", file=sys.stderr)

    # Calculate efficiency metrics if requested
    if args.efficiency or args.heatmap:
        for result in results:
            calculate_efficiency(result, args.cpu_cores, args.memory_gb)
        print(f"Calculated efficiency with {args.cpu_cores} CPU cores, {args.memory_gb}GB memory", file=sys.stderr)

    # Generate report
    if args.format == 'markdown':
        report = generate_markdown_report(results, args.title, include_efficiency=args.efficiency)
    elif args.format == 'html':
        report = generate_html_report(results, args.title, include_efficiency=args.efficiency)
    elif args.format == 'csv':
        report = generate_csv_report(results, include_efficiency=args.efficiency)

    # Write or print output
    if args.output:
        with open(args.output, 'w') as f:
            f.write(report)
        print(f"Report written to: {args.output}", file=sys.stderr)
    else:
        print(report)

    # Generate charts if requested
    if args.chart:
        generate_charts(results, args.chart)
        if args.efficiency:
            generate_efficiency_chart(results, args.chart)

    # Generate heatmap if requested
    if args.heatmap:
        generate_latency_heatmap(results, args.heatmap)


if __name__ == '__main__':
    main()
