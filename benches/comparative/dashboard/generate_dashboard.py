#!/usr/bin/env python3
"""Generate an HTML benchmark dashboard from comparative benchmark results."""

import argparse
import json
import sys
from pathlib import Path


def generate_dashboard(results_path: str, output_path: str) -> None:
    with open(results_path) as f:
        data = json.load(f)

    metadata = data.get("metadata", {})
    results = data.get("results", [])

    # Group by benchmark type
    grouped: dict[str, list] = {}
    for r in results:
        bt = r["benchmark_type"]
        grouped.setdefault(bt, []).append(r)

    # Color mapping
    colors = {
        "streamline": "#10b981",
        "kafka": "#f59e0b",
        "redpanda": "#ef4444",
    }

    charts_js = ""
    chart_divs = ""

    for i, (bench_type, bench_results) in enumerate(grouped.items()):
        if bench_type == "startup":
            # Bar chart for startup time
            labels = [r["target"] for r in bench_results]
            values = [r.get("startup_ms", 0) for r in bench_results]
            bg_colors = [colors.get(t, "#6b7280") for t in labels]

            chart_divs += f"""
            <div class="chart-card">
                <h3>Startup Time</h3>
                <canvas id="chart{i}"></canvas>
            </div>"""
            charts_js += f"""
            new Chart(document.getElementById('chart{i}'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(labels)},
                    datasets: [{{
                        label: 'Startup (ms)',
                        data: {json.dumps(values)},
                        backgroundColor: {json.dumps(bg_colors)},
                    }}]
                }},
                options: {{
                    responsive: true,
                    plugins: {{ title: {{ display: true, text: 'Startup Time (ms) — Lower is Better' }} }}
                }}
            }});"""
        else:
            # Throughput chart
            labels = [r["target"] for r in bench_results]
            msgs = [r.get("throughput_msgs_sec", 0) for r in bench_results]
            bg_colors = [colors.get(t, "#6b7280") for t in labels]

            chart_divs += f"""
            <div class="chart-card">
                <h3>{bench_type.replace('_', ' ').title()} — Throughput</h3>
                <canvas id="chart{i}a"></canvas>
            </div>"""
            charts_js += f"""
            new Chart(document.getElementById('chart{i}a'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(labels)},
                    datasets: [{{
                        label: 'msgs/sec',
                        data: {json.dumps(msgs)},
                        backgroundColor: {json.dumps(bg_colors)},
                    }}]
                }},
                options: {{
                    responsive: true,
                    plugins: {{ title: {{ display: true, text: '{bench_type} Throughput (msgs/sec) — Higher is Better' }} }}
                }}
            }});"""

            # Latency chart
            p50s = [r.get("latency_p50_ms", 0) for r in bench_results]
            p99s = [r.get("latency_p99_ms", 0) for r in bench_results]

            chart_divs += f"""
            <div class="chart-card">
                <h3>{bench_type.replace('_', ' ').title()} — Latency</h3>
                <canvas id="chart{i}b"></canvas>
            </div>"""
            charts_js += f"""
            new Chart(document.getElementById('chart{i}b'), {{
                type: 'bar',
                data: {{
                    labels: {json.dumps(labels)},
                    datasets: [
                        {{ label: 'p50 (ms)', data: {json.dumps(p50s)}, backgroundColor: 'rgba(59,130,246,0.7)' }},
                        {{ label: 'p99 (ms)', data: {json.dumps(p99s)}, backgroundColor: 'rgba(239,68,68,0.7)' }}
                    ]
                }},
                options: {{
                    responsive: true,
                    plugins: {{ title: {{ display: true, text: '{bench_type} Latency (ms) — Lower is Better' }} }}
                }}
            }});"""

    # Results table
    table_rows = ""
    for r in results:
        table_rows += f"""<tr>
            <td><span class="badge badge-{r['target']}">{r['target']}</span></td>
            <td>{r['benchmark_type']}</td>
            <td>{r.get('throughput_msgs_sec', 0):,.0f}</td>
            <td>{r.get('throughput_mb_sec', 0):,.2f}</td>
            <td>{r.get('latency_p50_ms', 0):.2f}</td>
            <td>{r.get('latency_p99_ms', 0):.2f}</td>
            <td>{r.get('startup_ms', 0):,}</td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Streamline Benchmark Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0f172a; color: #e2e8f0; }}
  .header {{ background: linear-gradient(135deg, #1e293b, #334155); padding: 2rem; text-align: center; }}
  .header h1 {{ font-size: 2rem; margin-bottom: 0.5rem; }}
  .header .meta {{ color: #94a3b8; font-size: 0.875rem; }}
  .container {{ max-width: 1200px; margin: 0 auto; padding: 2rem; }}
  .chart-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(500px, 1fr)); gap: 1.5rem; margin: 2rem 0; }}
  .chart-card {{ background: #1e293b; border-radius: 12px; padding: 1.5rem; border: 1px solid #334155; }}
  .chart-card h3 {{ margin-bottom: 1rem; color: #f1f5f9; }}
  table {{ width: 100%; border-collapse: collapse; margin: 2rem 0; background: #1e293b; border-radius: 12px; overflow: hidden; }}
  th, td {{ padding: 0.75rem 1rem; text-align: left; border-bottom: 1px solid #334155; }}
  th {{ background: #334155; font-weight: 600; color: #f1f5f9; }}
  .badge {{ padding: 2px 8px; border-radius: 4px; font-size: 0.75rem; font-weight: 600; text-transform: uppercase; }}
  .badge-streamline {{ background: #10b981; color: #fff; }}
  .badge-kafka {{ background: #f59e0b; color: #000; }}
  .badge-redpanda {{ background: #ef4444; color: #fff; }}
  h2 {{ margin: 2rem 0 1rem; }}
</style>
</head>
<body>
<div class="header">
  <h1>⚡ Streamline Benchmark Dashboard</h1>
  <div class="meta">
    Version {metadata.get('version', 'unknown')} |
    Commit {metadata.get('commit', 'unknown')} |
    {metadata.get('timestamp', '')} |
    Platform: {metadata.get('platform', 'unknown')}
  </div>
</div>
<div class="container">
  <h2>Performance Charts</h2>
  <div class="chart-grid">{chart_divs}</div>

  <h2>Raw Results</h2>
  <table>
    <thead>
      <tr><th>System</th><th>Benchmark</th><th>msgs/sec</th><th>MB/sec</th><th>p50 (ms)</th><th>p99 (ms)</th><th>Startup (ms)</th></tr>
    </thead>
    <tbody>{table_rows}</tbody>
  </table>
</div>
<script>{charts_js}</script>
</body>
</html>"""

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        f.write(html)

    print(f"Dashboard generated: {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate benchmark dashboard")
    parser.add_argument("--input", required=True, help="Path to comparative-results.json")
    parser.add_argument("--output", required=True, help="Path for output HTML dashboard")
    args = parser.parse_args()
    generate_dashboard(args.input, args.output)
