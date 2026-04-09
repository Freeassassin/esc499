#!/usr/bin/env python3
"""Generate benchmark comparison graphs from JSON result files.

Usage:
    python graph_results.py results_duckdb.json results_postgresql.json results_cedardb.json results_starrocks.json
    python graph_results.py --output-dir graphs/ results_*.json
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")  # Non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np


def load_results(paths: list[str]) -> list[dict]:
    results = []
    for p in paths:
        with open(p, encoding="utf-8") as f:
            data = json.load(f)
        data["_source"] = p
        results.append(data)
    return results


def extract_concurrency_vs_max_sf(data: dict) -> tuple[list[int], list[int | None]]:
    concurrencies = []
    max_sfs = []
    for entry in data.get("results", []):
        concurrencies.append(entry["concurrency"])
        max_sfs.append(entry["max_scale_factor"])
    return concurrencies, max_sfs


def plot_concurrency_vs_max_sf(all_data: list[dict], output_dir: Path) -> None:
    """Main graph: concurrency level (x) vs max scale factor (y) per engine."""
    fig, ax = plt.subplots(figsize=(12, 7))

    colors = {"duckdb": "#FFC107", "postgresql": "#336791", "cedardb": "#4CAF50", "starrocks": "#E91E63"}
    markers = {"duckdb": "o", "postgresql": "s", "cedardb": "D", "starrocks": "^"}

    for data in all_data:
        engine = data["engine"]
        concurrencies, max_sfs = extract_concurrency_vs_max_sf(data)
        if not concurrencies:
            continue

        # Replace None with 0 for plotting (bottleneck at SF=1)
        plot_sfs = [sf if sf is not None else 0 for sf in max_sfs]

        color = colors.get(engine, "#999999")
        marker = markers.get(engine, "o")

        ax.plot(concurrencies, plot_sfs, marker=marker, color=color, label=engine,
                linewidth=2, markersize=8, markeredgecolor="white", markeredgewidth=1)

        # Annotate each point
        for c, sf in zip(concurrencies, max_sfs):
            label_text = str(sf) if sf is not None else "✗"
            ax.annotate(label_text, (c, sf if sf else 0),
                       textcoords="offset points", xytext=(0, 12),
                       ha="center", fontsize=8, color=color, fontweight="bold")

    ax.set_xlabel("Concurrency Level (# users)", fontsize=12)
    ax.set_ylabel("Max Scale Factor (before bottleneck)", fontsize=12)
    ax.set_title("Concurrency vs Max Scale Factor — Fleet Distribution Compliance", fontsize=14)

    if all_data:
        all_c = []
        for d in all_data:
            c, _ = extract_concurrency_vs_max_sf(d)
            all_c.extend(c)
        if all_c:
            ax.set_xscale("log", base=2)
            ax.xaxis.set_major_formatter(ticker.ScalarFormatter())
            ax.xaxis.set_major_locator(ticker.FixedLocator(sorted(set(all_c))))

    ax.set_yscale("symlog", linthresh=1)
    ax.set_ylim(bottom=-0.5)
    ax.legend(fontsize=11, loc="upper right")
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    out = output_dir / "concurrency_vs_max_sf.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"Saved: {out}")


def plot_latency_distribution(all_data: list[dict], output_dir: Path) -> None:
    """Stacked bar chart of latency bucket distributions at SF=1, concurrency=1."""
    bucket_labels = [
        "(0s, 10ms]", "(10ms, 100ms]", "(100ms, 1s]", "(1s, 10s]",
        "(10s, 1min]", "(1min, 10min]", "(10min, 1h]", "(1h, 10h]", ">=10h",
    ]
    fleet_pcts = [13.7, 48.3, 24.9, 9.9, 2.2, 0.86, 0.08, 0.008, 9e-5]

    bucket_colors = [
        "#2ecc71", "#27ae60", "#f1c40f", "#e67e22",
        "#e74c3c", "#c0392b", "#8e44ad", "#2c3e50", "#1a1a2e",
    ]

    engines_with_data = []
    engine_bucket_pcts = []

    for data in all_data:
        engine = data["engine"]
        # Find SF=1, concurrency=1 run
        for entry in data.get("results", []):
            if entry["concurrency"] == 1:
                for run in entry.get("runs", []):
                    if run.get("scale_factor") == 1 and "bucket_distribution" in run:
                        pcts = [run["bucket_distribution"].get(b, {}).get("pct", 0) for b in bucket_labels]
                        engines_with_data.append(engine)
                        engine_bucket_pcts.append(pcts)
                        break
                break

    if not engines_with_data:
        print("No SF=1 concurrency=1 data found for latency distribution chart.")
        return

    # Add fleet as reference
    engines_with_data.append("Fleet\n(target)")
    engine_bucket_pcts.append(fleet_pcts)

    n_engines = len(engines_with_data)
    fig, ax = plt.subplots(figsize=(14, 7))

    x = np.arange(n_engines)
    width = 0.6
    bottom = np.zeros(n_engines)

    for i, (bucket, color) in enumerate(zip(bucket_labels, bucket_colors)):
        vals = [pcts[i] for pcts in engine_bucket_pcts]
        ax.bar(x, vals, width, bottom=bottom, label=bucket, color=color, edgecolor="white", linewidth=0.5)
        bottom += np.array(vals)

    ax.set_xlabel("Engine", fontsize=12)
    ax.set_ylabel("% of Queries", fontsize=12)
    ax.set_title("Query Latency Bucket Distribution (SF=1, Concurrency=1)", fontsize=14)
    ax.set_xticks(x)
    ax.set_xticklabels(engines_with_data, fontsize=11)
    ax.legend(bbox_to_anchor=(1.02, 1), loc="upper left", fontsize=9)
    ax.set_ylim(0, 105)
    ax.grid(True, axis="y", alpha=0.3)

    plt.tight_layout()
    out = output_dir / "latency_distribution_sf1.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {out}")


def plot_per_engine_detail(data: dict, output_dir: Path) -> None:
    """Individual engine: latency heatmap across (concurrency, scale_factor)."""
    engine = data["engine"]
    bucket_labels = [
        "(0s, 10ms]", "(10ms, 100ms]", "(100ms, 1s]", "(1s, 10s]",
        "(10s, 1min]", "(1min, 10min]", "(10min, 1h]", "(1h, 10h]", ">=10h",
    ]

    # Collect all run data
    rows = []  # (concurrency, sf, error_count, pcts_dict, bottleneck)
    for entry in data.get("results", []):
        c = entry["concurrency"]
        for run in entry.get("runs", []):
            sf = run.get("scale_factor", 0)
            bn = run.get("bottleneck", True)
            errors = run.get("error_count", 0)
            if "bucket_distribution" in run:
                pcts = {b: run["bucket_distribution"].get(b, {}).get("pct", 0) for b in bucket_labels}
                rows.append((c, sf, errors, pcts, bn))

    if not rows:
        return

    # Create a summary table figure
    fig, ax = plt.subplots(figsize=(12, max(3, len(rows) * 0.6 + 2)))
    ax.axis("off")

    headers = ["Concurrency", "SF", "Errors", "Bottleneck",
               "<1s%", "1-10s%", "10s-1m%", "1-10m%", ">10m%"]
    table_data = []
    cell_colors = []

    for c, sf, errors, pcts, bn in rows:
        lt_1s = pcts.get("(0s, 10ms]", 0) + pcts.get("(10ms, 100ms]", 0) + pcts.get("(100ms, 1s]", 0)
        b_1_10 = pcts.get("(1s, 10s]", 0)
        b_10_60 = pcts.get("(10s, 1min]", 0)
        b_60_600 = pcts.get("(1min, 10min]", 0)
        b_gt_600 = pcts.get("(10min, 1h]", 0) + pcts.get("(1h, 10h]", 0) + pcts.get(">=10h", 0)

        row = [
            str(c), str(sf), str(errors),
            "YES" if bn else "no",
            f"{lt_1s:.1f}", f"{b_1_10:.1f}", f"{b_10_60:.1f}", f"{b_60_600:.1f}", f"{b_gt_600:.1f}",
        ]
        table_data.append(row)

        bg = "#ffcccc" if bn else "#ccffcc"
        cell_colors.append([bg] * len(headers))

    table = ax.table(cellText=table_data, colLabels=headers, cellColours=cell_colors,
                     loc="center", cellLoc="center")
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1, 1.4)

    # Style header
    for j in range(len(headers)):
        table[0, j].set_facecolor("#333333")
        table[0, j].set_text_props(color="white", fontweight="bold")

    ax.set_title(f"{engine} — Run Details (green=pass, red=bottleneck)", fontsize=13, pad=20)

    plt.tight_layout()
    out = output_dir / f"detail_{engine}.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {out}")


def plot_error_summary(all_data: list[dict], output_dir: Path) -> None:
    """Bar chart of error counts per engine across all runs."""
    engines = []
    ok_counts = []
    error_counts = []

    for data in all_data:
        engine = data["engine"]
        total_ok = 0
        total_err = 0
        for entry in data.get("results", []):
            for run in entry.get("runs", []):
                total_q = run.get("total_queries", 0)
                errs = run.get("error_count", 0)
                total_ok += total_q - errs
                total_err += errs
        engines.append(engine)
        ok_counts.append(total_ok)
        error_counts.append(total_err)

    if not engines:
        return

    fig, ax = plt.subplots(figsize=(10, 5))
    x = np.arange(len(engines))
    width = 0.35

    ax.bar(x - width/2, ok_counts, width, label="Successful", color="#2ecc71")
    ax.bar(x + width/2, error_counts, width, label="Errors", color="#e74c3c")

    ax.set_xlabel("Engine", fontsize=12)
    ax.set_ylabel("Query Count", fontsize=12)
    ax.set_title("Total Queries: Successful vs Errors (all runs)", fontsize=14)
    ax.set_xticks(x)
    ax.set_xticklabels(engines, fontsize=11)
    ax.legend()
    ax.grid(True, axis="y", alpha=0.3)

    for i, (ok, err) in enumerate(zip(ok_counts, error_counts)):
        ax.annotate(f"{ok}", (i - width/2, ok), ha="center", va="bottom", fontsize=9)
        if err > 0:
            ax.annotate(f"{err}", (i + width/2, err), ha="center", va="bottom", fontsize=9, color="red")

    plt.tight_layout()
    out = output_dir / "error_summary.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"Saved: {out}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate benchmark graphs from JSON results")
    parser.add_argument("files", nargs="+", help="JSON result files (one per engine)")
    parser.add_argument("--output-dir", default="graphs", help="Directory for output images")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    all_data = load_results(args.files)
    print(f"Loaded {len(all_data)} result files: {[d['engine'] for d in all_data]}")

    # 1. Main comparison: concurrency vs max SF
    plot_concurrency_vs_max_sf(all_data, output_dir)

    # 2. Latency distribution at SF=1 concurrency=1
    plot_latency_distribution(all_data, output_dir)

    # 3. Per-engine detail tables
    for data in all_data:
        plot_per_engine_detail(data, output_dir)

    # 4. Error summary
    plot_error_summary(all_data, output_dir)

    print(f"\nAll graphs saved to {output_dir}/")


if __name__ == "__main__":
    main()
