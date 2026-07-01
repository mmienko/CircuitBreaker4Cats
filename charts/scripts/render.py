#!/usr/bin/env python3
"""Render AdaptiveRateLimiter behavior charts from CSV timeseries.

Pipeline:
    sbt "charts/run"                 # writes docs/charts/data/*.csv + manifest.csv
    pip install -r requirements.txt
    python charts/scripts/render.py  # writes docs/images/adaptive-rate-limiter/*.png

This script is data-driven: it reads manifest.csv and renders one PNG per listed scenario, so adding scenarios on
the Scala side requires no changes here.
"""

import argparse
import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # headless: render straight to files
import matplotlib.pyplot as plt

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_DIR = REPO_ROOT / "docs" / "charts" / "data"
DEFAULT_OUT_DIR = REPO_ROOT / "docs" / "images" / "adaptive-rate-limiter"


def read_rows(path):
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def load_samples(path):
    elapsed, aimd, admitted, capacity, observed, slow_start = [], [], [], [], [], []
    for row in read_rows(path):
        elapsed.append(float(row["elapsed_ms"]) / 1000.0)  # seconds
        aimd.append(float(row["aimd_rps"]))
        admitted.append(float(row["admitted_rps"]))
        capacity.append(float(row["backend_capacity_rps"]))
        observed.append(float(row["observed_failure_ratio"]))
        # Optional column: absent in older data, "1"/"0" otherwise.
        slow_start.append(row.get("slow_start_active", "0") == "1")
    return elapsed, aimd, admitted, capacity, observed, slow_start


def slow_start_spans(elapsed, slow_start):
    """Contiguous (start, end) time ranges where the limiter was in the slow-start (insufficient-data) regime."""
    spans = []
    start = None
    for t, active in zip(elapsed, slow_start):
        if active and start is None:
            start = t
        elif not active and start is not None:
            spans.append((start, t))
            start = None
    if start is not None:
        spans.append((start, elapsed[-1]))
    return spans


def render_scenario(entry, data_dir, out_dir):
    elapsed, aimd, admitted, capacity, observed, slow_start = load_samples(data_dir / entry["samples_file"])

    fig, ax_rate = plt.subplots(figsize=(11, 5))

    # Shade the spans where the measurement window was starved and the limiter ran blind on slow-start.
    slow_start_handle = None
    for start, end in slow_start_spans(elapsed, slow_start):
        slow_start_handle = ax_rate.axvspan(
            start, end, color="#f1c40f", alpha=0.15, label="slow-start (insufficient data)"
        )

    # Left axis: rates (requests / second). The AIMD estimate is the controlled variable; the dashed line is the
    # backend's true capacity (the bottleneck the AIMD is discovering); admitted is the measured throughput.
    capacity_line, = ax_rate.plot(
        elapsed, capacity, color="#7f8c8d", linewidth=1.5, linestyle="--", label="backend capacity"
    )
    admitted_line, = ax_rate.plot(
        elapsed, admitted, color="#16a085", linewidth=1.0, alpha=0.6, label="admitted (measured)"
    )
    rate_line, = ax_rate.step(
        elapsed, aimd, where="post", color="#2c3e50", linewidth=2.0, label="AIMD rate estimate"
    )
    ax_rate.set_xlabel("time (s)")
    ax_rate.set_ylabel("rate (requests / second)")
    ax_rate.set_ylim(bottom=0)
    ax_rate.grid(True, alpha=0.3)

    # Right axis: the limiter's sampled failure ratio in [0, 1].
    ax_ratio = ax_rate.twinx()
    observed_line, = ax_ratio.plot(
        elapsed, observed, color="#2980b9", linewidth=1.5, alpha=0.85, label="failure ratio (observed)"
    )
    ax_ratio.set_ylabel("failure ratio")
    ax_ratio.set_ylim(-0.02, 1.02)

    handles = [rate_line, admitted_line, capacity_line, observed_line]
    if slow_start_handle is not None:
        handles.append(slow_start_handle)
    # Legend in a single horizontal row above the plot so it never covers the curves.
    ax_rate.legend(
        handles=handles, loc="lower center", bbox_to_anchor=(0.5, 1.02), ncol=len(handles), frameon=False, fontsize=9
    )

    fig.suptitle(entry["scenario"], fontsize=14, fontweight="bold")
    ax_rate.set_title(entry["description"], fontsize=10, color="#555555", pad=30)
    fig.tight_layout()

    out_path = out_dir / f"{entry['scenario']}.png"
    fig.savefig(out_path, dpi=130, bbox_inches="tight")
    plt.close(fig)
    return out_path


def main():
    parser = argparse.ArgumentParser(description="Render AdaptiveRateLimiter charts from CSV timeseries.")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR, help="directory containing manifest.csv")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR, help="directory to write PNGs into")
    args = parser.parse_args()

    manifest_path = args.data_dir / "manifest.csv"
    if not manifest_path.exists():
        raise SystemExit(f"manifest not found: {manifest_path} (run `sbt \"charts/run\"` first)")

    args.out_dir.mkdir(parents=True, exist_ok=True)

    for entry in read_rows(manifest_path):
        out_path = render_scenario(entry, args.data_dir, args.out_dir)
        print(f"wrote {out_path}")


if __name__ == "__main__":
    main()
