#!/usr/bin/env python3
"""Visualization: comparison tables, accuracy charts, CONSORT flowchart.

Usage::

    python code/analysis/05_visualize.py --project-root .
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns


def _setup_path():
    here = Path(__file__).resolve().parent
    if str(here) not in sys.path:
        sys.path.insert(0, str(here))


_setup_path()

from _helpers import derivatives_connectivity


def load_classification_results(cls_dir: Path) -> dict:
    """Load all classification JSON results."""
    results = {}
    for fp in sorted(cls_dir.glob("results_*.json")):
        with open(fp) as f:
            results[fp.stem] = json.load(f)
    return results


def plot_intersite_comparison(results: dict, fig_dir: Path):
    """Bar chart comparing ABIDE I vs Abraham Table 2."""
    # Abraham Table 2 reference values (subsample #1: all subjects, MSDL+tangent)
    abraham_ref = {
        "inter_accuracy": 0.668,
        "inter_specificity": 0.723,
        "inter_sensitivity": 0.610,
        "intra_accuracy": 0.669,
    }

    fig, ax = plt.subplots(figsize=(8, 5))

    # Our ABIDE I results
    for key, res in results.items():
        if "intersite_abide1_ridge" in key:
            our_inter = res["mean_accuracy"]
            our_inter_std = res["std_accuracy"]
            break
    else:
        print("WARNING: No ABIDE I inter-site ridge results found", flush=True)
        return

    labels = ["Abraham et al.\n(C-PAC, N=871)", "This study\n(fMRIPrep, ABIDE I)"]
    values = [abraham_ref["inter_accuracy"], our_inter]
    errors = [0.054, our_inter_std]  # Abraham's SD from Table 2

    bars = ax.bar(labels, values, yerr=errors, capsize=5,
                  color=["#7fbfbf", "#bf7fbf"], edgecolor="black", linewidth=0.5)
    ax.set_ylabel("Inter-site accuracy (unweighted mean)")
    ax.set_ylim(0.4, 0.8)
    ax.axhline(0.537, color="gray", linestyle="--", linewidth=0.8, label="Chance (53.7%)")
    ax.legend()
    ax.set_title("Replication: MSDL + Tangent + RidgeClassifier")

    for bar, val in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.01,
                f"{val:.1%}", ha="center", va="bottom", fontsize=10)

    fig.tight_layout()
    fig.savefig(fig_dir / "intersite_comparison.png", dpi=150)
    fig.savefig(fig_dir / "intersite_comparison.svg")
    plt.close(fig)
    print(f"  Saved intersite_comparison.{{png,svg}}", flush=True)


def plot_persite_accuracy(results: dict, fig_dir: Path):
    """Per-site accuracy bar chart for inter-site CV."""
    for experiment in ("abide1", "both"):
        key = f"results_intersite_{experiment}_ridge"
        if key not in results:
            continue
        res = results[key]
        per_site = res["per_site"]

        sites = sorted(per_site.keys())
        accs = [per_site[s]["accuracy"] for s in sites]
        n_test = [per_site[s]["n_test"] for s in sites]

        fig, ax = plt.subplots(figsize=(max(12, len(sites) * 0.4), 5))
        colors = ["#4c9ed9" if "v1s" in s or experiment == "abide1"
                   else "#d94c4c" for s in sites]
        # Actually color by site name not prefix since per_site keys are site names
        ax.bar(range(len(sites)), accs, color="#4c9ed9", edgecolor="black",
               linewidth=0.3)
        ax.set_xticks(range(len(sites)))
        ax.set_xticklabels(sites, rotation=90, fontsize=7)
        ax.set_ylabel("Accuracy")
        ax.set_xlabel("Site")
        ax.axhline(res["mean_accuracy"], color="red", linestyle="--",
                    label=f"Mean: {res['mean_accuracy']:.1%}")
        ax.axhline(0.5, color="gray", linestyle=":", label="Chance")
        ax.set_ylim(0, 1)
        ax.legend()
        label = "ABIDE I" if experiment == "abide1" else "ABIDE I+II"
        ax.set_title(f"Per-site accuracy (inter-site CV, Ridge) -- {label}")

        fig.tight_layout()
        fig.savefig(fig_dir / f"persite_accuracy_{experiment}.png", dpi=150)
        fig.savefig(fig_dir / f"persite_accuracy_{experiment}.svg")
        plt.close(fig)
        print(f"  Saved persite_accuracy_{experiment}.{{png,svg}}", flush=True)


def plot_intrasite_boxplot(results: dict, fig_dir: Path):
    """Intra-site accuracy box plots."""
    for experiment in ("abide1", "both"):
        key = f"results_intrasite_{experiment}_ridge"
        if key not in results:
            continue
        res = results[key]
        per_site = res["per_site"]

        sites = sorted(per_site.keys())
        medians = [per_site[s]["median_accuracy"] for s in sites]
        means = [per_site[s]["mean_accuracy"] for s in sites]
        stds = [per_site[s]["std_accuracy"] for s in sites]

        fig, ax = plt.subplots(figsize=(max(10, len(sites) * 0.4), 5))
        ax.bar(range(len(sites)), medians, yerr=stds, capsize=3,
               color="#7fbf7f", edgecolor="black", linewidth=0.3)
        ax.set_xticks(range(len(sites)))
        ax.set_xticklabels(sites, rotation=90, fontsize=7)
        ax.set_ylabel("Median accuracy (100 splits)")
        ax.set_xlabel("Site")
        if res["mean_of_medians"] is not None:
            ax.axhline(res["mean_of_medians"], color="red", linestyle="--",
                        label=f"Mean: {res['mean_of_medians']:.1%}")
        ax.axhline(0.5, color="gray", linestyle=":", label="Chance")
        ax.set_ylim(0, 1)
        ax.legend()
        label = "ABIDE I" if experiment == "abide1" else "ABIDE I+II"
        ax.set_title(f"Intra-site accuracy (100 shuffle splits, Ridge) -- {label}")

        fig.tight_layout()
        fig.savefig(fig_dir / f"intrasite_accuracy_{experiment}.png", dpi=150)
        fig.savefig(fig_dir / f"intrasite_accuracy_{experiment}.svg")
        plt.close(fig)
        print(f"  Saved intrasite_accuracy_{experiment}.{{png,svg}}", flush=True)


def write_comparison_table(results: dict, fig_dir: Path):
    """Write CSV comparing our ABIDE I results with Abraham Table 2."""
    # Abraham Table 2 reference (subsample #1, MSDL+tangent+ridge)
    abraham = {
        "Source": "Abraham et al. 2017",
        "Sample": "ABIDE I (N=871)",
        "Pipeline": "C-PAC",
        "Inter-site Accuracy": "66.8%",
        "Inter-site Specificity": "72.3%",
        "Inter-site Sensitivity": "61.0%",
        "Intra-site Accuracy": "66.9%",
    }

    our_inter = results.get("results_intersite_abide1_ridge", {})
    our_intra = results.get("results_intrasite_abide1_ridge", {})

    ours = {
        "Source": "This study",
        "Sample": f"ABIDE I (N={our_inter.get('n_subjects', '?')})",
        "Pipeline": "fMRIPrep 25.2.4",
        "Inter-site Accuracy": f"{our_inter.get('mean_accuracy', 0):.1%}",
        "Inter-site Specificity": "TBD",
        "Inter-site Sensitivity": "TBD",
        "Intra-site Accuracy": f"{our_intra.get('mean_of_medians', 0):.1%}"
        if our_intra.get("mean_of_medians") else "TBD",
    }

    df = pd.DataFrame([abraham, ours])
    csv_path = fig_dir / "accuracy_comparison_table.csv"
    df.to_csv(csv_path, index=False)
    print(f"  Saved accuracy_comparison_table.csv", flush=True)

    # Also print to stdout
    print("\n  Comparison with Abraham et al. (2017):", flush=True)
    print(df.to_string(index=False), flush=True)


def write_consort_flowchart(project_root: Path, fig_dir: Path):
    """Write a text-based CONSORT subject inclusion/exclusion flowchart."""
    conn_dir = derivatives_connectivity(project_root)
    qc_path = conn_dir / "qc_prescreen.tsv"

    if not qc_path.exists():
        print("  WARNING: qc_prescreen.tsv not found, skipping CONSORT flowchart", flush=True)
        return

    qc_df = pd.read_csv(qc_path, sep="\t")

    n_total = 2194
    n_preproc_excl = 46
    n_no_dx = 181 - 31  # 181 total, 31 overlap with preprocessing exclusions
    n_screened = len(qc_df)
    n_pass = (qc_df["excluded_reason"] == "pass").sum()
    n_no_output = (qc_df["excluded_reason"] == "no_fmriprep_output").sum()
    n_high_fd = qc_df["excluded_reason"].str.contains("high_fd", na=False).sum()
    n_low_vol = qc_df["excluded_reason"].str.contains("low_volumes", na=False).sum()

    n_abide1 = (qc_df[(qc_df["excluded_reason"] == "pass") & (qc_df["source_dataset"] == "abide1")]).shape[0]
    n_abide2 = (qc_df[(qc_df["excluded_reason"] == "pass") & (qc_df["source_dataset"] == "abide2")]).shape[0]

    flowchart = f"""CONSORT-style Subject Inclusion Flowchart
==========================================

ABIDE I + II Total:                  {n_total:>5}

  Excluded (preprocessing failures):  {n_preproc_excl:>5}
    - No T1w:                          10
    - No BOLD:                         33
    - FreeSurfer crash:                 3

  Excluded (no diagnostic group):     {n_no_dx:>5}
    - GU_1, ETHZ_1, UCLA_Long, UPSM_Long

Entered QC screening:                {n_screened:>5}

  Excluded (QC):
    - No fMRIPrep output:             {n_no_output:>5}
    - Mean FD > 0.5 mm:               {n_high_fd:>5}
    - <120 usable volumes:            {n_low_vol:>5}

Included in analysis:                {n_pass:>5}
  - ABIDE I:                         {n_abide1:>5}
  - ABIDE II:                        {n_abide2:>5}
"""

    flowchart_path = fig_dir / "consort_flowchart.txt"
    with open(flowchart_path, "w") as f:
        f.write(flowchart)
    print(f"  Saved consort_flowchart.txt", flush=True)
    print(flowchart, flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=Path("."))
    args = parser.parse_args()
    root = args.project_root.resolve()

    conn_dir = derivatives_connectivity(root)
    cls_dir = conn_dir / "classification"
    fig_dir = conn_dir / "figures"
    fig_dir.mkdir(parents=True, exist_ok=True)

    sns.set_theme(style="whitegrid", font_scale=1.1)

    # Load results
    results = load_classification_results(cls_dir)
    if not results:
        print("ERROR: No classification results found. Run 04_classify.py first.", flush=True)
        sys.exit(1)

    print("Generating visualizations...", flush=True)

    # 1. Comparison table
    write_comparison_table(results, fig_dir)

    # 2. Inter-site comparison with Abraham
    plot_intersite_comparison(results, fig_dir)

    # 3. Per-site accuracy bars
    plot_persite_accuracy(results, fig_dir)

    # 4. Intra-site box plots
    plot_intrasite_boxplot(results, fig_dir)

    # 5. CONSORT flowchart
    write_consort_flowchart(root, fig_dir)

    print(f"\nAll figures saved to {fig_dir}/", flush=True)


if __name__ == "__main__":
    main()
