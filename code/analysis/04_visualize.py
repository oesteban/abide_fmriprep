#!/usr/bin/env python3
"""Visualize classification results: comparison with Abraham et al. 2017.

Generates:
- Accuracy comparison table (CSV) vs Abraham Table 2
- Per-site accuracy bar charts (inter-site CV)
- Intra-site accuracy box plots
- Subject inclusion/exclusion flowchart summary

Usage::

    python code/analysis/04_visualize.py [--project-root .]

Outputs::

    derivatives/connectivity/figures/
        accuracy_comparison_table.csv
        intersite_accuracy_barplot_abide1_ridge.png
        intersite_accuracy_barplot_both_ridge.png
        intrasite_accuracy_boxplot_abide1_ridge.png
        intrasite_accuracy_boxplot_both_ridge.png
        inclusion_flowchart.csv
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from analysis._helpers import (
    CONNECTIVITY_DIR,
    PROJECT_ROOT,
    read_json,
)

logger = logging.getLogger(__name__)

# Abraham et al. 2017, Table 2: best pipeline (MSDL, tangent, l2-Ridge)
# Mean inter-site accuracy for ABIDE I
ABRAHAM_INTERSITE_ACCURACY = 0.673


def _load_classification_results(
    class_dir: Path,
) -> dict[str, dict]:
    """Load all classification JSON results from directory."""
    results = {}
    for f in sorted(class_dir.glob("results_*.json")):
        results[f.stem] = read_json(f)
    return results


def plot_intersite_barplot(
    results: dict,
    experiment: str,
    classifier: str,
    output_dir: Path,
) -> None:
    """Bar chart of per-site accuracy for inter-site CV."""
    key = f"results_intersite_{experiment}_{classifier}"
    if key not in results:
        logger.warning("No results for %s", key)
        return

    data = results[key]
    per_site = data["per_site_accuracy"]
    per_site_n = data.get("per_site_n_subjects", {})

    sites = sorted(per_site.keys())
    accuracies = [per_site[s] for s in sites]
    n_subjects = [per_site_n.get(s, 0) for s in sites]

    fig, ax = plt.subplots(figsize=(max(12, len(sites) * 0.5), 6))
    bars = ax.bar(range(len(sites)), accuracies, color="steelblue", alpha=0.8)

    # Add subject count labels
    for i, (bar, n) in enumerate(zip(bars, n_subjects)):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.01,
            str(n),
            ha="center",
            va="bottom",
            fontsize=7,
            color="gray",
        )

    ax.axhline(
        y=data["mean_accuracy_per_site"],
        color="red",
        linestyle="--",
        linewidth=1.5,
        label=f"Mean: {data['mean_accuracy_per_site']:.3f}",
    )
    ax.axhline(y=0.5, color="gray", linestyle=":", linewidth=1, label="Chance")

    if experiment == "abide1":
        ax.axhline(
            y=ABRAHAM_INTERSITE_ACCURACY,
            color="green",
            linestyle="--",
            linewidth=1.5,
            label=f"Abraham 2017: {ABRAHAM_INTERSITE_ACCURACY:.3f}",
        )

    ax.set_xticks(range(len(sites)))
    ax.set_xticklabels(sites, rotation=45, ha="right", fontsize=8)
    ax.set_ylabel("Accuracy")
    ax.set_title(
        f"Inter-site accuracy ({experiment}, {classifier})\n"
        f"N={data['n_subjects']}, {data['n_sites']} sites"
    )
    ax.set_ylim(0, 1.05)
    ax.legend(loc="lower right")
    fig.tight_layout()

    out_path = output_dir / f"intersite_accuracy_barplot_{experiment}_{classifier}.png"
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    logger.info("Wrote %s", out_path)


def plot_intrasite_boxplot(
    results: dict,
    experiment: str,
    classifier: str,
    output_dir: Path,
) -> None:
    """Box plot of intra-site accuracy distributions."""
    key = f"results_intrasite_{experiment}_{classifier}"
    if key not in results:
        logger.warning("No results for %s", key)
        return

    data = results[key]
    per_site = data["per_site_results"]

    if not per_site:
        logger.warning("No intra-site results for %s", key)
        return

    sites = sorted(per_site.keys())
    all_scores = [per_site[s]["all_scores"] for s in sites]
    n_subjects = [per_site[s]["n_subjects"] for s in sites]

    fig, ax = plt.subplots(figsize=(max(12, len(sites) * 0.5), 6))
    bp = ax.boxplot(
        all_scores,
        labels=[f"{s}\n(n={n})" for s, n in zip(sites, n_subjects)],
        patch_artist=True,
    )
    for patch in bp["boxes"]:
        patch.set_facecolor("lightsteelblue")

    ax.axhline(y=0.5, color="gray", linestyle=":", linewidth=1, label="Chance")
    ax.axhline(
        y=data["median_of_medians"],
        color="red",
        linestyle="--",
        linewidth=1.5,
        label=f"Median of medians: {data['median_of_medians']:.3f}",
    )

    ax.set_xticklabels(
        [f"{s}\n(n={n})" for s, n in zip(sites, n_subjects)],
        rotation=45,
        ha="right",
        fontsize=8,
    )
    ax.set_ylabel("Accuracy")
    ax.set_title(
        f"Intra-site accuracy ({experiment}, {classifier})\n"
        f"{data['n_sites_evaluated']}/{data['n_sites_total']} sites evaluated"
    )
    ax.set_ylim(0, 1.05)
    ax.legend(loc="lower right")
    fig.tight_layout()

    out_path = output_dir / f"intrasite_accuracy_boxplot_{experiment}_{classifier}.png"
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    logger.info("Wrote %s", out_path)


def build_comparison_table(
    results: dict,
    output_dir: Path,
) -> pd.DataFrame:
    """Build Table comparing our results with Abraham et al. 2017."""
    rows = []

    # Abraham reference
    rows.append({
        "study": "Abraham 2017",
        "dataset": "ABIDE I",
        "classifier": "Ridge",
        "cv_scheme": "LOSO",
        "metric": "mean_per_site",
        "accuracy": ABRAHAM_INTERSITE_ACCURACY,
        "n_subjects": 871,
        "n_sites": 20,
    })

    # Our results
    for key, data in results.items():
        parts = key.replace("results_", "").split("_")
        cv_type = parts[0]  # intersite or intrasite
        experiment = parts[1]  # abide1 or both
        classifier = "_".join(parts[2:]) if len(parts) > 2 else "ridge"

        dataset_label = {
            "abide1": "ABIDE I",
            "abide2": "ABIDE II",
            "both": "ABIDE I+II",
        }.get(experiment, experiment)

        clf_label = {"ridge": "Ridge", "svc_linear": "SVC (linear)"}.get(
            classifier, classifier
        )

        if cv_type == "intersite":
            rows.append({
                "study": "This study",
                "dataset": dataset_label,
                "classifier": clf_label,
                "cv_scheme": "LOSO",
                "metric": "mean_per_site",
                "accuracy": data.get("mean_accuracy_per_site", np.nan),
                "n_subjects": data.get("n_subjects", 0),
                "n_sites": data.get("n_sites", 0),
            })
        elif cv_type == "intrasite":
            rows.append({
                "study": "This study",
                "dataset": dataset_label,
                "classifier": clf_label,
                "cv_scheme": "Intra-site SSS",
                "metric": "median_of_medians",
                "accuracy": data.get("median_of_medians", np.nan),
                "n_subjects": data.get("n_subjects_total", 0),
                "n_sites": data.get("n_sites_evaluated", 0),
            })

    df = pd.DataFrame(rows)
    out_path = output_dir / "accuracy_comparison_table.csv"
    df.to_csv(out_path, index=False, float_format="%.3f")
    logger.info("Wrote %s", out_path)
    return df


def build_inclusion_summary(
    conn_dir: Path,
    output_dir: Path,
) -> None:
    """Write subject inclusion/exclusion summary."""
    report_path = conn_dir / "extraction_report.tsv"
    if not report_path.exists():
        logger.warning("No extraction report found at %s", report_path)
        return

    report = pd.read_csv(report_path, sep="\t")
    summary = report["status"].value_counts().to_dict()

    participants_path = conn_dir / "participants.tsv"
    if participants_path.exists():
        participants = pd.read_csv(participants_path, sep="\t")
        summary["total_in_participants_tsv"] = len(participants)
        summary["with_dx_group"] = int(participants["dx_group"].notna().sum())

    rows = [{"category": k, "count": v} for k, v in sorted(summary.items())]
    df = pd.DataFrame(rows)
    out_path = output_dir / "inclusion_flowchart.csv"
    df.to_csv(out_path, index=False)
    logger.info("Wrote %s", out_path)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--output-dir", type=Path, default=None)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    conn_dir = args.output_dir or (args.project_root / "derivatives" / "connectivity")
    class_dir = conn_dir / "classification"
    fig_dir = conn_dir / "figures"
    fig_dir.mkdir(parents=True, exist_ok=True)

    # Load all classification results
    results = _load_classification_results(class_dir)
    if not results:
        logger.error("No classification results found in %s", class_dir)
        return

    # Comparison table
    build_comparison_table(results, fig_dir)

    # Per-experiment, per-classifier plots
    for key in results:
        parts = key.replace("results_", "").split("_")
        cv_type = parts[0]
        experiment = parts[1]
        classifier = "_".join(parts[2:]) if len(parts) > 2 else "ridge"

        if cv_type == "intersite":
            plot_intersite_barplot(results, experiment, classifier, fig_dir)
        elif cv_type == "intrasite":
            plot_intrasite_boxplot(results, experiment, classifier, fig_dir)

    # Inclusion summary
    build_inclusion_summary(conn_dir, fig_dir)


if __name__ == "__main__":
    main()
