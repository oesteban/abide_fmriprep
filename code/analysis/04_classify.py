#!/usr/bin/env python3
"""Classification with tangent embedding and cross-validation.

Implements the Abraham et al. (2017) replication:
  - TangentEmbeddingTransformer (re-fits geometric mean per CV fold)
  - RidgeClassifier (primary) + SVC(kernel="linear")
  - Inter-site CV (LeaveOneGroupOut)
  - Intra-site CV (StratifiedShuffleSplit, 100 splits, 20% test)
  - Two experiments: ABIDE I only, ABIDE I+II combined

Usage::

    python code/analysis/04_classify.py --project-root .
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import RidgeClassifier
from sklearn.model_selection import LeaveOneGroupOut, StratifiedShuffleSplit
from sklearn.pipeline import Pipeline
from sklearn.svm import SVC


def _setup_path():
    here = Path(__file__).resolve().parent
    if str(here) not in sys.path:
        sys.path.insert(0, str(here))


_setup_path()

from _helpers import N_MSDL_REGIONS, RANDOM_STATE, TangentEmbeddingTransformer, derivatives_connectivity


# --------------------------------------------------------------------------- #
# Cross-validation runners
# --------------------------------------------------------------------------- #


def run_intersite_cv(
    timeseries: list[np.ndarray],
    labels: np.ndarray,
    sites: np.ndarray,
    classifier_name: str = "ridge",
) -> dict:
    """Leave-one-site-out cross-validation.

    Returns dict with per-site accuracy and unweighted mean.
    """
    clf = _make_classifier(classifier_name)
    logo = LeaveOneGroupOut()

    unique_sites = np.unique(sites)
    site_results = {}

    for train_idx, test_idx in logo.split(timeseries, labels, groups=sites):
        test_site = sites[test_idx[0]]
        X_train = [timeseries[i] for i in train_idx]
        X_test = [timeseries[i] for i in test_idx]
        y_train, y_test = labels[train_idx], labels[test_idx]

        pipe = Pipeline([
            ("tangent", TangentEmbeddingTransformer()),
            ("classifier", clf),
        ])
        pipe.fit(X_train, y_train)
        accuracy = pipe.score(X_test, y_test)

        site_results[test_site] = {
            "accuracy": round(float(accuracy), 6),
            "n_test": int(len(test_idx)),
            "n_asd": int((y_test == 1).sum()),
            "n_tc": int((y_test == 0).sum()),
        }

    accuracies = [v["accuracy"] for v in site_results.values()]
    return {
        "cv_scheme": "intersite_leave_one_site_out",
        "classifier": classifier_name,
        "n_sites": len(unique_sites),
        "n_subjects": len(labels),
        "mean_accuracy": round(float(np.mean(accuracies)), 6),
        "std_accuracy": round(float(np.std(accuracies)), 6),
        "per_site": site_results,
    }


def run_intrasite_cv(
    timeseries: list[np.ndarray],
    labels: np.ndarray,
    sites: np.ndarray,
    classifier_name: str = "ridge",
    n_splits: int = 100,
    test_size: float = 0.2,
    random_state: int = 42,
    min_subjects: int = 10,
    min_per_class: int = 5,
) -> dict:
    """Intra-site stratified shuffle split cross-validation.

    Returns per-site median accuracy.
    """
    unique_sites = np.unique(sites)
    site_results = {}

    for site in unique_sites:
        site_mask = sites == site
        y_site = labels[site_mask]

        # Check minimum requirements
        if len(y_site) < min_subjects:
            continue
        if (y_site == 1).sum() < min_per_class or (y_site == 0).sum() < min_per_class:
            continue

        ts_site = [timeseries[i] for i, m in enumerate(site_mask) if m]
        sss = StratifiedShuffleSplit(
            n_splits=n_splits, test_size=test_size, random_state=random_state
        )

        fold_accs = []
        for train_idx, test_idx in sss.split(ts_site, y_site):
            X_train = [ts_site[i] for i in train_idx]
            X_test = [ts_site[i] for i in test_idx]
            y_train, y_test = y_site[train_idx], y_site[test_idx]

            clf = _make_classifier(classifier_name)
            pipe = Pipeline([
                ("tangent", TangentEmbeddingTransformer()),
                ("classifier", clf),
            ])
            pipe.fit(X_train, y_train)
            fold_accs.append(pipe.score(X_test, y_test))

        site_results[site] = {
            "median_accuracy": round(float(np.median(fold_accs)), 6),
            "mean_accuracy": round(float(np.mean(fold_accs)), 6),
            "std_accuracy": round(float(np.std(fold_accs)), 6),
            "n_subjects": int(site_mask.sum()),
            "n_asd": int((y_site == 1).sum()),
            "n_tc": int((y_site == 0).sum()),
        }

    median_accs = [v["median_accuracy"] for v in site_results.values()]
    return {
        "cv_scheme": "intrasite_stratified_shuffle_split",
        "classifier": classifier_name,
        "n_splits": n_splits,
        "test_size": test_size,
        "random_state": random_state,
        "n_sites_evaluated": len(site_results),
        "n_sites_skipped": len(unique_sites) - len(site_results),
        "mean_of_medians": round(float(np.mean(median_accs)), 6) if median_accs else None,
        "per_site": site_results,
    }


def _make_classifier(name: str):
    if name == "ridge":
        return RidgeClassifier()
    elif name == "svc":
        return SVC(kernel="linear")
    else:
        raise ValueError(f"Unknown classifier: {name}")


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #


def classify(project_root: Path):
    """Run all classification experiments."""
    np.random.seed(RANDOM_STATE)
    conn_dir = derivatives_connectivity(project_root)
    qc_path = conn_dir / "qc_prescreen.tsv"

    qc_df = pd.read_csv(qc_path, sep="\t")
    qc_pass = qc_df[qc_df["excluded_reason"] == "pass"].copy()

    # Load time series
    print("Loading time series...")
    from _helpers import bep017_stem

    timeseries_all = []
    labels_all = []
    sites_all = []
    datasets_all = []
    subject_ids = []

    for _, row in qc_pass.iterrows():
        sub_id = row["participant_id"]
        run_label = row["selected_run"]
        stem = bep017_stem(sub_id, run_label)
        ts_path = (
            conn_dir / sub_id / "ses-1" / "func"
            / f"{stem}_stat-mean_timeseries.parquet"
        )
        if not ts_path.exists():
            continue
        ts = pd.read_parquet(ts_path).values
        if ts.shape[1] != N_MSDL_REGIONS:
            continue
        timeseries_all.append(ts)
        labels_all.append(1 if row["group"] == "ASD" else 0)
        # Use dataset-qualified site name to avoid collisions (e.g., UCLA_1
        # appears in both ABIDE I and II as distinct sites/scanners)
        sites_all.append(f"{row['source_dataset']}_{row['source_site']}")
        datasets_all.append(row["source_dataset"])
        subject_ids.append(sub_id)

    labels_all = np.array(labels_all)
    sites_all = np.array(sites_all)
    datasets_all = np.array(datasets_all)

    print(f"  Loaded {len(timeseries_all)} subjects "
          f"({(labels_all == 1).sum()} ASD, {(labels_all == 0).sum()} TC)")

    # Create classification output directory
    cls_dir = conn_dir / "classification"
    cls_dir.mkdir(parents=True, exist_ok=True)

    # --- Experiment 1: ABIDE I only ---
    abide1_mask = datasets_all == "abide1"
    ts_a1 = [timeseries_all[i] for i in range(len(timeseries_all)) if abide1_mask[i]]
    y_a1 = labels_all[abide1_mask]
    sites_a1 = sites_all[abide1_mask]

    print(f"\n=== Experiment 1: ABIDE I only (N={len(ts_a1)}) ===")

    for clf_name in ("ridge", "svc"):
        print(f"\n  Inter-site CV ({clf_name})...")
        result = run_intersite_cv(ts_a1, y_a1, sites_a1, clf_name)
        result["experiment"] = "abide1"
        result["timestamp"] = datetime.now(timezone.utc).isoformat()
        out_path = cls_dir / f"results_intersite_abide1_{clf_name}.json"
        with open(out_path, "w") as f:
            json.dump(result, f, indent=2)
        print(f"    Mean accuracy: {result['mean_accuracy']:.4f} "
              f"(+/- {result['std_accuracy']:.4f})")

        print(f"  Intra-site CV ({clf_name})...")
        result = run_intrasite_cv(ts_a1, y_a1, sites_a1, clf_name)
        result["experiment"] = "abide1"
        result["timestamp"] = datetime.now(timezone.utc).isoformat()
        out_path = cls_dir / f"results_intrasite_abide1_{clf_name}.json"
        with open(out_path, "w") as f:
            json.dump(result, f, indent=2)
        if result["mean_of_medians"] is not None:
            print(f"    Mean of medians: {result['mean_of_medians']:.4f}")

    # --- Experiment 2: ABIDE I + II combined ---
    print(f"\n=== Experiment 2: ABIDE I+II combined (N={len(timeseries_all)}) ===")

    for clf_name in ("ridge", "svc"):
        print(f"\n  Inter-site CV ({clf_name})...")
        result = run_intersite_cv(timeseries_all, labels_all, sites_all, clf_name)
        result["experiment"] = "both"
        result["timestamp"] = datetime.now(timezone.utc).isoformat()
        out_path = cls_dir / f"results_intersite_both_{clf_name}.json"
        with open(out_path, "w") as f:
            json.dump(result, f, indent=2)
        print(f"    Mean accuracy: {result['mean_accuracy']:.4f} "
              f"(+/- {result['std_accuracy']:.4f})")

        print(f"  Intra-site CV ({clf_name})...")
        result = run_intrasite_cv(timeseries_all, labels_all, sites_all, clf_name)
        result["experiment"] = "both"
        result["timestamp"] = datetime.now(timezone.utc).isoformat()
        out_path = cls_dir / f"results_intrasite_both_{clf_name}.json"
        with open(out_path, "w") as f:
            json.dump(result, f, indent=2)
        if result["mean_of_medians"] is not None:
            print(f"    Mean of medians: {result['mean_of_medians']:.4f}")

    print(f"\nAll results saved to {cls_dir}/")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=Path("."))
    args = parser.parse_args()
    classify(args.project_root.resolve())


if __name__ == "__main__":
    main()
