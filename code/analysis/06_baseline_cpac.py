#!/usr/bin/env python3
"""C-PAC baseline classification (reads pre-extracted parquets).

Reads MSDL time series from connectivity-cpac/ (written by
extract_subject.py --source cpac) and runs tangent + Ridge/SVC
classification with LOGO and intra-site CV.

Usage::

    python code/analysis/06_baseline_cpac.py --project-root .
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from nilearn.datasets import fetch_abide_pcp
from sklearn.linear_model import RidgeClassifier
from sklearn.model_selection import LeaveOneGroupOut, StratifiedShuffleSplit
from sklearn.pipeline import Pipeline
from sklearn.svm import SVC


def _setup_path():
    here = Path(__file__).resolve().parent
    if str(here) not in sys.path:
        sys.path.insert(0, str(here))


_setup_path()

from _helpers import (
    N_MSDL_REGIONS,
    RANDOM_STATE,
    TangentEmbeddingTransformer,
    derivatives_connectivity,
)


def load_cpac_timeseries(project_root: Path, data_dir: str | None = None):
    """Load pre-extracted C-PAC parquets + phenotypic data."""
    conn_dir = derivatives_connectivity(project_root, variant="cpac")

    # Get phenotypic info from PCP
    print("Fetching PCP phenotypic data...", flush=True)
    abide = fetch_abide_pcp(data_dir=data_dir, pipeline="cpac", band_pass_filtering=True,
                            global_signal_regression=False, derivatives=["func_preproc"],
                            quality_checked=True, verbose=0)
    phenotypic = abide.phenotypic

    timeseries = []
    labels = []
    sites = []
    subject_ids = []

    for i in range(len(phenotypic)):
        dx = int(phenotypic["DX_GROUP"].iloc[i])
        site = str(phenotypic["SITE_ID"].iloc[i])
        sub_id = str(int(phenotypic["SUB_ID"].iloc[i])).zfill(7)
        sub_label = f"sub-{sub_id}"

        if dx not in (1, 2):
            continue

        func_dir = conn_dir / sub_label / "ses-1" / "func"
        parquets = list(func_dir.glob("*_timeseries.parquet")) if func_dir.is_dir() else []
        if not parquets:
            continue

        ts = pd.read_parquet(parquets[0]).values
        if ts.shape[1] != N_MSDL_REGIONS:
            continue

        timeseries.append(ts)
        labels.append(1 if dx == 1 else 0)
        sites.append(site)
        subject_ids.append(sub_id)

    print(f"  Loaded {len(timeseries)} subjects from parquets", flush=True)
    return timeseries, np.array(labels), np.array(sites), subject_ids


def run_intersite_cv(timeseries, labels, sites, classifier_name="ridge"):
    clf = RidgeClassifier() if classifier_name == "ridge" else SVC(kernel="linear")
    logo = LeaveOneGroupOut()
    site_results = {}
    for train_idx, test_idx in logo.split(timeseries, labels, groups=sites):
        test_site = sites[test_idx[0]]
        pipe = Pipeline([("tangent", TangentEmbeddingTransformer()), ("classifier", clf)])
        pipe.fit([timeseries[i] for i in train_idx], labels[train_idx])
        accuracy = pipe.score([timeseries[i] for i in test_idx], labels[test_idx])
        site_results[test_site] = {
            "accuracy": round(float(accuracy), 6),
            "n_test": int(len(test_idx)),
            "n_asd": int((labels[test_idx] == 1).sum()),
            "n_tc": int((labels[test_idx] == 0).sum()),
        }
    accuracies = [v["accuracy"] for v in site_results.values()]
    return {
        "cv_scheme": "intersite_leave_one_site_out",
        "classifier": classifier_name,
        "n_sites": len(set(sites)), "n_subjects": len(labels),
        "mean_accuracy": round(float(np.mean(accuracies)), 6),
        "std_accuracy": round(float(np.std(accuracies)), 6),
        "per_site": site_results,
    }


def run_intrasite_cv(timeseries, labels, sites, classifier_name="ridge"):
    unique_sites = np.unique(sites)
    site_results = {}
    for site in unique_sites:
        mask = sites == site
        y = labels[mask]
        if len(y) < 10 or (y == 1).sum() < 5 or (y == 0).sum() < 5:
            continue
        ts_site = [timeseries[i] for i, m in enumerate(mask) if m]
        sss = StratifiedShuffleSplit(n_splits=100, test_size=0.2, random_state=RANDOM_STATE)
        accs = []
        for tr_i, te_i in sss.split(ts_site, y):
            clf = RidgeClassifier() if classifier_name == "ridge" else SVC(kernel="linear")
            pipe = Pipeline([("tangent", TangentEmbeddingTransformer()), ("classifier", clf)])
            pipe.fit([ts_site[i] for i in tr_i], y[tr_i])
            accs.append(pipe.score([ts_site[i] for i in te_i], y[te_i]))
        site_results[site] = {
            "median_accuracy": round(float(np.median(accs)), 6),
            "mean_accuracy": round(float(np.mean(accs)), 6),
            "std_accuracy": round(float(np.std(accs)), 6),
            "n_subjects": int(mask.sum()),
        }
    medians = [v["median_accuracy"] for v in site_results.values()]
    return {
        "cv_scheme": "intrasite_stratified_shuffle_split",
        "classifier": classifier_name,
        "n_sites_evaluated": len(site_results),
        "mean_of_medians": round(float(np.mean(medians)), 6) if medians else None,
        "per_site": site_results,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=Path("."))
    parser.add_argument("--data-dir", type=str, default=None)
    args = parser.parse_args()
    root = args.project_root.resolve()
    np.random.seed(RANDOM_STATE)

    timeseries, labels, sites, _ = load_cpac_timeseries(root, args.data_dir)
    print(f"\n=== C-PAC Baseline (N={len(timeseries)}) ===", flush=True)
    print(f"  ASD: {(labels == 1).sum()}, TC: {(labels == 0).sum()}, Sites: {len(np.unique(sites))}", flush=True)

    cls_dir = derivatives_connectivity(root, variant="cpac") / "classification"
    cls_dir.mkdir(parents=True, exist_ok=True)

    for clf_name in ("ridge", "svc"):
        print(f"\n  Inter-site ({clf_name})...", flush=True)
        r = run_intersite_cv(timeseries, labels, sites, clf_name)
        r["experiment"] = "cpac_baseline"
        r["timestamp"] = datetime.now(timezone.utc).isoformat()
        with open(cls_dir / f"results_intersite_cpac_{clf_name}.json", "w") as f:
            json.dump(r, f, indent=2)
        print(f"    {r['mean_accuracy']:.1%} (+/- {r['std_accuracy']:.1%})", flush=True)

        print(f"  Intra-site ({clf_name})...", flush=True)
        r = run_intrasite_cv(timeseries, labels, sites, clf_name)
        r["experiment"] = "cpac_baseline"
        r["timestamp"] = datetime.now(timezone.utc).isoformat()
        with open(cls_dir / f"results_intrasite_cpac_{clf_name}.json", "w") as f:
            json.dump(r, f, indent=2)
        if r["mean_of_medians"]:
            print(f"    Mean of medians: {r['mean_of_medians']:.1%}", flush=True)


if __name__ == "__main__":
    main()
