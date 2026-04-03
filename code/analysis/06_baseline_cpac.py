#!/usr/bin/env python3
"""Baseline: run the same classification on C-PAC preprocessed ABIDE I.

Downloads the Preprocessed Connectomes Project (PCP) C-PAC data via
nilearn's fetch_abide_pcp(), extracts MSDL time series, and runs the
same tangent + RidgeClassifier / SVC pipeline as 04_classify.py.

This isolates whether the accuracy gap vs Abraham et al. (2017) is due
to preprocessing (fMRIPrep vs C-PAC) or analysis code differences.

Usage::

    python code/analysis/06_baseline_cpac.py --project-root . [--data-dir /path/to/cache]
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
from nilearn.datasets import fetch_abide_pcp, fetch_atlas_msdl
from nilearn.maskers import NiftiMapsMasker
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


def extract_cpac_timeseries(data_dir: str | None = None):
    """Fetch C-PAC ABIDE I data and extract MSDL time series.

    Uses the same parameters as Abraham: band_pass_filtering=True
    (C-PAC already applied 0.01-0.1 Hz), quality_checked=True.

    If data is already cached, loads directly from disk to avoid
    nilearn's slow re-verification of all 871 files.
    """
    import glob
    import pandas as pd

    cache_dir = Path(data_dir or Path.home() / "nilearn_data") / "ABIDE_pcp" / "cpac" / "filt_noglobal"
    phenotypic_path = Path(data_dir or Path.home() / "nilearn_data") / "ABIDE_pcp" / "Phenotypic_V1_0b_preprocessed1.csv"

    # Always use fetch_abide_pcp to ensure all subjects are downloaded.
    # This verifies cached files and fetches any missing ones.
    print("Fetching ABIDE PCP (C-PAC, func_preproc)...", flush=True)
    print("  (This may take a while on first run or if cache is incomplete)", flush=True)
    abide = fetch_abide_pcp(
        data_dir=data_dir,
        pipeline="cpac",
        band_pass_filtering=True,
        global_signal_regression=False,
        derivatives=["func_preproc"],
        quality_checked=True,
        verbose=1,
    )
    func_files = abide.func_preproc
    phenotypic = abide.phenotypic
    print(f"  Total subjects: {len(func_files)}", flush=True)

    atlas = fetch_atlas_msdl()
    print(f"  MSDL atlas loaded ({len(atlas.labels)} regions)", flush=True)

    # Extract time series -- C-PAC data is already denoised, so no confounds needed
    # Abraham used standardize=True, detrend=True, low_pass/high_pass already applied by C-PAC
    masker = NiftiMapsMasker(
        maps_img=atlas.maps,
        standardize="zscore_sample",
        detrend=True,
        low_pass=None,   # already band-pass filtered by C-PAC
        high_pass=None,
    )

    timeseries_list = []
    labels = []
    sites = []
    subject_ids = []
    skipped = 0

    print(f"  Extracting time series from {len(func_files)} subjects...", flush=True)
    for i, func in enumerate(func_files):
        dx = int(phenotypic["DX_GROUP"].iloc[i])
        site = str(phenotypic["SITE_ID"].iloc[i])
        sub_id = str(phenotypic["SUB_ID"].iloc[i])

        if dx not in (1, 2):
            skipped += 1
            continue

        try:
            ts = masker.fit_transform(func)
            if ts.shape[1] != N_MSDL_REGIONS:
                skipped += 1
                continue
            timeseries_list.append(ts)
            labels.append(1 if dx == 1 else 0)  # 1=ASD, 2=TC -> 1=ASD, 0=TC
            sites.append(site)
            subject_ids.append(sub_id)
        except Exception as e:
            print(f"  WARNING: subject {sub_id} failed: {e}", flush=True)
            skipped += 1

        if (i + 1) % 50 == 0:
            print(f"  Processed {i + 1}/{len(func_files)} "
                  f"(extracted: {len(timeseries_list)}, skipped: {skipped})", flush=True)

    print(f"  Extracted: {len(timeseries_list)}, skipped: {skipped}", flush=True)
    return timeseries_list, np.array(labels), np.array(sites), subject_ids


def run_intersite_cv(timeseries, labels, sites, classifier_name="ridge"):
    clf = RidgeClassifier() if classifier_name == "ridge" else SVC(kernel="linear")
    logo = LeaveOneGroupOut()
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
        "n_sites": len(set(sites)),
        "n_subjects": len(labels),
        "mean_accuracy": round(float(np.mean(accuracies)), 6),
        "std_accuracy": round(float(np.std(accuracies)), 6),
        "per_site": site_results,
    }


def run_intrasite_cv(timeseries, labels, sites, classifier_name="ridge",
                     n_splits=100, test_size=0.2, random_state=42,
                     min_subjects=10, min_per_class=5):
    unique_sites = np.unique(sites)
    site_results = {}

    for site in unique_sites:
        site_mask = sites == site
        y_site = labels[site_mask]
        if len(y_site) < min_subjects:
            continue
        if (y_site == 1).sum() < min_per_class or (y_site == 0).sum() < min_per_class:
            continue

        ts_site = [timeseries[i] for i, m in enumerate(site_mask) if m]
        sss = StratifiedShuffleSplit(n_splits=n_splits, test_size=test_size, random_state=random_state)
        fold_accs = []

        for train_idx, test_idx in sss.split(ts_site, y_site):
            X_train = [ts_site[i] for i in train_idx]
            X_test = [ts_site[i] for i in test_idx]
            y_train, y_test = y_site[train_idx], y_site[test_idx]
            clf = RidgeClassifier() if classifier_name == "ridge" else SVC(kernel="linear")
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
        "n_sites_evaluated": len(site_results),
        "mean_of_medians": round(float(np.mean(median_accs)), 6) if median_accs else None,
        "per_site": site_results,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=Path("."))
    parser.add_argument("--data-dir", type=str, default=None,
                        help="Cache directory for PCP downloads (default: ~/nilearn_data)")
    args = parser.parse_args()
    root = args.project_root.resolve()

    np.random.seed(RANDOM_STATE)

    # Extract time series from C-PAC data
    timeseries, labels, sites, subject_ids = extract_cpac_timeseries(args.data_dir)

    print(f"\n=== Baseline: C-PAC preprocessed ABIDE I (N={len(timeseries)}) ===", flush=True)
    print(f"  ASD: {(labels == 1).sum()}, TC: {(labels == 0).sum()}", flush=True)
    print(f"  Sites: {len(np.unique(sites))}", flush=True)

    cls_dir = derivatives_connectivity(root) / "classification"
    cls_dir.mkdir(parents=True, exist_ok=True)

    for clf_name in ("ridge", "svc"):
        print(f"\n  Inter-site CV ({clf_name})...", flush=True)
        result = run_intersite_cv(timeseries, labels, sites, clf_name)
        result["experiment"] = "cpac_baseline"
        result["timestamp"] = datetime.now(timezone.utc).isoformat()
        with open(cls_dir / f"results_intersite_cpac_{clf_name}.json", "w") as f:
            json.dump(result, f, indent=2)
        print(f"    Mean accuracy: {result['mean_accuracy']:.4f} "
              f"(+/- {result['std_accuracy']:.4f})", flush=True)

        print(f"  Intra-site CV ({clf_name})...", flush=True)
        result = run_intrasite_cv(timeseries, labels, sites, clf_name)
        result["experiment"] = "cpac_baseline"
        result["timestamp"] = datetime.now(timezone.utc).isoformat()
        with open(cls_dir / f"results_intrasite_cpac_{clf_name}.json", "w") as f:
            json.dump(result, f, indent=2)
        if result["mean_of_medians"] is not None:
            print(f"    Mean of medians: {result['mean_of_medians']:.4f}", flush=True)

    print(f"\nResults saved to {cls_dir}/", flush=True)


if __name__ == "__main__":
    main()
