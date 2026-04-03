#!/usr/bin/env python3
"""Faithful replication of Abraham et al. (2017) on C-PAC ABIDE I data.

Addresses all identified discrepancies vs the original implementation:
  1. Uses Abraham's exact 10-fold CV splits (from cv_abide.zip)
  2. Group-level confound regression (site + age + sex) on tangent features
  3. Nested CV for hyperparameter tuning (Ridge alpha)
  4. LedoitWolf(assume_centered=True) for ConnectivityMeasure
  5. Exact same 871 subjects via fetch_abide_pcp(quality_checked=True)

Also runs a step-by-step ablation to measure each fix's contribution.

Usage::

    python code/analysis/07_faithful_replication.py --project-root . [--data-dir /path]
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from nilearn.datasets import fetch_abide_pcp, fetch_atlas_msdl
from nilearn.maskers import NiftiMapsMasker
from sklearn.linear_model import RidgeClassifier
from sklearn.model_selection import (
    GridSearchCV,
    LeaveOneGroupOut,
    PredefinedSplit,
)
from sklearn.svm import SVC


def _setup_path():
    here = Path(__file__).resolve().parent
    if str(here) not in sys.path:
        sys.path.insert(0, str(here))


_setup_path()

from _helpers import (
    N_MSDL_REGIONS,
    TangentEmbeddingTransformer,
    derivatives_connectivity,
    fetch_abraham_cv_splits,
    regress_confounds,
)


# --------------------------------------------------------------------------- #
# Data loading
# --------------------------------------------------------------------------- #


def load_cpac_data(data_dir: str | None = None):
    """Fetch C-PAC ABIDE I data, extract MSDL time series, return all metadata."""
    print("Fetching ABIDE PCP (C-PAC, func_preproc)...", flush=True)
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
    masker = NiftiMapsMasker(
        maps_img=atlas.maps,
        standardize="zscore_sample",
        detrend=True,
        low_pass=None,
        high_pass=None,
    )

    timeseries_list = []
    labels = []
    sites = []
    subject_ids = []
    ages = []
    sexes = []
    skipped = 0

    print(f"  Extracting time series from {len(func_files)} subjects...", flush=True)
    for i, func in enumerate(func_files):
        dx = int(phenotypic["DX_GROUP"].iloc[i])
        site = str(phenotypic["SITE_ID"].iloc[i])
        sub_id = int(phenotypic["SUB_ID"].iloc[i])
        age = float(phenotypic["AGE_AT_SCAN"].iloc[i])
        sex = int(phenotypic["SEX"].iloc[i])  # 1=M, 2=F

        if dx not in (1, 2):
            skipped += 1
            continue

        try:
            ts = masker.fit_transform(func)
            if ts.shape[1] != N_MSDL_REGIONS:
                skipped += 1
                continue
            timeseries_list.append(ts)
            labels.append(1 if dx == 1 else 0)
            sites.append(site)
            subject_ids.append(sub_id)
            ages.append(age)
            sexes.append(sex)
        except Exception as e:
            print(f"  WARNING: subject {sub_id} failed: {e}", flush=True)
            skipped += 1

        if (i + 1) % 50 == 0:
            print(f"  Processed {i + 1}/{len(func_files)} "
                  f"(extracted: {len(timeseries_list)}, skipped: {skipped})", flush=True)

    print(f"  Extracted: {len(timeseries_list)}, skipped: {skipped}", flush=True)
    return {
        "timeseries": timeseries_list,
        "labels": np.array(labels),
        "sites": np.array(sites),
        "subject_ids": np.array(subject_ids),
        "ages": np.array(ages),
        "sexes": np.array(sexes),
    }


# --------------------------------------------------------------------------- #
# Classification variants
# --------------------------------------------------------------------------- #


def run_classification(
    data: dict,
    variant: str,
    cv_splits: dict | None = None,
    assume_centered: bool = False,
    confound_regression: bool = False,
    tune_hyperparams: bool = False,
    classifier_name: str = "ridge",
) -> dict:
    """Run classification with configurable options.

    Parameters
    ----------
    variant : str
        Label for this configuration.
    cv_splits : dict, optional
        Abraham's fold assignments {subject_id: fold_idx}. If None, use LOGO.
    assume_centered : bool
        Pass to LedoitWolf in ConnectivityMeasure.
    confound_regression : bool
        Regress site + age + sex from tangent features.
    tune_hyperparams : bool
        Use GridSearchCV for Ridge alpha / SVC C.
    """
    timeseries = data["timeseries"]
    labels = data["labels"]
    sites = data["sites"]
    subject_ids = data["subject_ids"]
    ages = data["ages"]
    sexes = data["sexes"]

    # Build CV iterator
    if cv_splits is not None:
        # Use Abraham's predefined folds
        fold_ids = np.array([cv_splits.get(int(sid), -1) for sid in subject_ids])
        # Subjects not in cv_splits get fold -1 (excluded)
        mask = fold_ids >= 0
        if mask.sum() < len(mask):
            print(f"    {mask.sum()}/{len(mask)} subjects matched to CV splits", flush=True)
            timeseries = [timeseries[i] for i in range(len(timeseries)) if mask[i]]
            labels = labels[mask]
            sites = sites[mask]
            subject_ids = subject_ids[mask]
            ages = ages[mask]
            sexes = sexes[mask]
            fold_ids = fold_ids[mask]
        cv = PredefinedSplit(fold_ids)
        n_folds = len(np.unique(fold_ids))
    else:
        # LeaveOneGroupOut
        cv = LeaveOneGroupOut()
        fold_ids = None
        n_folds = len(np.unique(sites))

    # Build site dummy variables for confound regression
    unique_sites = np.unique(sites)
    site_to_idx = {s: i for i, s in enumerate(unique_sites)}

    fold_results = []

    splits = cv.split(timeseries, labels, groups=sites if fold_ids is None else None)
    for fold_i, (train_idx, test_idx) in enumerate(splits):
        ts_train = [timeseries[i] for i in train_idx]
        ts_test = [timeseries[i] for i in test_idx]
        y_train, y_test = labels[train_idx], labels[test_idx]

        # Compute tangent features
        tangent = TangentEmbeddingTransformer(assume_centered=assume_centered)
        tangent.fit(ts_train)
        X_train = tangent.transform(ts_train)
        X_test = tangent.transform(ts_test)

        # Confound regression (site + age + sex)
        if confound_regression:
            def _build_confounds(idx):
                n = len(idx)
                site_dummies = np.zeros((n, len(unique_sites)))
                for j, i in enumerate(idx):
                    site_dummies[j, site_to_idx[sites[i]]] = 1.0
                return np.column_stack([
                    site_dummies,
                    ages[idx].reshape(-1, 1),
                    sexes[idx].reshape(-1, 1),
                ])

            conf_train = _build_confounds(train_idx)
            conf_test = _build_confounds(test_idx)
            X_train, X_test = regress_confounds(X_train, X_test, conf_train, conf_test)

        # Classifier
        if tune_hyperparams:
            if classifier_name == "ridge":
                clf = GridSearchCV(
                    RidgeClassifier(),
                    {"alpha": np.logspace(-3, 3, 7)},
                    cv=5,
                    scoring="accuracy",
                )
            else:
                clf = GridSearchCV(
                    SVC(kernel="linear"),
                    {"C": np.logspace(-3, 3, 7)},
                    cv=5,
                    scoring="accuracy",
                )
        else:
            clf = RidgeClassifier() if classifier_name == "ridge" else SVC(kernel="linear")

        clf.fit(X_train, y_train)
        accuracy = clf.score(X_test, y_test)

        test_sites = np.unique(sites[test_idx])
        fold_results.append({
            "fold": fold_i,
            "accuracy": round(float(accuracy), 6),
            "n_test": int(len(test_idx)),
            "n_asd": int((y_test == 1).sum()),
            "n_tc": int((y_test == 0).sum()),
            "test_sites": list(test_sites),
        })

    accuracies = [r["accuracy"] for r in fold_results]
    return {
        "variant": variant,
        "cv_scheme": "abraham_10fold" if cv_splits else "leave_one_group_out",
        "classifier": classifier_name,
        "n_folds": n_folds,
        "n_subjects": len(labels),
        "n_sites": len(unique_sites),
        "assume_centered": assume_centered,
        "confound_regression": confound_regression,
        "tune_hyperparams": tune_hyperparams,
        "mean_accuracy": round(float(np.mean(accuracies)), 6),
        "std_accuracy": round(float(np.std(accuracies)), 6),
        "per_fold": fold_results,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=Path("."))
    parser.add_argument("--data-dir", type=str, default=None)
    args = parser.parse_args()
    root = args.project_root.resolve()
    data_dir = args.data_dir

    # Load data
    data = load_cpac_data(data_dir)
    print(f"\n=== Faithful Replication: C-PAC ABIDE I (N={len(data['timeseries'])}) ===",
          flush=True)
    print(f"  ASD: {(data['labels'] == 1).sum()}, TC: {(data['labels'] == 0).sum()}",
          flush=True)
    print(f"  Sites: {len(np.unique(data['sites']))}", flush=True)

    # Fetch Abraham's CV splits
    print("\nFetching Abraham's CV splits...", flush=True)
    cv_splits = fetch_abraham_cv_splits(Path(data_dir) if data_dir else None)

    cls_dir = derivatives_connectivity(root, variant="cpac") / "classification"
    cls_dir.mkdir(parents=True, exist_ok=True)

    # Ablation study: apply fixes incrementally
    variants = [
        {
            "name": "A_our_baseline",
            "desc": "Our original: LOGO, no confound reg, no tuning, default LW",
            "cv_splits": None,
            "assume_centered": False,
            "confound_regression": False,
            "tune_hyperparams": False,
        },
        {
            "name": "B_abraham_cv",
            "desc": "+ Abraham's 10-fold CV splits",
            "cv_splits": cv_splits,
            "assume_centered": False,
            "confound_regression": False,
            "tune_hyperparams": False,
        },
        {
            "name": "C_confound_reg",
            "desc": "+ group-level confound regression (site+age+sex)",
            "cv_splits": cv_splits,
            "assume_centered": False,
            "confound_regression": True,
            "tune_hyperparams": False,
        },
        {
            "name": "D_tuning",
            "desc": "+ nested CV hyperparameter tuning",
            "cv_splits": cv_splits,
            "assume_centered": False,
            "confound_regression": True,
            "tune_hyperparams": True,
        },
        {
            "name": "E_full_replication",
            "desc": "All fixes: Abraham CV + confounds + tuning + LW(centered)",
            "cv_splits": cv_splits,
            "assume_centered": True,
            "confound_regression": True,
            "tune_hyperparams": True,
        },
    ]

    all_results = []
    for v in variants:
        for clf_name in ("ridge", "svc"):
            print(f"\n--- {v['name']} ({clf_name}): {v['desc']} ---", flush=True)
            result = run_classification(
                data,
                variant=v["name"],
                cv_splits=v["cv_splits"],
                assume_centered=v["assume_centered"],
                confound_regression=v["confound_regression"],
                tune_hyperparams=v["tune_hyperparams"],
                classifier_name=clf_name,
            )
            out_path = cls_dir / f"results_faithful_{v['name']}_{clf_name}.json"
            with open(out_path, "w") as f:
                json.dump(result, f, indent=2)
            print(f"    Accuracy: {result['mean_accuracy']:.1%} "
                  f"(+/- {result['std_accuracy']:.1%})", flush=True)
            all_results.append(result)

    # Print summary table
    print("\n" + "=" * 70, flush=True)
    print("ABLATION SUMMARY (Abraham target: 66.8% for Ridge inter-site)", flush=True)
    print("=" * 70, flush=True)
    print(f"{'Variant':<25s} {'Ridge':>8s} {'SVC':>8s}", flush=True)
    print("-" * 45, flush=True)
    for v in variants:
        ridge = [r for r in all_results if r["variant"] == v["name"] and r["classifier"] == "ridge"]
        svc = [r for r in all_results if r["variant"] == v["name"] and r["classifier"] == "svc"]
        r_acc = f"{ridge[0]['mean_accuracy']:.1%}" if ridge else "---"
        s_acc = f"{svc[0]['mean_accuracy']:.1%}" if svc else "---"
        print(f"{v['name']:<25s} {r_acc:>8s} {s_acc:>8s}", flush=True)
    print(f"{'Abraham (2017)':<25s} {'66.8%':>8s} {'---':>8s}", flush=True)

    print(f"\nAll results saved to {cls_dir}/", flush=True)


if __name__ == "__main__":
    main()
