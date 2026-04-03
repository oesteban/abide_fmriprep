#!/usr/bin/env python3
"""fMRIPrep baseline classification: Abraham's exact sample.

Reads pre-extracted v1 parquets for Abraham's 714-subject CV split,
runs variant E classification. Direct comparison with C-PAC ablation E.

Extraction is handled by extract_subject.py; this script is
classification-only.

Usage::

    python code/analysis/09_exact_abraham_sample.py --project-root . [--data-dir /path]
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
from sklearn.model_selection import GridSearchCV, PredefinedSplit
from sklearn.svm import SVC


def _setup_path():
    here = Path(__file__).resolve().parent
    if str(here) not in sys.path:
        sys.path.insert(0, str(here))


_setup_path()

from _helpers import (
    N_MSDL_REGIONS,
    RANDOM_STATE,
    SPACE,
    TangentEmbeddingTransformer,
    derivatives_connectivity,
    derivatives_fmriprep,
    fetch_abraham_cv_splits,
    find_confounds,
    regress_confounds,
    software_versions,
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=Path("."))
    parser.add_argument("--data-dir", type=str, default=None)
    parser.add_argument("--source-variant", default="v1",
                        help="Which fMRIPrep variant's parquets to read (default: v1).")
    args = parser.parse_args()
    root = args.project_root.resolve()
    np.random.seed(RANDOM_STATE)

    # Abraham's CV splits
    print("Fetching Abraham's CV splits...", flush=True)
    cv_splits = fetch_abraham_cv_splits(Path(args.data_dir) if args.data_dir else None)
    print(f"  {len(cv_splits)} subjects", flush=True)

    # Map source_subject_id → participant_id
    pheno = pd.read_csv(root / "inputs" / "abide-both" / "participants.tsv", sep="\t")
    a1 = pheno[pheno["source_dataset"] == "abide1"]
    sid_to_row = {int(r["source_subject_id"]): r for _, r in a1.iterrows()}

    # Read parquets from the source variant dataset
    source_dir = derivatives_connectivity(root, variant=args.source_variant)

    timeseries = []
    labels = []
    site_labels = []
    age_values = []
    sex_values = []
    fold_values = []
    matched = 0
    missing = 0

    for source_sid, fold_idx in sorted(cv_splits.items()):
        row = sid_to_row.get(source_sid)
        if row is None:
            missing += 1
            continue

        pid = row["participant_id"]
        group = row["group"]
        if group not in ("ASD", "TC"):
            missing += 1
            continue

        # Load parquet from source variant
        func_dir = source_dir / pid / "ses-1" / "func"
        parquets = list(func_dir.glob("*_timeseries.parquet")) if func_dir.is_dir() else []
        if not parquets:
            missing += 1
            continue

        ts = pd.read_parquet(parquets[0]).values
        if ts.shape[1] != N_MSDL_REGIONS:
            missing += 1
            continue

        timeseries.append(ts)
        labels.append(1 if group == "ASD" else 0)
        site_labels.append(str(row["source_site"]))
        age_values.append(float(row["age"]) if pd.notna(row.get("age")) else 25.0)
        sex_values.append(1 if row.get("sex") == "M" else 2)
        fold_values.append(fold_idx)
        matched += 1

    labels = np.array(labels)
    site_labels = np.array(site_labels)
    ages = np.array(age_values)
    sexes = np.array(sex_values)
    fold_ids = np.array(fold_values)

    print(f"\n=== fMRIPrep baseline (Abraham's exact sample) ===", flush=True)
    print(f"  Matched: {matched} / {len(cv_splits)} (missing: {missing})", flush=True)
    print(f"  ASD: {(labels == 1).sum()}, TC: {(labels == 0).sum()}", flush=True)
    print(f"  Sites: {len(np.unique(site_labels))}, Folds: {len(np.unique(fold_ids))}", flush=True)

    # Variant E classification
    cls_dir = derivatives_connectivity(root, variant="fmriprep-baseline") / "classification"
    cls_dir.mkdir(parents=True, exist_ok=True)

    unique_sites = np.unique(site_labels)
    site_to_idx = {s: i for i, s in enumerate(unique_sites)}
    cv = PredefinedSplit(fold_ids)

    for clf_name in ("ridge", "svc"):
        print(f"\n  Variant E ({clf_name}, {len(np.unique(fold_ids))}-fold)...", flush=True)
        fold_results = []

        for fold_i, (train_idx, test_idx) in enumerate(cv.split(timeseries, labels)):
            ts_train = [timeseries[i] for i in train_idx]
            ts_test = [timeseries[i] for i in test_idx]
            y_train, y_test = labels[train_idx], labels[test_idx]

            tangent = TangentEmbeddingTransformer(assume_centered=True)
            tangent.fit(ts_train)
            X_train = tangent.transform(ts_train)
            X_test = tangent.transform(ts_test)

            def _confounds(idx):
                n = len(idx)
                sd = np.zeros((n, len(unique_sites)))
                for j, i in enumerate(idx):
                    sd[j, site_to_idx[site_labels[i]]] = 1.0
                return np.column_stack([sd, ages[idx].reshape(-1, 1), sexes[idx].reshape(-1, 1)])

            X_train, X_test = regress_confounds(X_train, X_test, _confounds(train_idx), _confounds(test_idx))

            if clf_name == "ridge":
                clf = GridSearchCV(RidgeClassifier(), {"alpha": np.logspace(-3, 3, 7)}, cv=5, scoring="accuracy")
            else:
                clf = GridSearchCV(SVC(kernel="linear"), {"C": np.logspace(-3, 3, 7)}, cv=5, scoring="accuracy")

            clf.fit(X_train, y_train)
            acc = clf.score(X_test, y_test)
            fold_results.append({"fold": fold_i, "accuracy": round(float(acc), 6), "n_test": int(len(test_idx))})

        accuracies = [r["accuracy"] for r in fold_results]
        result = {
            "experiment": "fmriprep_baseline",
            "variant": "E_full_replication",
            "source_variant": args.source_variant,
            "cv_scheme": "abraham_10fold",
            "classifier": clf_name,
            "n_folds": len(np.unique(fold_ids)),
            "n_subjects": matched,
            "n_missing": missing,
            "mean_accuracy": round(float(np.mean(accuracies)), 6),
            "std_accuracy": round(float(np.std(accuracies)), 6),
            "per_fold": fold_results,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "software_versions": software_versions(),
        }
        with open(cls_dir / f"results_exact_abraham_fmriprep_{clf_name}.json", "w") as f:
            json.dump(result, f, indent=2)
        print(f"    Accuracy: {result['mean_accuracy']:.1%} (+/- {result['std_accuracy']:.1%})", flush=True)

    print(f"\nResults saved to {cls_dir}/", flush=True)


if __name__ == "__main__":
    main()
