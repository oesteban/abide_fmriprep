#!/usr/bin/env python3
"""fMRIPrep baseline: same subjects, folds, and classification as C-PAC ablation E.

Takes Abraham's exact 714-subject CV split, uses fMRIPrep-preprocessed BOLD
instead of C-PAC, extracts MSDL time series, and runs variant E classification.
Direct comparison with 07_faithful_replication.py variant E.

Usage::

    python code/analysis/09_exact_abraham_sample.py --project-root . \
        --data-dir /path/to/nilearn_cache [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import nibabel as nib
import numpy as np
import pandas as pd
from nilearn.datasets import fetch_atlas_msdl
from nilearn.maskers import NiftiMapsMasker
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
    TangentEmbeddingTransformer,
    derivatives_connectivity,
    derivatives_fmriprep,
    fetch_abraham_cv_splits,
    find_confounds,
    bold_path_from_confounds,
    get_tr,
    regress_confounds,
    software_versions,
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=Path("."))
    parser.add_argument("--data-dir", type=str, default=None)
    parser.add_argument("--dry-run", action="store_true",
                        help="Check subject matching without extracting or classifying.")
    args = parser.parse_args()
    root = args.project_root.resolve()
    np.random.seed(RANDOM_STATE)

    # 1. Get Abraham's 714-subject CV fold assignments
    print("Fetching Abraham's CV splits...", flush=True)
    cv_splits = fetch_abraham_cv_splits(
        Path(args.data_dir) if args.data_dir else None
    )
    print(f"  {len(cv_splits)} subjects in CV splits", flush=True)

    # 2. Map PCP source_subject_id → our participant_id
    pheno = pd.read_csv(root / "inputs" / "abide-both" / "participants.tsv", sep="\t")
    a1 = pheno[pheno["source_dataset"] == "abide1"]
    sid_to_row = {int(r["source_subject_id"]): r for _, r in a1.iterrows()}

    # 3. Check fMRIPrep availability for each CV subject
    fmriprep_dir = derivatives_fmriprep(root)
    atlas = fetch_atlas_msdl()

    subjects = []  # list of dicts with all info needed
    no_mapping = []
    no_fmriprep = []

    for source_sid, fold_idx in sorted(cv_splits.items()):
        row = sid_to_row.get(source_sid)
        if row is None:
            no_mapping.append(source_sid)
            continue

        pid = row["participant_id"]
        runs = find_confounds(pid, fmriprep_dir)
        if not runs:
            no_fmriprep.append((source_sid, pid))
            continue

        run_label, conf_path = runs[0]  # first available run
        bold = bold_path_from_confounds(conf_path)

        subjects.append({
            "source_sid": source_sid,
            "participant_id": pid,
            "fold": fold_idx,
            "run_label": run_label,
            "conf_path": conf_path,
            "bold_path": bold,
            "group": row["group"],
            "site": str(row["source_site"]),
            "age": float(row["age"]) if pd.notna(row.get("age")) else 25.0,
            "sex": 1 if row.get("sex") == "M" else 2,
        })

    print(f"\n  Matched to fMRIPrep: {len(subjects)}", flush=True)
    print(f"  No mapping in participants.tsv: {len(no_mapping)}", flush=True)
    print(f"  No fMRIPrep derivatives: {len(no_fmriprep)}", flush=True)
    if no_fmriprep:
        for sid, pid in no_fmriprep:
            print(f"    {pid} (source_sid={sid})", flush=True)

    folds = sorted(set(s["fold"] for s in subjects))
    sites = sorted(set(s["site"] for s in subjects))
    print(f"  Folds: {len(folds)}, Sites: {len(sites)}", flush=True)

    if args.dry_run:
        # Check BOLD availability (file exists on disk vs annex pointer)
        bold_available = sum(1 for s in subjects if s["bold_path"].exists() and s["bold_path"].stat().st_size > 1000)
        bold_pointer = len(subjects) - bold_available
        print(f"\n  BOLD files available (content on disk): {bold_available}", flush=True)
        print(f"  BOLD files needing datalad get: {bold_pointer}", flush=True)
        print(f"\n  DRY RUN complete. {len(subjects)} subjects ready for extraction.", flush=True)
        return

    # 4. Extract MSDL time series for all matched subjects
    print(f"\nExtracting time series for {len(subjects)} subjects...", flush=True)
    masker = NiftiMapsMasker(
        maps_img=atlas.maps,
        standardize="zscore_sample",
        detrend=True,
        low_pass=None,
        high_pass=None,
    )

    timeseries = []
    labels = []
    site_labels = []
    age_values = []
    sex_values = []
    fold_values = []
    extracted = 0
    failed = 0

    for i, s in enumerate(subjects):
        try:
            ts = masker.fit_transform(str(s["bold_path"]))
            if ts.shape[1] != N_MSDL_REGIONS:
                failed += 1
                continue
            timeseries.append(ts)
            labels.append(1 if s["group"] == "ASD" else 0)
            site_labels.append(s["site"])
            age_values.append(s["age"])
            sex_values.append(s["sex"])
            fold_values.append(s["fold"])
            extracted += 1
        except Exception as e:
            print(f"  FAILED: {s['participant_id']}: {e}", flush=True)
            failed += 1

        if (i + 1) % 50 == 0:
            print(f"  {i + 1}/{len(subjects)} (extracted: {extracted}, failed: {failed})", flush=True)

    labels = np.array(labels)
    site_labels = np.array(site_labels)
    ages = np.array(age_values)
    sexes = np.array(sex_values)
    fold_ids = np.array(fold_values)

    print(f"\n=== fMRIPrep baseline (Abraham's exact sample) ===", flush=True)
    print(f"  Extracted: {extracted}, Failed: {failed}", flush=True)
    print(f"  ASD: {(labels == 1).sum()}, TC: {(labels == 0).sum()}", flush=True)
    print(f"  Sites: {len(np.unique(site_labels))}, Folds: {len(np.unique(fold_ids))}", flush=True)

    # 5. Variant E classification (same as 07_faithful_replication.py variant E)
    conn_dir = derivatives_connectivity(root, variant="fmriprep-baseline")
    cls_dir = conn_dir / "classification"
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
            "cv_scheme": "abraham_10fold",
            "classifier": clf_name,
            "n_folds": len(np.unique(fold_ids)),
            "n_subjects": extracted,
            "n_subjects_abraham": len(cv_splits),
            "n_failed": failed,
            "mean_accuracy": round(float(np.mean(accuracies)), 6),
            "std_accuracy": round(float(np.std(accuracies)), 6),
            "per_fold": fold_results,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "software_versions": software_versions(),
        }
        out_path = cls_dir / f"results_exact_abraham_fmriprep_{clf_name}.json"
        with open(out_path, "w") as f:
            json.dump(result, f, indent=2)
        print(f"    Accuracy: {result['mean_accuracy']:.1%} (+/- {result['std_accuracy']:.1%})", flush=True)

    print(f"\nResults saved to {cls_dir}/", flush=True)


if __name__ == "__main__":
    main()
