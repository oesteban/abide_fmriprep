#!/usr/bin/env python3
"""Run variant E on Abraham's exact 714-subject sample using fMRIPrep data.

Extracts time series for any subjects in Abraham's CV splits that don't
already have extracted data (subjects that failed our QC but passed PCP's),
then runs the full classification on the matched sample.

This ensures an apples-to-apples comparison: same subjects, same CV folds,
same classification settings -- only the preprocessing differs.

Usage::

    python code/analysis/09_exact_abraham_sample.py --project-root . [--data-dir /path]
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import h5py
import nibabel as nib
import numpy as np
import pandas as pd
from nilearn.datasets import fetch_atlas_msdl
from nilearn.image import clean_img
from nilearn.interfaces.fmriprep import load_confounds
from nilearn.maskers import NiftiMapsMasker
from nilearn.signal import clean
from sklearn.linear_model import RidgeClassifier
from sklearn.model_selection import GridSearchCV, PredefinedSplit
from sklearn.svm import SVC


def _setup_path():
    here = Path(__file__).resolve().parent
    if str(here) not in sys.path:
        sys.path.insert(0, str(here))


_setup_path()

from _helpers import (
    CONFOUND_COMPCOR,
    CONFOUND_MOTION,
    CONFOUND_N_COMPCOR,
    CONFOUND_STRATEGY,
    LOW_PASS,
    N_MSDL_REGIONS,
    RANDOM_STATE,
    SPACE,
    TangentEmbeddingTransformer,
    bep017_stem,
    bold_json_from_confounds,
    bold_path_from_confounds,
    brain_mask_from_confounds,
    derivatives_connectivity,
    derivatives_fmriprep,
    fetch_abraham_cv_splits,
    find_confounds,
    get_tr,
    output_dir,
    regress_confounds,
    software_versions,
)


def extract_single_subject(subject_id, fmriprep_dir, conn_dir):
    """Extract MSDL time series for one subject, ignoring QC thresholds.

    Uses v3 (two-stage) denoising to match the current extraction on disk.
    Returns the path to the parquet file, or None on failure.
    """
    runs = find_confounds(subject_id, fmriprep_dir)
    if not runs:
        return None

    # Pick run-1 (or first available) since we don't have QC-based selection
    run_label, conf_path = runs[0]

    bold = bold_path_from_confounds(conf_path)
    mask = brain_mask_from_confounds(conf_path)
    if not bold.exists() or not mask.exists():
        return None

    tr = get_tr(conf_path)
    atlas = fetch_atlas_msdl()
    region_labels = list(atlas.labels)

    # Stage 1: voxel-level cleaning
    confounds_stage1, sample_mask = load_confounds(
        str(bold),
        strategy=CONFOUND_STRATEGY,
        motion=CONFOUND_MOTION,
        compcor=CONFOUND_COMPCOR,
        n_compcor=CONFOUND_N_COMPCOR,
        demean=True,
    )
    bold_clean = clean_img(
        str(bold),
        confounds=confounds_stage1,
        low_pass=LOW_PASS,
        high_pass=0.01,
        t_r=tr,
        detrend=True,
        mask_img=str(mask),
    )

    # Stage 2: ROI extraction + tCompCor
    masker = NiftiMapsMasker(
        maps_img=atlas.maps,
        standardize="zscore_sample",
        detrend=False,
        low_pass=None,
        high_pass=None,
        t_r=tr,
    )
    timeseries = masker.fit_transform(bold_clean)

    confounds_tsv = pd.read_csv(conf_path, sep="\t")
    tcompcor_cols = sorted([c for c in confounds_tsv.columns if c.startswith("t_comp_cor_")])[:CONFOUND_N_COMPCOR]
    if tcompcor_cols:
        confounds_stage2 = confounds_tsv[tcompcor_cols].values
        timeseries = clean(timeseries, confounds=confounds_stage2, detrend=False, standardize="zscore_sample")

    # Write parquet
    odir = output_dir(subject_id, conn_dir)
    stem = bep017_stem(subject_id, run_label)
    ts_path = odir / f"{stem}_stat-mean_timeseries.parquet"
    ts_df = pd.DataFrame(timeseries, columns=region_labels)
    ts_df.to_parquet(ts_path, index=False)

    return str(ts_path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=Path("."))
    parser.add_argument("--data-dir", type=str, default=None)
    args = parser.parse_args()
    root = args.project_root.resolve()
    np.random.seed(RANDOM_STATE)

    # Fetch Abraham's CV splits
    print("Fetching Abraham's CV splits...", flush=True)
    cv_splits = fetch_abraham_cv_splits(
        Path(args.data_dir) if args.data_dir else None
    )
    print(f"  {len(cv_splits)} subjects in CV splits", flush=True)

    # Map source_subject_id <-> participant_id
    pheno = pd.read_csv(root / "inputs" / "abide-both" / "participants.tsv", sep="\t")
    a1 = pheno[pheno["source_dataset"] == "abide1"].copy()
    sid_to_row = {}
    for _, row in a1.iterrows():
        sid_to_row[int(row["source_subject_id"])] = row

    fmriprep_dir = derivatives_fmriprep(root)
    conn_dir = derivatives_connectivity(root, variant="fmriprep-baseline")

    # For each Abraham CV subject, load or extract time series
    print("\nLoading/extracting time series for Abraham's exact sample...", flush=True)
    timeseries = []
    labels = []
    sites = []
    ages = []
    sexes = []
    fold_ids = []
    matched = 0
    extracted_new = 0
    skipped = 0

    for source_sid, fold_idx in sorted(cv_splits.items()):
        row = sid_to_row.get(source_sid)
        if row is None:
            skipped += 1
            continue

        pid = row["participant_id"]
        group = row["group"]
        if group not in ("ASD", "TC"):
            skipped += 1
            continue

        # Try to load existing extracted time series
        func_dir = conn_dir / pid / "ses-1" / "func"
        ts = None
        if func_dir.is_dir():
            parquets = list(func_dir.glob("*_timeseries.parquet"))
            if parquets:
                ts = pd.read_parquet(parquets[0]).values

        # If not extracted, extract now
        if ts is None:
            ts_path = extract_single_subject(pid, fmriprep_dir, conn_dir)
            if ts_path:
                ts = pd.read_parquet(ts_path).values
                extracted_new += 1
            else:
                skipped += 1
                continue

        if ts.shape[1] != N_MSDL_REGIONS:
            skipped += 1
            continue

        timeseries.append(ts)
        labels.append(1 if group == "ASD" else 0)
        sites.append(str(row["source_site"]))
        fold_ids.append(fold_idx)
        ages.append(float(row["age"]) if pd.notna(row.get("age")) else 25.0)
        sexes.append(1 if row.get("sex") == "M" else 2)
        matched += 1

        if matched % 100 == 0:
            print(f"  Loaded {matched} subjects ({extracted_new} newly extracted)", flush=True)

    labels = np.array(labels)
    sites = np.array(sites)
    ages = np.array(ages)
    sexes = np.array(sexes)
    fold_ids = np.array(fold_ids)

    print(f"\n=== Abraham's exact sample on fMRIPrep data ===", flush=True)
    print(f"  Matched: {matched} / {len(cv_splits)} ({extracted_new} newly extracted, {skipped} skipped)", flush=True)
    print(f"  ASD: {(labels == 1).sum()}, TC: {(labels == 0).sum()}", flush=True)
    print(f"  Sites: {len(np.unique(sites))}, Folds: {len(np.unique(fold_ids))}", flush=True)

    # Run variant E classification with Abraham's predefined splits
    cls_dir = conn_dir / "classification"
    cls_dir.mkdir(parents=True, exist_ok=True)

    unique_sites = np.unique(sites)
    site_to_idx = {s: i for i, s in enumerate(unique_sites)}
    cv = PredefinedSplit(fold_ids)

    for clf_name in ("ridge", "svc"):
        print(f"\n  Variant E ({clf_name}, {len(np.unique(fold_ids))}-fold)...", flush=True)
        fold_results = []

        for fold_i, (train_idx, test_idx) in enumerate(cv.split(timeseries, labels)):
            ts_train = [timeseries[i] for i in train_idx]
            ts_test = [timeseries[i] for i in test_idx]
            y_train, y_test = labels[train_idx], labels[test_idx]

            # Tangent embedding
            tangent = TangentEmbeddingTransformer(assume_centered=True)
            tangent.fit(ts_train)
            X_train = tangent.transform(ts_train)
            X_test = tangent.transform(ts_test)

            # Confound regression (site + age + sex)
            def _build_confounds(idx):
                n = len(idx)
                site_dummies = np.zeros((n, len(unique_sites)))
                for j, i in enumerate(idx):
                    site_dummies[j, site_to_idx[sites[i]]] = 1.0
                return np.column_stack([site_dummies, ages[idx].reshape(-1, 1), sexes[idx].reshape(-1, 1)])

            X_train, X_test = regress_confounds(
                X_train, X_test, _build_confounds(train_idx), _build_confounds(test_idx)
            )

            # Classifier with nested CV tuning
            if clf_name == "ridge":
                clf = GridSearchCV(RidgeClassifier(), {"alpha": np.logspace(-3, 3, 7)}, cv=5, scoring="accuracy")
            else:
                clf = GridSearchCV(SVC(kernel="linear"), {"C": np.logspace(-3, 3, 7)}, cv=5, scoring="accuracy")

            clf.fit(X_train, y_train)
            acc = clf.score(X_test, y_test)
            fold_results.append({"fold": fold_i, "accuracy": round(float(acc), 6), "n_test": int(len(test_idx))})

        accuracies = [r["accuracy"] for r in fold_results]
        result = {
            "experiment": "fmriprep_exact_abraham_sample",
            "variant": "E_full_replication",
            "cv_scheme": "abraham_10fold",
            "classifier": clf_name,
            "n_folds": len(np.unique(fold_ids)),
            "n_subjects": matched,
            "n_subjects_abraham": len(cv_splits),
            "n_newly_extracted": extracted_new,
            "n_skipped": skipped,
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
