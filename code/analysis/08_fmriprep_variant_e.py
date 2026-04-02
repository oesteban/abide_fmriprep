#!/usr/bin/env python3
"""Run variant E classification on fMRIPrep-extracted ABIDE data.

Applies Abraham-faithful classification settings to the fMRIPrep time
series already extracted in derivatives/connectivity/:
  - Group-level confound regression (site + age + sex)
  - Nested CV hyperparameter tuning (Ridge alpha, SVC C)
  - LedoitWolf(assume_centered=True) for tangent embedding
  - LeaveOneGroupOut CV (since Abraham's fold assignments don't map to
    our fMRIPrep subject IDs)

Runs two experiments: ABIDE I only and ABIDE I+II combined.

Usage::

    python code/analysis/08_fmriprep_variant_e.py --project-root .
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from nilearn.connectome import ConnectivityMeasure
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.covariance import LedoitWolf
from sklearn.linear_model import LinearRegression, RidgeClassifier
from sklearn.model_selection import GridSearchCV, LeaveOneGroupOut
from sklearn.svm import SVC


def _setup_path():
    here = Path(__file__).resolve().parent
    if str(here) not in sys.path:
        sys.path.insert(0, str(here))


_setup_path()

from _helpers import (
    N_MSDL_REGIONS,
    bep017_stem,
    derivatives_connectivity,
    eligible_subjects,
    site_prefix,
)


class TangentEmbeddingTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, assume_centered=True):
        self.assume_centered = assume_centered

    def fit(self, X, y=None):
        self._conn = ConnectivityMeasure(
            cov_estimator=LedoitWolf(assume_centered=self.assume_centered),
            kind="tangent", vectorize=True, discard_diagonal=True,
        )
        self._conn.fit(X)
        return self

    def transform(self, X):
        return self._conn.transform(X)


def load_fmriprep_timeseries(project_root: Path):
    """Load extracted time series + phenotypic data for all QC-passing subjects."""
    conn_dir = derivatives_connectivity(project_root)
    qc_path = conn_dir / "qc_prescreen.tsv"
    qc_df = pd.read_csv(qc_path, sep="\t")
    qc_pass = qc_df[qc_df["excluded_reason"] == "pass"].copy()

    # Load phenotypic data for age/sex
    pheno = eligible_subjects(project_root)
    pheno_lookup = pheno.set_index("participant_id")

    timeseries = []
    labels = []
    sites = []  # dataset-qualified to avoid collisions
    ages = []
    sexes = []
    datasets = []
    subject_ids = []

    for _, row in qc_pass.iterrows():
        sub_id = row["participant_id"]
        run_label = row["selected_run"]
        stem = bep017_stem(sub_id, run_label)
        ts_path = conn_dir / sub_id / "ses-1" / "func" / f"{stem}_stat-mean_timeseries.parquet"

        if not ts_path.exists():
            continue

        ts = pd.read_parquet(ts_path).values
        if ts.shape[1] != N_MSDL_REGIONS:
            continue

        # Get phenotypic info
        if sub_id not in pheno_lookup.index:
            continue
        p = pheno_lookup.loc[sub_id]

        timeseries.append(ts)
        labels.append(1 if row["group"] == "ASD" else 0)
        sites.append(f"{row['source_dataset']}_{row['source_site']}")
        datasets.append(row["source_dataset"])
        subject_ids.append(sub_id)

        # Age and sex (with fallback for missing)
        age = float(p["age"]) if pd.notna(p.get("age")) else 25.0
        sex_val = p.get("sex", "M")
        sex = 1 if sex_val == "M" else 2
        ages.append(age)
        sexes.append(sex)

    print(f"  Loaded {len(timeseries)} subjects", flush=True)
    return {
        "timeseries": timeseries,
        "labels": np.array(labels),
        "sites": np.array(sites),
        "datasets": np.array(datasets),
        "ages": np.array(ages),
        "sexes": np.array(sexes),
        "subject_ids": subject_ids,
    }


def run_variant_e(data, experiment_label, classifier_name="ridge"):
    """Run variant E classification: confound reg + tuning + LW centered + LOGO."""
    timeseries = data["timeseries"]
    labels = data["labels"]
    sites = data["sites"]
    ages = data["ages"]
    sexes = data["sexes"]

    unique_sites = np.unique(sites)
    site_to_idx = {s: i for i, s in enumerate(unique_sites)}
    logo = LeaveOneGroupOut()

    fold_results = []
    for fold_i, (train_idx, test_idx) in enumerate(logo.split(timeseries, labels, groups=sites)):
        test_site = sites[test_idx[0]]
        ts_train = [timeseries[i] for i in train_idx]
        ts_test = [timeseries[i] for i in test_idx]
        y_train, y_test = labels[train_idx], labels[test_idx]

        # Tangent embedding (LedoitWolf assume_centered=True)
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
            return np.column_stack([
                site_dummies,
                ages[idx].reshape(-1, 1),
                sexes[idx].reshape(-1, 1),
            ])

        conf_train = _build_confounds(train_idx)
        conf_test = _build_confounds(test_idx)
        reg = LinearRegression().fit(conf_train, X_train)
        X_train = X_train - reg.predict(conf_train)
        X_test = X_test - reg.predict(conf_test)

        # Classifier with nested CV tuning
        if classifier_name == "ridge":
            clf = GridSearchCV(
                RidgeClassifier(), {"alpha": np.logspace(-3, 3, 7)},
                cv=5, scoring="accuracy",
            )
        else:
            clf = GridSearchCV(
                SVC(kernel="linear"), {"C": np.logspace(-3, 3, 7)},
                cv=5, scoring="accuracy",
            )

        clf.fit(X_train, y_train)
        accuracy = clf.score(X_test, y_test)

        fold_results.append({
            "fold": fold_i,
            "test_site": test_site,
            "accuracy": round(float(accuracy), 6),
            "n_test": int(len(test_idx)),
            "n_asd": int((y_test == 1).sum()),
            "n_tc": int((y_test == 0).sum()),
        })

        if (fold_i + 1) % 5 == 0:
            accs_so_far = [r["accuracy"] for r in fold_results]
            print(f"    Fold {fold_i + 1}/{len(unique_sites)}: "
                  f"running mean = {np.mean(accs_so_far):.1%}", flush=True)

    accuracies = [r["accuracy"] for r in fold_results]
    return {
        "experiment": experiment_label,
        "variant": "E_full_replication",
        "cv_scheme": "leave_one_group_out",
        "classifier": classifier_name,
        "confound_regression": True,
        "tune_hyperparams": True,
        "assume_centered": True,
        "n_folds": len(unique_sites),
        "n_subjects": len(labels),
        "n_sites": len(unique_sites),
        "mean_accuracy": round(float(np.mean(accuracies)), 6),
        "std_accuracy": round(float(np.std(accuracies)), 6),
        "per_fold": fold_results,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=Path("."))
    args = parser.parse_args()
    root = args.project_root.resolve()

    print("Loading fMRIPrep-extracted time series...", flush=True)
    data = load_fmriprep_timeseries(root)
    print(f"  ASD: {(data['labels'] == 1).sum()}, TC: {(data['labels'] == 0).sum()}", flush=True)
    print(f"  Sites: {len(np.unique(data['sites']))}", flush=True)

    cls_dir = derivatives_connectivity(root) / "classification"
    cls_dir.mkdir(parents=True, exist_ok=True)

    # --- Experiment 1: ABIDE I only ---
    abide1_mask = data["datasets"] == "abide1"
    data_a1 = {
        "timeseries": [data["timeseries"][i] for i in range(len(data["timeseries"])) if abide1_mask[i]],
        "labels": data["labels"][abide1_mask],
        "sites": data["sites"][abide1_mask],
        "ages": data["ages"][abide1_mask],
        "sexes": data["sexes"][abide1_mask],
    }
    n_a1 = len(data_a1["timeseries"])
    print(f"\n=== ABIDE I only (N={n_a1}, {len(np.unique(data_a1['sites']))} sites) ===", flush=True)

    for clf_name in ("ridge", "svc"):
        print(f"\n  Variant E ({clf_name})...", flush=True)
        result = run_variant_e(data_a1, "fmriprep_abide1", clf_name)
        with open(cls_dir / f"results_variantE_fmriprep_abide1_{clf_name}.json", "w") as f:
            json.dump(result, f, indent=2)
        print(f"    Mean accuracy: {result['mean_accuracy']:.1%} "
              f"(+/- {result['std_accuracy']:.1%})", flush=True)

    # --- Experiment 2: ABIDE I + II ---
    n_all = len(data["timeseries"])
    print(f"\n=== ABIDE I+II (N={n_all}, {len(np.unique(data['sites']))} sites) ===", flush=True)

    for clf_name in ("ridge", "svc"):
        print(f"\n  Variant E ({clf_name})...", flush=True)
        result = run_variant_e(data, "fmriprep_both", clf_name)
        with open(cls_dir / f"results_variantE_fmriprep_both_{clf_name}.json", "w") as f:
            json.dump(result, f, indent=2)
        print(f"    Mean accuracy: {result['mean_accuracy']:.1%} "
              f"(+/- {result['std_accuracy']:.1%})", flush=True)

    print(f"\nAll results saved to {cls_dir}/", flush=True)


if __name__ == "__main__":
    main()
