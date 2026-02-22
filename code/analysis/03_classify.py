#!/usr/bin/env python3
"""Classification: replicate Abraham et al. 2017 Table 2.

Implements:
- TangentEmbeddingTransformer: sklearn-compatible wrapper around
  nilearn ConnectivityMeasure(kind="tangent") with proper fit/transform
  separation (geometric mean estimated from training data only).
- Inter-site CV: LeaveOneGroupOut (leave-one-site-out)
- Intra-site CV: StratifiedShuffleSplit (100 splits, 20% test)
- Classifiers: RidgeClassifier (primary), SVC(kernel="linear")
- Two experiments: ABIDE I only, ABIDE I+II combined

Usage::

    python code/analysis/03_classify.py [--project-root .]

Outputs::

    derivatives/connectivity/classification/
        results_intersite_abide1.json
        results_intrasite_abide1.json
        results_intersite_both.json
        results_intrasite_both.json
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.linear_model import RidgeClassifier
from sklearn.model_selection import (
    LeaveOneGroupOut,
    StratifiedShuffleSplit,
    cross_val_score,
)
from sklearn.pipeline import Pipeline
from sklearn.svm import SVC

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from analysis._helpers import (
    CONNECTIVITY_DIR,
    MSDL_N_FEATURES,
    MSDL_N_REGIONS,
    PROJECT_ROOT,
    find_timeseries_path,
    read_timeseries_parquet,
    write_json,
)

logger = logging.getLogger(__name__)

MIN_SUBJECTS_PER_SITE = 10
MIN_PER_CLASS_INTRASITE = 5


class TangentEmbeddingTransformer(BaseEstimator, TransformerMixin):
    """sklearn-compatible wrapper for nilearn tangent connectivity.

    Fits the geometric mean on training data and projects both train
    and test data into tangent space, avoiding information leakage.

    Parameters
    ----------
    vectorize : bool
        Whether to return upper-triangle vector (True) or full matrix.
    discard_diagonal : bool
        Whether to discard diagonal entries in vectorized output.
    """

    def __init__(self, vectorize: bool = True, discard_diagonal: bool = True):
        self.vectorize = vectorize
        self.discard_diagonal = discard_diagonal

    @staticmethod
    def _to_list(X):
        """Ensure X is a plain list of 2D arrays (handles np object arrays)."""
        if isinstance(X, np.ndarray) and X.dtype == object:
            return list(X)
        return list(X)

    def fit(self, X, y=None):
        """Estimate geometric mean from training time series.

        Parameters
        ----------
        X : list of ndarray or object array
            Each element is (n_timepoints, n_regions).
        """
        from nilearn.connectome import ConnectivityMeasure

        self.conn_ = ConnectivityMeasure(
            kind="tangent",
            vectorize=self.vectorize,
            discard_diagonal=self.discard_diagonal,
        )
        self.conn_.fit(self._to_list(X))
        return self

    def transform(self, X):
        """Project time series into tangent space.

        Parameters
        ----------
        X : list of ndarray or object array
            Each element is (n_timepoints, n_regions).

        Returns
        -------
        ndarray of shape (n_subjects, n_features)
        """
        return self.conn_.transform(self._to_list(X))


def _load_timeseries_and_labels(
    conn_dir: Path,
    dataset_filter: str | None = None,
) -> tuple[list[np.ndarray], np.ndarray, np.ndarray, np.ndarray]:
    """Load all available time series and matching labels.

    Returns
    -------
    timeseries : list of ndarray
        Per-subject time series arrays.
    labels : ndarray
        Binary labels (1=ASD, 2=TC).
    sites : ndarray
        Site labels (strings).
    pids : ndarray
        Participant IDs.
    """
    participants_tsv = conn_dir / "participants.tsv"
    participants = pd.read_csv(participants_tsv, sep="\t")

    if dataset_filter:
        participants = participants[
            participants["source_dataset"] == dataset_filter
        ].copy()

    timeseries = []
    labels = []
    sites = []
    pids = []

    for _, row in participants.iterrows():
        pid = row["participant_id"]
        dx = row.get("dx_group")

        # Skip subjects without diagnosis
        if pd.isna(dx):
            continue

        ts_file = find_timeseries_path(pid, conn_dir=conn_dir)
        if ts_file is None:
            continue

        ts = read_timeseries_parquet(ts_file)
        if ts.shape[1] != MSDL_N_REGIONS:
            continue

        timeseries.append(ts)
        labels.append(int(dx))
        sites.append(row["source_site"])
        pids.append(pid)

    return (
        timeseries,
        np.array(labels),
        np.array(sites),
        np.array(pids),
    )


def run_intersite_cv(
    timeseries: list[np.ndarray],
    labels: np.ndarray,
    sites: np.ndarray,
    classifier_name: str = "ridge",
) -> dict:
    """Leave-one-site-out cross-validation.

    Returns
    -------
    dict
        Results with per-site and mean accuracy.
    """
    if classifier_name == "ridge":
        clf = RidgeClassifier()
    elif classifier_name == "svc_linear":
        clf = SVC(kernel="linear")
    else:
        raise ValueError(f"Unknown classifier: {classifier_name}")

    pipe = Pipeline([
        ("tangent", TangentEmbeddingTransformer()),
        ("classifier", clf),
    ])

    logo = LeaveOneGroupOut()
    unique_sites = np.unique(sites)
    n_sites = len(unique_sites)

    logger.info(
        "Inter-site CV: %d subjects, %d sites, classifier=%s",
        len(labels), n_sites, classifier_name,
    )

    per_site_accuracy = {}
    all_preds = np.zeros_like(labels)
    all_tested = np.zeros(len(labels), dtype=bool)

    # Use a dummy array for split() to avoid ragged-array issues with numpy
    dummy_X = np.arange(len(timeseries))
    for fold_idx, (train_idx, test_idx) in enumerate(logo.split(dummy_X, labels, sites)):
        site_name = sites[test_idx[0]]
        train_ts = [timeseries[i] for i in train_idx]
        test_ts = [timeseries[i] for i in test_idx]

        pipe.fit(train_ts, labels[train_idx])
        preds = pipe.predict(test_ts)
        acc = (preds == labels[test_idx]).mean()
        per_site_accuracy[site_name] = float(acc)
        all_preds[test_idx] = preds
        all_tested[test_idx] = True

        logger.info(
            "  Fold %d/%d site=%s n_test=%d acc=%.3f",
            fold_idx + 1, n_sites, site_name, len(test_idx), acc,
        )

    mean_accuracy = float(np.mean(list(per_site_accuracy.values())))
    overall_accuracy = float((all_preds[all_tested] == labels[all_tested]).mean())

    logger.info(
        "Inter-site: mean_per_site=%.3f overall=%.3f",
        mean_accuracy, overall_accuracy,
    )

    return {
        "cv_scheme": "leave_one_site_out",
        "classifier": classifier_name,
        "n_subjects": int(len(labels)),
        "n_sites": int(n_sites),
        "mean_accuracy_per_site": mean_accuracy,
        "overall_accuracy": overall_accuracy,
        "per_site_accuracy": per_site_accuracy,
        "per_site_n_subjects": {
            site: int((sites == site).sum()) for site in unique_sites
        },
    }


def run_intrasite_cv(
    timeseries: list[np.ndarray],
    labels: np.ndarray,
    sites: np.ndarray,
    classifier_name: str = "ridge",
    n_splits: int = 100,
    test_size: float = 0.2,
    random_state: int = 42,
) -> dict:
    """Within-site stratified shuffle split cross-validation.

    Returns
    -------
    dict
        Per-site median accuracy, overall summary.
    """
    if classifier_name == "ridge":
        clf = RidgeClassifier()
    elif classifier_name == "svc_linear":
        clf = SVC(kernel="linear")
    else:
        raise ValueError(f"Unknown classifier: {classifier_name}")

    pipe = Pipeline([
        ("tangent", TangentEmbeddingTransformer()),
        ("classifier", clf),
    ])

    unique_sites = np.unique(sites)
    per_site_results = {}

    logger.info(
        "Intra-site CV: %d subjects, %d sites, classifier=%s",
        len(labels), len(unique_sites), classifier_name,
    )

    for site_name in unique_sites:
        site_mask = sites == site_name
        site_ts = [timeseries[i] for i, m in enumerate(site_mask) if m]
        site_labels = labels[site_mask]

        n_site = len(site_labels)
        n_asd = (site_labels == 1).sum()
        n_tc = (site_labels == 2).sum()

        if n_site < MIN_SUBJECTS_PER_SITE:
            logger.info("  Skipping %s: n=%d < %d", site_name, n_site, MIN_SUBJECTS_PER_SITE)
            continue
        if n_asd < MIN_PER_CLASS_INTRASITE or n_tc < MIN_PER_CLASS_INTRASITE:
            logger.info(
                "  Skipping %s: ASD=%d, TC=%d (need >= %d each)",
                site_name, n_asd, n_tc, MIN_PER_CLASS_INTRASITE,
            )
            continue

        sss = StratifiedShuffleSplit(
            n_splits=n_splits, test_size=test_size, random_state=random_state,
        )
        # Wrap list in object array to avoid ragged-array conversion issues
        site_ts_arr = np.empty(len(site_ts), dtype=object)
        for idx_s, ts_s in enumerate(site_ts):
            site_ts_arr[idx_s] = ts_s
        scores = cross_val_score(pipe, site_ts_arr, site_labels, cv=sss, scoring="accuracy")
        per_site_results[site_name] = {
            "n_subjects": int(n_site),
            "n_asd": int(n_asd),
            "n_tc": int(n_tc),
            "median_accuracy": float(np.median(scores)),
            "mean_accuracy": float(np.mean(scores)),
            "std_accuracy": float(np.std(scores)),
            "all_scores": scores.tolist(),
        }
        logger.info(
            "  %s: n=%d median=%.3f mean=%.3f",
            site_name, n_site, np.median(scores), np.mean(scores),
        )

    n_evaluated = len(per_site_results)
    if n_evaluated > 0:
        median_of_medians = float(
            np.median([r["median_accuracy"] for r in per_site_results.values()])
        )
    else:
        median_of_medians = float("nan")

    logger.info(
        "Intra-site: %d/%d sites evaluated, median_of_medians=%.3f",
        n_evaluated, len(unique_sites), median_of_medians,
    )

    return {
        "cv_scheme": "intra_site_stratified_shuffle_split",
        "classifier": classifier_name,
        "n_splits": n_splits,
        "test_size": test_size,
        "n_subjects_total": int(len(labels)),
        "n_sites_total": int(len(unique_sites)),
        "n_sites_evaluated": n_evaluated,
        "median_of_medians": median_of_medians,
        "per_site_results": per_site_results,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument(
        "--classifiers",
        nargs="+",
        default=["ridge", "svc_linear"],
        choices=["ridge", "svc_linear"],
    )
    parser.add_argument(
        "--experiments",
        nargs="+",
        default=["abide1", "both"],
        choices=["abide1", "abide2", "both"],
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    conn_dir = args.output_dir or (args.project_root / "derivatives" / "connectivity")
    class_dir = conn_dir / "classification"
    class_dir.mkdir(parents=True, exist_ok=True)

    for experiment in args.experiments:
        dataset_filter = experiment if experiment != "both" else None
        exp_label = experiment

        logger.info("=" * 60)
        logger.info("Experiment: %s", exp_label)
        logger.info("=" * 60)

        timeseries, labels, sites, pids = _load_timeseries_and_labels(
            conn_dir, dataset_filter=dataset_filter,
        )

        if len(timeseries) == 0:
            logger.warning("No data for experiment '%s', skipping.", exp_label)
            continue

        logger.info(
            "Loaded %d subjects (ASD=%d, TC=%d, %d sites)",
            len(labels),
            (labels == 1).sum(),
            (labels == 2).sum(),
            len(np.unique(sites)),
        )

        for clf_name in args.classifiers:
            # Inter-site CV
            logger.info("--- Inter-site CV, classifier=%s ---", clf_name)
            intersite_results = run_intersite_cv(
                timeseries, labels, sites, classifier_name=clf_name,
            )
            out_file = class_dir / f"results_intersite_{exp_label}_{clf_name}.json"
            write_json(out_file, intersite_results)
            logger.info("Wrote %s", out_file)

            # Intra-site CV
            logger.info("--- Intra-site CV, classifier=%s ---", clf_name)
            intrasite_results = run_intrasite_cv(
                timeseries, labels, sites, classifier_name=clf_name,
            )
            out_file = class_dir / f"results_intrasite_{exp_label}_{clf_name}.json"
            write_json(out_file, intrasite_results)
            logger.info("Wrote %s", out_file)


if __name__ == "__main__":
    main()
