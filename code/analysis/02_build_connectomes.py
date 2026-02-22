#!/usr/bin/env python3
"""Build tangent connectomes from per-subject MSDL time series.

Loads all per-subject ``_stat-mean_timeseries.parquet`` files, computes
the tangent embedding (ConnectivityMeasure), and writes:

- Per-subject tangent relmat (HDF5) back into each subject's func/ dir
- Group-level stacked features (NPZ + JSON) for classification

Usage::

    python code/analysis/02_build_connectomes.py [--project-root .]

.. note::

    This whole-sample tangent embedding is for exploration and visualization.
    The classification script (03_classify.py) re-fits ConnectivityMeasure
    within each CV fold to avoid information leakage.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from analysis._helpers import (
    CONNECTIVITY_DIR,
    MSDL_N_FEATURES,
    MSDL_N_REGIONS,
    PROJECT_ROOT,
    load_participants,
    find_timeseries_path,
    read_timeseries_parquet,
    tangent_relmat_path,
    write_json,
    write_relmat_h5,
)

logger = logging.getLogger(__name__)


def build_connectomes(
    project_root: Path = PROJECT_ROOT,
    conn_dir: Path | None = None,
    dataset_filter: str | None = None,
) -> None:
    """Compute tangent connectomes for all available subjects."""
    from nilearn.connectome import ConnectivityMeasure
    from nilearn.datasets import fetch_atlas_msdl

    if conn_dir is None:
        conn_dir = project_root / "derivatives" / "connectivity"

    # Load phenotypic table
    participants = load_participants(conn_dir / "participants.tsv")
    atlas = fetch_atlas_msdl()
    region_labels = list(atlas.labels)

    # Optional dataset filter
    if dataset_filter:
        participants = participants[participants["source_dataset"] == dataset_filter]

    # Collect available time series
    all_timeseries = []
    valid_pids = []

    for pid in participants.index:
        ts_file = find_timeseries_path(pid, conn_dir=conn_dir)
        if ts_file is None:
            continue
        ts = read_timeseries_parquet(ts_file)
        if ts.shape[1] != MSDL_N_REGIONS:
            logger.warning(
                "Skipping %s: expected %d regions, got %d",
                pid, MSDL_N_REGIONS, ts.shape[1],
            )
            continue
        all_timeseries.append(ts)
        valid_pids.append(pid)

    n_subjects = len(all_timeseries)
    logger.info("Loaded time series for %d / %d subjects", n_subjects, len(participants))

    if n_subjects == 0:
        logger.error("No time series found. Run 01_extract_timeseries.py first.")
        return

    # -- Tangent embedding (whole-sample, for exploration) --
    # Fit once (non-vectorized) to get full matrices; vectorize manually.
    from nilearn.connectome import sym_matrix_to_vec

    conn_measure = ConnectivityMeasure(
        kind="tangent", vectorize=False, discard_diagonal=False,
    )
    tangent_matrices = conn_measure.fit_transform(all_timeseries)

    # Vectorize upper triangle (excluding diagonal) for group NPZ
    connectomes = np.array([
        sym_matrix_to_vec(mat, discard_diagonal=True) for mat in tangent_matrices
    ])
    assert connectomes.shape == (n_subjects, MSDL_N_FEATURES), (
        f"Expected ({n_subjects}, {MSDL_N_FEATURES}), got {connectomes.shape}"
    )

    # -- Write per-subject tangent relmat --
    for pid, mat in zip(valid_pids, tangent_matrices):
        out_path = tangent_relmat_path(pid, conn_dir=conn_dir)
        write_relmat_h5(
            out_path,
            mat,
            region_labels,
            measure="tangent",
            extra_attrs={
                "reference": "geometric_mean",
                "n_subjects_in_reference": n_subjects,
            },
        )

    logger.info("Wrote %d per-subject tangent relmat files", n_subjects)

    # -- Write group-level stacked features --
    group_dir = conn_dir / "group"
    group_dir.mkdir(parents=True, exist_ok=True)

    # Gather phenotypic info for valid subjects
    valid_participants = participants.loc[valid_pids]

    np.savez_compressed(
        group_dir / "group_atlas-MSDL_stat-tangent_relmat.npz",
        connectomes=connectomes,
        participant_ids=np.array(valid_pids),
        dx_group=valid_participants["dx_group"].values
        if "dx_group" in valid_participants.columns
        else np.full(n_subjects, np.nan),
        site_labels=valid_participants["source_site"].values,
        source_dataset=valid_participants["source_dataset"].values,
        age_at_scan=valid_participants["age_at_scan"].values
        if "age_at_scan" in valid_participants.columns
        else np.full(n_subjects, np.nan),
        sex=valid_participants["sex"].values
        if "sex" in valid_participants.columns
        else np.full(n_subjects, np.nan),
    )

    write_json(
        group_dir / "group_atlas-MSDL_stat-tangent_relmat.json",
        {
            "atlas": "MSDL",
            "n_regions": MSDL_N_REGIONS,
            "n_features": MSDL_N_FEATURES,
            "n_subjects": n_subjects,
            "measure": "tangent",
            "vectorize": True,
            "discard_diagonal": True,
            "datasets_included": sorted(valid_participants["source_dataset"].unique().tolist()),
            "sites_included": sorted(valid_participants["source_site"].unique().tolist()),
        },
    )
    logger.info("Wrote group-level tangent features: %s", group_dir)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project-root", type=Path, default=PROJECT_ROOT,
    )
    parser.add_argument(
        "--output-dir", type=Path, default=None,
        help="Connectivity derivatives directory",
    )
    parser.add_argument(
        "--dataset", type=str, default=None,
        choices=["abide1", "abide2"],
        help="Filter to a single dataset",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    build_connectomes(
        project_root=args.project_root,
        conn_dir=args.output_dir,
        dataset_filter=args.dataset,
    )


if __name__ == "__main__":
    main()
