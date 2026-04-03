#!/usr/bin/env python3
"""Build tangent connectomes from extracted time series (group-level exploration).

Loads all per-subject parquet time series, computes tangent embedding, and
writes per-subject tangent relmat files plus a group-level stacked feature
matrix.

Usage::

    python code/analysis/03_build_connectomes.py --project-root .

Note: The classification script (04_classify.py) re-fits the tangent
embedding within each CV fold.  This script produces the full-dataset
embedding for visualization and exploration only.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import h5py
import numpy as np
import pandas as pd
from nilearn.connectome import ConnectivityMeasure
from nilearn.datasets import fetch_atlas_msdl


def _setup_path():
    here = Path(__file__).resolve().parent
    if str(here) not in sys.path:
        sys.path.insert(0, str(here))


_setup_path()

from _helpers import (
    N_MSDL_REGIONS,
    N_TANGENT_FEATURES,
    bep017_stem,
    derivatives_connectivity,
    output_dir,
)


def load_all_timeseries(
    conn_dir: Path,
    qc_df: pd.DataFrame,
) -> tuple[list[np.ndarray], list[str]]:
    """Load parquet time series for all subjects that passed QC.

    Returns (list of T_i x 39 arrays, list of participant_ids).
    """
    timeseries_list = []
    subject_ids = []

    for _, row in qc_df.iterrows():
        if row["excluded_reason"] != "pass":
            continue
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
            print(f"WARNING: {sub_id} has {ts.shape[1]} regions, expected {N_MSDL_REGIONS}", flush=True)
            continue
        timeseries_list.append(ts)
        subject_ids.append(sub_id)

    return timeseries_list, subject_ids


def build_connectomes(project_root: Path, variant: str = "v1"):
    """Build tangent connectomes for all subjects."""
    conn_dir = derivatives_connectivity(project_root, variant=variant)
    qc_path = conn_dir / "qc_prescreen.tsv"
    qc_df = pd.read_csv(qc_path, sep="\t")

    print("Loading time series...", flush=True)
    timeseries_list, subject_ids = load_all_timeseries(conn_dir, qc_df)
    print(f"  Loaded {len(timeseries_list)} subjects", flush=True)

    if not timeseries_list:
        print("ERROR: No time series found. Run 02_extract_timeseries.py first.", flush=True)
        sys.exit(1)

    # Compute tangent embedding
    print("Computing tangent embedding...", flush=True)
    conn_measure = ConnectivityMeasure(
        kind="tangent", vectorize=True, discard_diagonal=True
    )
    connectomes = conn_measure.fit_transform(timeseries_list)  # (N, 741)
    print(f"  Feature matrix shape: {connectomes.shape}", flush=True)
    assert connectomes.shape[1] == N_TANGENT_FEATURES

    # Get the full tangent matrices (not vectorized) for per-subject output
    conn_full = ConnectivityMeasure(kind="tangent", vectorize=False)
    tangent_matrices = conn_full.fit_transform(timeseries_list)  # (N, 39, 39)

    # Get region labels
    atlas = fetch_atlas_msdl()
    region_labels = list(atlas.labels)

    # --- Per-subject tangent relmat ---
    print("Writing per-subject tangent relmat files...", flush=True)
    for i, sub_id in enumerate(subject_ids):
        row = qc_df[qc_df["participant_id"] == sub_id].iloc[0]
        run_label = row["selected_run"]
        stem = bep017_stem(sub_id, run_label)
        odir = output_dir(sub_id, conn_dir)
        relmat_path = odir / f"{stem}_stat-tangent_relmat.h5"

        with h5py.File(relmat_path, "w") as hf:
            ds = hf.create_dataset(
                "matrix", data=tangent_matrices[i], compression="gzip"
            )
            ds.attrs["regions"] = region_labels
            ds.attrs["measure"] = "tangent"
            ds.attrs["atlas"] = "MSDL"
            ds.attrs["n_regions"] = N_MSDL_REGIONS

    # --- Group-level stacked features ---
    print("Writing group-level feature matrix...", flush=True)
    group_dir = conn_dir / "group"
    group_dir.mkdir(parents=True, exist_ok=True)

    # Merge with phenotypic data for labels
    qc_pass = qc_df[qc_df["participant_id"].isin(subject_ids)].set_index("participant_id")
    qc_pass = qc_pass.loc[subject_ids]  # maintain order

    np.savez_compressed(
        group_dir / "group_atlas-MSDL_stat-tangent_relmat.npz",
        connectomes=connectomes,
        participant_ids=np.array(subject_ids),
        dx_group=qc_pass["group"].values,
        source_dataset=qc_pass["source_dataset"].values,
        source_site=qc_pass["source_site"].values,
    )

    # Metadata JSON
    meta = {
        "Atlas": "MSDL",
        "NumberOfRegions": N_MSDL_REGIONS,
        "NumberOfFeatures": N_TANGENT_FEATURES,
        "NumberOfSubjects": len(subject_ids),
        "ConnectivityMeasure": "tangent",
        "Vectorized": True,
        "DiscardDiagonal": True,
        "Timestamp": datetime.now(timezone.utc).isoformat(),
    }
    with open(group_dir / "group_atlas-MSDL_stat-tangent_relmat.json", "w") as f:
        json.dump(meta, f, indent=2)

    print(f"\nDone. {len(subject_ids)} subjects, {connectomes.shape[1]} features.", flush=True)
    print(f"  Group file: {group_dir / 'group_atlas-MSDL_stat-tangent_relmat.npz'}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=Path("."))
    parser.add_argument("--variant", default="v1", help="Connectivity variant.")
    args = parser.parse_args()
    build_connectomes(args.project_root.resolve(), variant=args.variant)


if __name__ == "__main__":
    main()
