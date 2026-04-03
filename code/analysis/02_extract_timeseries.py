#!/usr/bin/env python3
"""Per-subject time-series extraction with MSDL atlas.

Loads fMRIPrep BOLD + confounds for one subject's selected run, applies
confound regression and low-pass filtering, extracts MSDL atlas time series,
computes atlas coverage and Pearson correlation.

Usage::

    python code/analysis/02_extract_timeseries.py \\
        --project-root . --participant-id sub-v1s0x0050642 --run run-1

Reads the selected run from qc_prescreen.tsv if --run is not specified.

Output (BEP017 naming in derivatives/connectivity/):
  - *_atlas-MSDL_stat-mean_timeseries.parquet
  - *_atlas-MSDL_stat-mean_timeseries.json
  - *_atlas-MSDL_stat-coverage_bold.tsv
  - *_atlas-MSDL_stat-pearsoncorrelation_relmat.h5
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
from nilearn.image import resample_to_img
from nilearn.interfaces.fmriprep import load_confounds
from nilearn.maskers import NiftiMapsMasker


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
    MIN_ATLAS_COVERAGE,
    N_MSDL_REGIONS,
    SPACE,
    bep017_stem,
    bold_path_from_confounds,
    brain_mask_from_confounds,
    derivatives_connectivity,
    derivatives_fmriprep,
    find_confounds,
    get_tr,
    output_dir,
    software_versions,
)


def compute_atlas_coverage(mask_path: Path, atlas_maps_img) -> np.ndarray:
    """Compute per-region coverage of BOLD mask over MSDL atlas.

    Returns a 1-D array of length N_MSDL_REGIONS with coverage fractions.
    """
    mask_img = nib.load(str(mask_path))

    # Resample atlas to mask grid
    atlas_resampled = resample_to_img(
        atlas_maps_img, mask_img, interpolation="continuous"
    )
    atlas_data = atlas_resampled.get_fdata()
    mask_data = mask_img.get_fdata().astype(bool)

    coverage = np.zeros(N_MSDL_REGIONS)
    for i in range(N_MSDL_REGIONS):
        region_map = atlas_data[..., i]
        # Weighted coverage: fraction of atlas weight inside mask
        total_weight = np.abs(region_map).sum()
        if total_weight > 0:
            masked_weight = np.abs(region_map[mask_data]).sum()
            coverage[i] = masked_weight / total_weight
    return coverage


def extract_timeseries(
    subject_id: str,
    run_label: str,
    project_root: Path,
) -> dict:
    """Extract MSDL time series for one subject/run.

    Returns a dict with status info (pass/fail, output paths, QC metrics).
    """
    fmriprep_dir = derivatives_fmriprep(project_root)
    conn_dir = derivatives_connectivity(project_root)

    # Find the confounds file for the selected run
    runs = find_confounds(subject_id, fmriprep_dir)
    conf_path = None
    for rl, cp in runs:
        if rl == run_label:
            conf_path = cp
            break

    if conf_path is None:
        return {"status": "error", "reason": f"No confounds for {run_label}"}

    bold = bold_path_from_confounds(conf_path)
    mask = brain_mask_from_confounds(conf_path)

    if not bold.exists():
        return {"status": "error", "reason": "BOLD file not found (not fetched?)"}
    if not mask.exists():
        return {"status": "error", "reason": "Brain mask not found"}

    # Get TR from JSON sidecar
    tr = get_tr(conf_path)

    # Fetch MSDL atlas
    atlas = fetch_atlas_msdl()
    atlas_maps_img = nib.load(atlas.maps)
    region_labels = list(atlas.labels)

    # Check atlas coverage
    coverage = compute_atlas_coverage(mask, atlas_maps_img)
    mean_coverage = float(coverage.mean())

    if mean_coverage < MIN_ATLAS_COVERAGE:
        return {
            "status": "excluded",
            "reason": f"low_coverage ({mean_coverage:.3f} < {MIN_ATLAS_COVERAGE})",
            "mean_coverage": mean_coverage,
        }

    # --- Two-stage denoising (replicating Abraham et al. 2017) ---
    #
    # Stage 1 (voxel-level): regress 24 motion + 5 aCompCor + cosine HP,
    #          apply band-pass filter.  Mimics C-PAC voxel-level cleaning.
    # Stage 2 (ROI-level):   extract MSDL time series from clean BOLD,
    #          then regress 5 tCompCor (high-variance voxel PCs).
    #          Mimics Abraham Section 2.3.

    # Stage 1: load aCompCor confounds and clean at voxel level
    confounds_stage1, sample_mask = load_confounds(
        str(bold),
        strategy=CONFOUND_STRATEGY,
        motion=CONFOUND_MOTION,
        compcor=CONFOUND_COMPCOR,
        n_compcor=CONFOUND_N_COMPCOR,
        demean=True,
    )

    from nilearn.image import clean_img
    bold_clean = clean_img(
        str(bold),
        confounds=confounds_stage1,
        low_pass=LOW_PASS,
        high_pass=0.01,
        t_r=tr,
        detrend=True,
        mask_img=str(mask),
    )

    # Stage 2: extract ROI time series from clean BOLD
    masker = NiftiMapsMasker(
        maps_img=atlas.maps,
        standardize="zscore_sample",
        detrend=False,
        low_pass=None,   # already applied in stage 1
        high_pass=None,   # already applied in stage 1
        t_r=tr,
    )
    timeseries = masker.fit_transform(bold_clean)

    # Stage 2b: regress tCompCor from ROI signals (Abraham Section 2.3)
    # Read tCompCor columns directly from fMRIPrep's confounds TSV
    # (load_confounds requires high_pass with compcor, which we don't want here)
    confounds_tsv = pd.read_csv(conf_path, sep="\t")
    tcompcor_cols = [c for c in confounds_tsv.columns if c.startswith("t_comp_cor_")]
    tcompcor_cols = sorted(tcompcor_cols)[:CONFOUND_N_COMPCOR]
    confounds_stage2 = confounds_tsv[tcompcor_cols].values

    from nilearn.signal import clean
    timeseries = clean(
        timeseries,
        confounds=confounds_stage2,
        detrend=False,
        standardize="zscore_sample",
    )

    # Compute per-subject Pearson correlation
    correlation = np.corrcoef(timeseries.T)  # (39, 39)

    # --- Write outputs ---
    odir = output_dir(subject_id, conn_dir)
    stem = bep017_stem(subject_id, run_label)

    # 1. Time series (parquet)
    ts_df = pd.DataFrame(timeseries, columns=region_labels)
    ts_path = odir / f"{stem}_stat-mean_timeseries.parquet"
    ts_df.to_parquet(ts_path, index=False)

    # 2. Time series sidecar (JSON)
    sidecar = {
        "RepetitionTime": tr,
        "NumberOfVolumes": int(timeseries.shape[0]),
        "NumberOfVolumesDiscarded": int(confounds_stage1.shape[0] - timeseries.shape[0]) if sample_mask is not None else 0,
        "Atlas": "MSDL",
        "NumberOfRegions": N_MSDL_REGIONS,
        "ConfoundStrategy": "two_stage_acompcor_then_tcompcor",
        "Stage1Confounds": list(CONFOUND_STRATEGY),
        "Stage2Confounds": ["temporal_compcor"],
        "ConfoundMotion": CONFOUND_MOTION,
        "ConfoundCompCor": CONFOUND_COMPCOR,
        "ConfoundNCompCor": CONFOUND_N_COMPCOR,
        "LowPassHz": LOW_PASS,
        "Standardize": "zscore_sample",
        "MeanAtlasCoverage": round(mean_coverage, 4),
        "SelectedRun": run_label,
        "SoftwareVersions": software_versions(),
        "Timestamp": datetime.now(timezone.utc).isoformat(),
    }
    sidecar_path = odir / f"{stem}_stat-mean_timeseries.json"
    with open(sidecar_path, "w") as f:
        json.dump(sidecar, f, indent=2)

    # 3. Coverage (TSV)
    cov_df = pd.DataFrame(
        {"region": region_labels, "coverage": coverage}
    )
    cov_path = odir / f"{stem}_stat-coverage_bold.tsv"
    cov_df.to_csv(cov_path, sep="\t", index=False)

    # 4. Pearson correlation (HDF5)
    corr_path = odir / f"{stem}_stat-pearsoncorrelation_relmat.h5"
    with h5py.File(corr_path, "w") as hf:
        ds = hf.create_dataset("matrix", data=correlation, compression="gzip")
        ds.attrs["regions"] = region_labels
        ds.attrs["measure"] = "pearson_correlation"
        ds.attrs["atlas"] = "MSDL"
        ds.attrs["n_regions"] = N_MSDL_REGIONS
        ds.attrs["tr"] = tr

    return {
        "status": "pass",
        "n_volumes": int(timeseries.shape[0]),
        "mean_coverage": round(mean_coverage, 4),
        "timeseries_path": str(ts_path),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=Path("."))
    parser.add_argument("--participant-id", required=True)
    parser.add_argument(
        "--run",
        default=None,
        help="Run label (e.g., run-1). If omitted, reads from qc_prescreen.tsv.",
    )
    args = parser.parse_args()
    root = args.project_root.resolve()
    sub_id = args.participant_id

    run_label = args.run
    if run_label is None:
        # Read from pre-screen output
        qc_path = derivatives_connectivity(root) / "qc_prescreen.tsv"
        qc_df = pd.read_csv(qc_path, sep="\t")
        row = qc_df[qc_df["participant_id"] == sub_id]
        if row.empty:
            print(f"ERROR: {sub_id} not found in {qc_path}", file=sys.stderr)
            sys.exit(1)
        row = row.iloc[0]
        if row["excluded_reason"] != "pass":
            print(f"SKIP: {sub_id} excluded ({row['excluded_reason']})")
            sys.exit(0)
        run_label = row["selected_run"]

    print(f"Extracting: {sub_id} {run_label}")
    result = extract_timeseries(sub_id, run_label, root)
    print(f"  Status: {result['status']}")
    if result["status"] == "pass":
        print(f"  Volumes: {result['n_volumes']}, Coverage: {result['mean_coverage']}")
    elif "reason" in result:
        print(f"  Reason: {result['reason']}")


if __name__ == "__main__":
    main()
