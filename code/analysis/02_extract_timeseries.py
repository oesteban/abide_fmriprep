#!/usr/bin/env python3
"""Per-subject time-series extraction with MSDL atlas.

Supports three extraction variants:
  v1: Single-stage ROI-level confound regression (24 Friston + 5 aCompCor
      + cosine HP + 0.1 Hz low-pass via NiftiMapsMasker).
  v2: v1 + 5 high-variance voxel PCs (top 2% variance) as additional
      confound regressors.
  v3: Two-stage denoising -- voxel-level clean_img (aCompCor + band-pass),
      then ROI extraction + tCompCor regression (Abraham Section 2.3).

Usage::

    python code/analysis/02_extract_timeseries.py \\
        --project-root . --participant-id sub-v1s0x0050642 --variant v1

Reads the selected run from qc_prescreen.tsv if --run is not specified.
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
from nilearn.image import clean_img, resample_to_img
from nilearn.interfaces.fmriprep import load_confounds
from nilearn.maskers import NiftiMapsMasker
from nilearn.signal import clean, high_variance_confounds


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

#: Extraction variant descriptions (for sidecar metadata).
VARIANT_DESCRIPTIONS = {
    "v1": "single_stage_roi_level",
    "v2": "single_stage_roi_level_plus_high_variance",
    "v3": "two_stage_acompcor_then_tcompcor",
}


def compute_atlas_coverage(mask_path: Path, atlas_maps_img) -> np.ndarray:
    """Compute per-region coverage of BOLD mask over MSDL atlas."""
    mask_img = nib.load(str(mask_path))
    atlas_resampled = resample_to_img(
        atlas_maps_img, mask_img, interpolation="continuous"
    )
    atlas_data = atlas_resampled.get_fdata()
    mask_data = mask_img.get_fdata().astype(bool)

    coverage = np.zeros(N_MSDL_REGIONS)
    for i in range(N_MSDL_REGIONS):
        region_map = atlas_data[..., i]
        total_weight = np.abs(region_map).sum()
        if total_weight > 0:
            coverage[i] = np.abs(region_map[mask_data]).sum() / total_weight
    return coverage


# --------------------------------------------------------------------------- #
# Extraction variants
# --------------------------------------------------------------------------- #


def _extract_v1(bold, mask, conf_path, tr, atlas, masker):
    """v1: Single-stage ROI-level confound regression."""
    confounds, sample_mask = load_confounds(
        str(bold),
        strategy=CONFOUND_STRATEGY,
        motion=CONFOUND_MOTION,
        compcor=CONFOUND_COMPCOR,
        n_compcor=CONFOUND_N_COMPCOR,
        demean=True,
    )
    timeseries = masker.fit_transform(
        str(bold), confounds=confounds, sample_mask=sample_mask
    )
    sidecar_extra = {
        "ConfoundStrategy": VARIANT_DESCRIPTIONS["v1"],
        "Confounds": list(CONFOUND_STRATEGY),
    }
    return timeseries, sidecar_extra


def _extract_v2(bold, mask, conf_path, tr, atlas, masker):
    """v2: v1 + high-variance voxel confounds."""
    confounds, sample_mask = load_confounds(
        str(bold),
        strategy=CONFOUND_STRATEGY,
        motion=CONFOUND_MOTION,
        compcor=CONFOUND_COMPCOR,
        n_compcor=CONFOUND_N_COMPCOR,
        demean=True,
    )

    # Compute high-variance voxel PCs
    bold_img = nib.load(str(bold))
    mask_img = nib.load(str(mask))
    bold_data = bold_img.get_fdata()
    mask_data = mask_img.get_fdata().astype(bool)
    voxel_ts = bold_data[mask_data].T
    if sample_mask is not None:
        voxel_ts = voxel_ts[sample_mask]
    hv_confounds = high_variance_confounds(voxel_ts, n_confounds=5, percentile=2.0)

    hv_df = pd.DataFrame(
        hv_confounds,
        columns=[f"hv_comp_{i:02d}" for i in range(hv_confounds.shape[1])],
        index=confounds.index if sample_mask is None else confounds.index[sample_mask],
    )
    confounds_combined = pd.concat([confounds, hv_df], axis=1)
    del bold_data, voxel_ts

    timeseries = masker.fit_transform(
        str(bold), confounds=confounds_combined, sample_mask=sample_mask
    )
    sidecar_extra = {
        "ConfoundStrategy": VARIANT_DESCRIPTIONS["v2"],
        "Confounds": list(CONFOUND_STRATEGY) + ["high_variance"],
        "HighVarianceNConfounds": 5,
        "HighVariancePercentile": 2.0,
    }
    return timeseries, sidecar_extra


def _extract_v3(bold, mask, conf_path, tr, atlas, masker):
    """v3: Two-stage denoising (Abraham et al. 2017, Section 2.3)."""
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

    # Stage 2: extract from clean BOLD (no filtering -- already done)
    masker_clean = NiftiMapsMasker(
        maps_img=atlas.maps,
        standardize="zscore_sample",
        detrend=False,
        low_pass=None,
        high_pass=None,
        t_r=tr,
    )
    timeseries = masker_clean.fit_transform(bold_clean)

    # Stage 2b: regress tCompCor
    confounds_tsv = pd.read_csv(conf_path, sep="\t")
    tcompcor_cols = sorted(
        [c for c in confounds_tsv.columns if c.startswith("t_comp_cor_")]
    )[:CONFOUND_N_COMPCOR]
    if tcompcor_cols:
        timeseries = clean(
            timeseries,
            confounds=confounds_tsv[tcompcor_cols].values,
            detrend=False,
            standardize="zscore_sample",
        )

    sidecar_extra = {
        "ConfoundStrategy": VARIANT_DESCRIPTIONS["v3"],
        "Stage1Confounds": list(CONFOUND_STRATEGY),
        "Stage2Confounds": ["temporal_compcor"],
    }
    return timeseries, sidecar_extra


_EXTRACT_FN = {"v1": _extract_v1, "v2": _extract_v2, "v3": _extract_v3}


# --------------------------------------------------------------------------- #
# Main extraction
# --------------------------------------------------------------------------- #


def extract_timeseries(
    subject_id: str,
    run_label: str,
    project_root: Path,
    variant: str = "v1",
) -> dict:
    """Extract MSDL time series for one subject/run."""
    fmriprep_dir = derivatives_fmriprep(project_root)
    conn_dir = derivatives_connectivity(project_root, variant=variant)

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

    tr = get_tr(conf_path)

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

    # Build masker for v1/v2 (v3 builds its own internally)
    masker = NiftiMapsMasker(
        maps_img=atlas.maps,
        standardize="zscore_sample",
        detrend=False,
        low_pass=LOW_PASS,
        high_pass=None,  # handled by cosine confound regressors
        t_r=tr,
    )

    # Run the selected extraction variant
    extract_fn = _EXTRACT_FN[variant]
    timeseries, sidecar_extra = extract_fn(bold, mask, conf_path, tr, atlas, masker)

    # Compute per-subject Pearson correlation
    correlation = np.corrcoef(timeseries.T)

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
        "Atlas": "MSDL",
        "NumberOfRegions": N_MSDL_REGIONS,
        "Variant": variant,
        **sidecar_extra,
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
    cov_df = pd.DataFrame({"region": region_labels, "coverage": coverage})
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
        "--variant",
        choices=("v1", "v2", "v3"),
        default="v1",
        help="Extraction variant (default: v1).",
    )
    parser.add_argument(
        "--run",
        default=None,
        help="Run label (e.g., run-1). If omitted, reads from qc_prescreen.tsv.",
    )
    args = parser.parse_args()
    root = args.project_root.resolve()
    sub_id = args.participant_id
    variant = args.variant

    run_label = args.run
    if run_label is None:
        qc_path = derivatives_connectivity(root, variant=variant) / "qc_prescreen.tsv"
        if not qc_path.exists():
            # Fall back to default connectivity dir
            qc_path = derivatives_connectivity(root) / "qc_prescreen.tsv"
        qc_df = pd.read_csv(qc_path, sep="\t")
        row = qc_df[qc_df["participant_id"] == sub_id]
        if row.empty:
            print(f"ERROR: {sub_id} not found in {qc_path}", file=sys.stderr, flush=True)
            sys.exit(1)
        row = row.iloc[0]
        if row["excluded_reason"] != "pass":
            print(f"SKIP: {sub_id} excluded ({row['excluded_reason']})", flush=True)
            sys.exit(0)
        run_label = row["selected_run"]

    print(f"Extracting: {sub_id} {run_label} (variant {variant})", flush=True)
    result = extract_timeseries(sub_id, run_label, root, variant=variant)
    print(f"  Status: {result['status']}", flush=True)
    if result["status"] == "pass":
        print(f"  Volumes: {result['n_volumes']}, Coverage: {result['mean_coverage']}", flush=True)
    elif "reason" in result:
        print(f"  Reason: {result['reason']}", flush=True)


if __name__ == "__main__":
    main()
