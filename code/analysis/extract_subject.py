#!/usr/bin/env python3
"""Unified per-subject MSDL time series extraction.

Extracts time series for one subject across all requested variants in a
single pass, sharing the BOLD load across variants. Runs on ALL subjects
regardless of QC -- filtering happens at the analysis step.

Variants:
  v1: Single-stage ROI-level confound regression
  v2: v1 + 5 high-variance voxel PCs
  v3: Two-stage denoising (voxel aCompCor → ROI tCompCor)

For C-PAC source (--source cpac), iterates over all 871 PCP subjects
and writes to connectivity-cpac/.

Usage::

    # fMRIPrep (per-subject, called from SLURM array)
    python code/analysis/extract_subject.py \\
        --project-root . --participant-id sub-v1s0x0050642 --variants v1 v2 v3

    # C-PAC (all subjects in one call)
    python code/analysis/extract_subject.py \\
        --project-root . --source cpac --data-dir /path/to/cache
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

VARIANT_DESCRIPTIONS = {
    "v1": "single_stage_roi_level",
    "v2": "single_stage_roi_level_plus_high_variance",
    "v3": "two_stage_acompcor_then_tcompcor",
    "cpac": "cpac_preprocessed",
}


# --------------------------------------------------------------------------- #
# Atlas coverage
# --------------------------------------------------------------------------- #

def compute_atlas_coverage(mask_path, atlas_maps_img):
    mask_img = nib.load(str(mask_path))
    atlas_resampled = resample_to_img(atlas_maps_img, mask_img, interpolation="continuous")
    atlas_data = atlas_resampled.get_fdata()
    mask_data = mask_img.get_fdata().astype(bool)
    coverage = np.zeros(N_MSDL_REGIONS)
    for i in range(N_MSDL_REGIONS):
        region_map = atlas_data[..., i]
        total = np.abs(region_map).sum()
        if total > 0:
            coverage[i] = np.abs(region_map[mask_data]).sum() / total
    return coverage


# --------------------------------------------------------------------------- #
# Per-variant extraction functions
# --------------------------------------------------------------------------- #

def _extract_v1(bold_path, mask_path, conf_path, tr, masker):
    confounds, sample_mask = load_confounds(
        str(bold_path), strategy=CONFOUND_STRATEGY, motion=CONFOUND_MOTION,
        compcor=CONFOUND_COMPCOR, n_compcor=CONFOUND_N_COMPCOR, demean=True,
    )
    ts = masker.fit_transform(str(bold_path), confounds=confounds, sample_mask=sample_mask)
    extra = {"ConfoundStrategy": VARIANT_DESCRIPTIONS["v1"], "Confounds": list(CONFOUND_STRATEGY)}
    return ts, extra


def _extract_v2(bold_path, mask_path, conf_path, tr, masker):
    confounds, sample_mask = load_confounds(
        str(bold_path), strategy=CONFOUND_STRATEGY, motion=CONFOUND_MOTION,
        compcor=CONFOUND_COMPCOR, n_compcor=CONFOUND_N_COMPCOR, demean=True,
    )
    bold_data = nib.load(str(bold_path)).get_fdata()
    mask_data = nib.load(str(mask_path)).get_fdata().astype(bool)
    voxel_ts = bold_data[mask_data].T
    if sample_mask is not None:
        voxel_ts = voxel_ts[sample_mask]
    hvc = high_variance_confounds(voxel_ts, n_confounds=5, percentile=2.0)
    hv_df = pd.DataFrame(hvc, columns=[f"hv_comp_{i:02d}" for i in range(hvc.shape[1])],
                         index=confounds.index if sample_mask is None else confounds.index[sample_mask])
    confounds_combined = pd.concat([confounds, hv_df], axis=1)
    del bold_data, voxel_ts
    ts = masker.fit_transform(str(bold_path), confounds=confounds_combined, sample_mask=sample_mask)
    extra = {"ConfoundStrategy": VARIANT_DESCRIPTIONS["v2"],
             "Confounds": list(CONFOUND_STRATEGY) + ["high_variance"],
             "HighVarianceNConfounds": 5, "HighVariancePercentile": 2.0}
    return ts, extra


def _extract_v3(bold_path, mask_path, conf_path, tr, atlas):
    confounds_s1, sample_mask = load_confounds(
        str(bold_path), strategy=CONFOUND_STRATEGY, motion=CONFOUND_MOTION,
        compcor=CONFOUND_COMPCOR, n_compcor=CONFOUND_N_COMPCOR, demean=True,
    )
    bold_clean = clean_img(str(bold_path), confounds=confounds_s1, low_pass=LOW_PASS,
                           high_pass=0.01, t_r=tr, detrend=True, mask_img=str(mask_path))
    masker_clean = NiftiMapsMasker(maps_img=atlas.maps, standardize="zscore_sample",
                                   detrend=False, low_pass=None, high_pass=None, t_r=tr)
    ts = masker_clean.fit_transform(bold_clean)
    conf_tsv = pd.read_csv(conf_path, sep="\t")
    tcols = sorted([c for c in conf_tsv.columns if c.startswith("t_comp_cor_")])[:CONFOUND_N_COMPCOR]
    if tcols:
        ts = clean(ts, confounds=conf_tsv[tcols].values, detrend=False, standardize="zscore_sample")
    extra = {"ConfoundStrategy": VARIANT_DESCRIPTIONS["v3"],
             "Stage1Confounds": list(CONFOUND_STRATEGY), "Stage2Confounds": ["temporal_compcor"]}
    return ts, extra


_EXTRACT_FN = {"v1": _extract_v1, "v2": _extract_v2, "v3": _extract_v3}


# --------------------------------------------------------------------------- #
# Output writing
# --------------------------------------------------------------------------- #

def write_outputs(ts, subject_id, run_label, variant, conn_dir, tr, coverage, region_labels, extra_sidecar):
    odir = output_dir(subject_id, conn_dir)
    stem = bep017_stem(subject_id, run_label)

    # Parquet
    pd.DataFrame(ts, columns=region_labels).to_parquet(
        odir / f"{stem}_stat-mean_timeseries.parquet", index=False)

    # JSON sidecar
    sidecar = {
        "RepetitionTime": tr, "NumberOfVolumes": int(ts.shape[0]),
        "Atlas": "MSDL", "NumberOfRegions": N_MSDL_REGIONS, "Variant": variant,
        **extra_sidecar,
        "ConfoundMotion": CONFOUND_MOTION, "ConfoundCompCor": CONFOUND_COMPCOR,
        "ConfoundNCompCor": CONFOUND_N_COMPCOR, "LowPassHz": LOW_PASS,
        "Standardize": "zscore_sample",
        "MeanAtlasCoverage": round(float(coverage.mean()), 4),
        "SelectedRun": run_label,
        "SoftwareVersions": software_versions(),
        "Timestamp": datetime.now(timezone.utc).isoformat(),
    }
    with open(odir / f"{stem}_stat-mean_timeseries.json", "w") as f:
        json.dump(sidecar, f, indent=2)

    # Coverage TSV
    pd.DataFrame({"region": region_labels, "coverage": coverage}).to_csv(
        odir / f"{stem}_stat-coverage_bold.tsv", sep="\t", index=False)

    # Pearson correlation HDF5
    corr = np.corrcoef(ts.T)
    with h5py.File(odir / f"{stem}_stat-pearsoncorrelation_relmat.h5", "w") as hf:
        ds = hf.create_dataset("matrix", data=corr, compression="gzip")
        ds.attrs["regions"] = region_labels
        ds.attrs["measure"] = "pearson_correlation"
        ds.attrs["atlas"] = "MSDL"
        ds.attrs["n_regions"] = N_MSDL_REGIONS
        ds.attrs["tr"] = tr


# --------------------------------------------------------------------------- #
# fMRIPrep extraction (per subject, multiple variants)
# --------------------------------------------------------------------------- #

def extract_fmriprep_subject(subject_id, project_root, variants, run_label=None):
    fmriprep_dir = derivatives_fmriprep(project_root)

    runs = find_confounds(subject_id, fmriprep_dir)
    if not runs:
        print(f"  SKIP: {subject_id} -- no confounds found", flush=True)
        return

    # Select run: use specified, or first available (no QC-based selection at extraction)
    if run_label:
        conf_path = None
        for rl, cp in runs:
            if rl == run_label:
                conf_path = cp
                break
        if conf_path is None:
            print(f"  SKIP: {subject_id} -- run {run_label} not found", flush=True)
            return
        selected_run = run_label
    else:
        selected_run, conf_path = runs[0]

    bold = bold_path_from_confounds(conf_path)
    mask = brain_mask_from_confounds(conf_path)
    if not bold.exists():
        print(f"  SKIP: {subject_id} -- BOLD not available (needs datalad get)", flush=True)
        return
    if not mask.exists():
        print(f"  SKIP: {subject_id} -- brain mask not found", flush=True)
        return

    tr = get_tr(conf_path)
    atlas = fetch_atlas_msdl()
    atlas_maps_img = nib.load(atlas.maps)
    region_labels = list(atlas.labels)

    # Coverage (shared across variants)
    coverage = compute_atlas_coverage(mask, atlas_maps_img)

    # Build masker for v1/v2
    masker = NiftiMapsMasker(maps_img=atlas.maps, standardize="zscore_sample",
                             detrend=False, low_pass=LOW_PASS, high_pass=None, t_r=tr)

    for variant in variants:
        conn_dir = derivatives_connectivity(project_root, variant=variant)
        try:
            if variant in ("v1", "v2"):
                ts, extra = _EXTRACT_FN[variant](bold, mask, conf_path, tr, masker)
            else:  # v3
                ts, extra = _extract_v3(bold, mask, conf_path, tr, atlas)

            if ts.shape[1] != N_MSDL_REGIONS:
                print(f"  WARN: {subject_id} {variant} -- {ts.shape[1]} regions", flush=True)
                continue

            write_outputs(ts, subject_id, selected_run, variant, conn_dir, tr, coverage, region_labels, extra)
            print(f"  {subject_id} {variant}: {ts.shape[0]} volumes, coverage={coverage.mean():.3f}", flush=True)
        except Exception as e:
            print(f"  FAIL: {subject_id} {variant}: {e}", flush=True)


# --------------------------------------------------------------------------- #
# C-PAC extraction (all subjects in one call)
# --------------------------------------------------------------------------- #

def extract_cpac_all(project_root, data_dir=None):
    from nilearn.datasets import fetch_abide_pcp

    print("Fetching ABIDE PCP (C-PAC)...", flush=True)
    abide = fetch_abide_pcp(data_dir=data_dir, pipeline="cpac", band_pass_filtering=True,
                            global_signal_regression=False, derivatives=["func_preproc"],
                            quality_checked=True, verbose=1)
    phenotypic = abide.phenotypic
    print(f"  {len(abide.func_preproc)} subjects", flush=True)

    atlas = fetch_atlas_msdl()
    region_labels = list(atlas.labels)
    masker = NiftiMapsMasker(maps_img=atlas.maps, standardize="zscore_sample", detrend=True)
    conn_dir = derivatives_connectivity(project_root, variant="cpac")

    extracted = 0
    for i, func in enumerate(abide.func_preproc):
        sub_id = str(int(phenotypic["SUB_ID"].iloc[i])).zfill(7)
        sub_label = f"sub-{sub_id}"

        try:
            ts = masker.fit_transform(func)
            if ts.shape[1] != N_MSDL_REGIONS:
                continue

            odir_path = conn_dir / sub_label / "ses-1" / "func"
            odir_path.mkdir(parents=True, exist_ok=True)
            stem = f"{sub_label}_ses-1_task-rest_run-1_space-MNI152_atlas-MSDL"

            pd.DataFrame(ts, columns=region_labels).to_parquet(
                odir_path / f"{stem}_stat-mean_timeseries.parquet", index=False)

            sidecar = {
                "Atlas": "MSDL", "NumberOfRegions": N_MSDL_REGIONS,
                "NumberOfVolumes": int(ts.shape[0]),
                "Pipeline": "cpac", "Variant": "cpac",
                "ConfoundStrategy": VARIANT_DESCRIPTIONS["cpac"],
                "BandPassFiltering": True, "GlobalSignalRegression": False,
                "Standardize": "zscore_sample", "Detrend": True,
                "SoftwareVersions": software_versions(),
                "Timestamp": datetime.now(timezone.utc).isoformat(),
            }
            with open(odir_path / f"{stem}_stat-mean_timeseries.json", "w") as f:
                json.dump(sidecar, f, indent=2)

            extracted += 1
        except Exception as e:
            print(f"  FAIL: {sub_label}: {e}", flush=True)

        if (i + 1) % 50 == 0:
            print(f"  {i + 1}/{len(abide.func_preproc)} ({extracted} extracted)", flush=True)

    print(f"  Done: {extracted} extracted", flush=True)


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--project-root", type=Path, default=Path("."))
    parser.add_argument("--participant-id", default=None,
                        help="Subject ID (for fMRIPrep source). Required unless --source cpac.")
    parser.add_argument("--variants", nargs="+", default=["v1", "v2", "v3"],
                        choices=["v1", "v2", "v3"],
                        help="Extraction variants (default: v1 v2 v3).")
    parser.add_argument("--run", default=None, help="Run label override.")
    parser.add_argument("--source", default="fmriprep", choices=["fmriprep", "cpac"],
                        help="Data source (default: fmriprep).")
    parser.add_argument("--data-dir", type=str, default=None,
                        help="Cache directory for PCP downloads (cpac source only).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Check subject availability without extracting.")
    args = parser.parse_args()
    root = args.project_root.resolve()

    if args.source == "cpac":
        extract_cpac_all(root, data_dir=args.data_dir)
    else:
        if not args.participant_id:
            print("ERROR: --participant-id required for fMRIPrep source", file=sys.stderr, flush=True)
            sys.exit(1)

        if args.dry_run:
            fmriprep_dir = derivatives_fmriprep(root)
            runs = find_confounds(args.participant_id, fmriprep_dir)
            if runs:
                rl, cp = runs[0]
                bold = bold_path_from_confounds(cp)
                available = bold.exists() and bold.stat().st_size > 1000
                print(f"{args.participant_id}: {len(runs)} run(s), BOLD={'available' if available else 'needs staging'}", flush=True)
            else:
                print(f"{args.participant_id}: no confounds found", flush=True)
            return

        extract_fmriprep_subject(args.participant_id, root, args.variants, run_label=args.run)


if __name__ == "__main__":
    main()
