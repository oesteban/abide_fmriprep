#!/usr/bin/env python3
"""Per-subject time series extraction from fMRIPrep outputs.

For each subject, loads fMRIPrep preprocessed BOLD, applies confound
regression (24 motion + 5 anat CompCor + cosine high-pass), low-pass
filtering at 0.1 Hz, and extracts MSDL atlas (39 regions) time series.

Usage::

    # Single subject
    python code/analysis/01_extract_timeseries.py --participant-id sub-v1s0x0050642

    # All subjects
    python code/analysis/01_extract_timeseries.py --all

    # Specific subject list
    python code/analysis/01_extract_timeseries.py --participants-file lists/batch1.txt

Outputs per subject (BEP017 naming)::

    derivatives/connectivity/sub-{id}/ses-1/func/
        ..._atlas-MSDL_stat-mean_timeseries.parquet
        ..._atlas-MSDL_stat-mean_timeseries.json
        ..._atlas-MSDL_stat-coverage_bold.tsv
        ..._atlas-MSDL_stat-pearsoncorrelation_relmat.h5
"""

from __future__ import annotations

import argparse
import logging
import sys
import traceback
from pathlib import Path

import nilearn
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from analysis._helpers import (
    CONNECTIVITY_DIR,
    FMRIPREP_DIR,
    MAX_MEAN_FD,
    MIN_ATLAS_COVERAGE,
    MIN_VOLUMES_AFTER_SCRUB,
    MSDL_N_REGIONS,
    PARTICIPANTS_TSV,
    PROJECT_ROOT,
    SPACE,
    compute_atlas_coverage,
    coverage_path,
    fmriprep_bold_path,
    fmriprep_brain_mask_path,
    get_tr_from_sidecar,
    pearson_relmat_path,
    timeseries_json_path,
    timeseries_path,
    write_coverage_tsv,
    write_json,
    write_relmat_h5,
    write_timeseries_parquet,
)

logger = logging.getLogger(__name__)


def _get_msdl_atlas():
    """Fetch MSDL atlas and return (maps_img, region_labels)."""
    from nilearn.datasets import fetch_atlas_msdl

    atlas = fetch_atlas_msdl()
    import nibabel as nib

    maps_img = nib.load(atlas.maps)
    region_labels = list(atlas.labels)
    return maps_img, region_labels


def extract_timeseries_single(
    participant_id: str,
    fmriprep_dir: Path = FMRIPREP_DIR,
    output_dir: Path = CONNECTIVITY_DIR,
    ses: str = "1",
    task: str = "rest",
    run: str | None = "1",
    overwrite: bool = False,
) -> dict:
    """Extract MSDL time series for a single subject.

    Returns
    -------
    dict
        Status report with keys: participant_id, status, message, [qc_metrics].
    """
    from nilearn.image import load_img
    from nilearn.interfaces.fmriprep import load_confounds
    from nilearn.maskers import NiftiMapsMasker

    report = {"participant_id": participant_id, "status": "skipped", "message": ""}

    # Check if output already exists
    ts_out = timeseries_path(participant_id, ses=ses, task=task, run=run, conn_dir=output_dir)
    if ts_out.exists() and not overwrite:
        report["message"] = "Output already exists"
        return report

    # -- Locate fMRIPrep outputs --
    bold_file = fmriprep_bold_path(
        participant_id, ses=ses, task=task, run=run, fmriprep_dir=fmriprep_dir,
    )
    if not bold_file.exists():
        # Try without run entity (some subjects may not have run-1)
        bold_file = fmriprep_bold_path(
            participant_id, ses=ses, task=task, run=None, fmriprep_dir=fmriprep_dir,
        )
        if not bold_file.exists():
            report["status"] = "failed"
            report["message"] = f"BOLD file not found: {bold_file}"
            return report
        run = None  # Update for downstream path builders

    mask_file = fmriprep_brain_mask_path(
        participant_id, ses=ses, task=task, run=run, fmriprep_dir=fmriprep_dir,
    )

    # -- Get TR --
    try:
        tr = get_tr_from_sidecar(bold_file)
    except (FileNotFoundError, ValueError) as e:
        report["status"] = "failed"
        report["message"] = f"TR extraction failed: {e}"
        return report

    # -- Load confounds --
    try:
        confounds, sample_mask = load_confounds(
            bold_file,
            strategy=("motion", "compcor", "high_pass"),
            motion="full",
            compcor="anat_combined",
            n_compcor=5,
            demean=True,
        )
    except Exception as e:
        report["status"] = "failed"
        report["message"] = f"Confound loading failed: {e}"
        return report

    # -- QC: mean framewise displacement --
    try:
        confounds_tsv = bold_file.parent / bold_file.name.replace(
            f"_space-{SPACE}_desc-preproc_bold.nii.gz",
            "_desc-confounds_timeseries.tsv",
        )
        confounds_full = pd.read_csv(confounds_tsv, sep="\t")
        fd = confounds_full["framewise_displacement"]
        mean_fd = fd.mean()
        if mean_fd > MAX_MEAN_FD:
            report["status"] = "excluded_qc"
            report["message"] = f"Mean FD={mean_fd:.3f} > {MAX_MEAN_FD}"
            return report
    except Exception as e:
        logger.warning("Could not compute mean FD for %s: %s", participant_id, e)
        mean_fd = np.nan

    # -- QC: volumes after scrubbing --
    img = load_img(bold_file)
    n_volumes_original = img.shape[-1]
    if sample_mask is not None:
        n_volumes = len(sample_mask)
    else:
        n_volumes = n_volumes_original

    if n_volumes < MIN_VOLUMES_AFTER_SCRUB:
        report["status"] = "excluded_qc"
        report["message"] = (
            f"Only {n_volumes} volumes after scrubbing "
            f"(minimum: {MIN_VOLUMES_AFTER_SCRUB})"
        )
        return report

    # -- Fetch MSDL atlas --
    atlas_maps_img, region_labels = _get_msdl_atlas()

    # -- QC: atlas coverage --
    if mask_file.exists():
        coverage = compute_atlas_coverage(mask_file, atlas_maps_img)
        min_coverage = coverage.min()
        mean_coverage = coverage.mean()
        if mean_coverage < MIN_ATLAS_COVERAGE:
            report["status"] = "excluded_qc"
            report["message"] = (
                f"Mean atlas coverage={mean_coverage:.3f} < {MIN_ATLAS_COVERAGE}"
            )
            return report
    else:
        coverage = np.full(len(region_labels), np.nan)
        min_coverage = np.nan
        mean_coverage = np.nan
        logger.warning("Brain mask not found for %s, skipping coverage QC", participant_id)

    # -- Extract time series --
    masker = NiftiMapsMasker(
        maps_img=atlas_maps_img,
        standardize="zscore_sample",
        detrend=False,  # handled by cosine confound regressors
        low_pass=0.1,   # Abraham's band-pass upper bound
        high_pass=None,  # handled by cosine confound regressors
        t_r=tr,
        memory="nilearn_cache",
        memory_level=1,
    )

    try:
        ts = masker.fit_transform(
            bold_file,
            confounds=confounds,
            sample_mask=sample_mask,
        )
    except Exception as e:
        report["status"] = "failed"
        report["message"] = f"Time series extraction failed: {e}"
        return report

    assert ts.shape[1] == MSDL_N_REGIONS, (
        f"Expected {MSDL_N_REGIONS} regions, got {ts.shape[1]}"
    )

    # -- Compute Pearson correlation --
    from nilearn.connectome import ConnectivityMeasure

    corr_measure = ConnectivityMeasure(kind="correlation", vectorize=False)
    corr_matrix = corr_measure.fit_transform([ts])[0]

    # -- Write outputs --
    kwargs = {"ses": ses, "task": task, "run": run, "conn_dir": output_dir}

    # Time series (Parquet)
    write_timeseries_parquet(
        timeseries_path(participant_id, **kwargs), ts, region_labels,
    )

    # JSON sidecar
    qc_metrics = {
        "RepetitionTime": tr,
        "n_volumes_original": int(n_volumes_original),
        "n_volumes_after_scrub": n_volumes,
        "mean_framewise_displacement": float(mean_fd) if not np.isnan(mean_fd) else None,
        "mean_atlas_coverage": float(mean_coverage) if not np.isnan(mean_coverage) else None,
        "min_atlas_coverage": float(min_coverage) if not np.isnan(min_coverage) else None,
        "confound_model": {
            "motion": "full",
            "compcor": "anat_combined",
            "n_compcor": 5,
            "high_pass": "cosine_regressors",
            "low_pass_hz": 0.1,
            "standardize": "zscore_sample",
        },
        "atlas": "MSDL",
        "n_regions": MSDL_N_REGIONS,
        "space": SPACE,
        "nilearn_version": nilearn.__version__,
    }
    write_json(timeseries_json_path(participant_id, **kwargs), qc_metrics)

    # Coverage TSV
    write_coverage_tsv(
        coverage_path(participant_id, **kwargs), coverage, region_labels,
    )

    # Pearson correlation matrix (HDF5)
    write_relmat_h5(
        pearson_relmat_path(participant_id, **kwargs),
        corr_matrix,
        region_labels,
        measure="pearsoncorrelation",
        extra_attrs={"tr": tr, "n_volumes": n_volumes},
    )

    report["status"] = "success"
    report["message"] = f"{ts.shape[0]} timepoints x {ts.shape[1]} regions"
    report["qc_metrics"] = qc_metrics
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--participant-id", type=str, help="Single participant ID")
    group.add_argument("--all", action="store_true", help="Process all participants")
    group.add_argument(
        "--participants-file", type=Path, help="File with one participant ID per line",
    )
    parser.add_argument(
        "--project-root", type=Path, default=PROJECT_ROOT,
    )
    parser.add_argument(
        "--fmriprep-dir", type=Path, default=None,
        help="fMRIPrep derivatives directory (default: derivatives/fmriprep-25.2)",
    )
    parser.add_argument(
        "--output-dir", type=Path, default=None,
        help="Output directory (default: derivatives/connectivity)",
    )
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--ses", default="1")
    parser.add_argument("--task", default="rest")
    parser.add_argument("--run", default="1")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    fmriprep_dir = args.fmriprep_dir or (args.project_root / "derivatives" / "fmriprep-25.2")
    output_dir = args.output_dir or (args.project_root / "derivatives" / "connectivity")

    # Build subject list
    if args.participant_id:
        subjects = [args.participant_id]
    elif args.participants_file:
        subjects = [
            line.strip()
            for line in args.participants_file.read_text().splitlines()
            if line.strip() and not line.startswith("#")
        ]
    else:  # --all
        participants = pd.read_csv(
            args.project_root / "inputs" / "abide-both" / "participants.tsv",
            sep="\t",
        )
        subjects = participants["participant_id"].tolist()

    logger.info("Processing %d subjects", len(subjects))

    results = []
    for i, pid in enumerate(subjects, 1):
        logger.info("[%d/%d] Processing %s", i, len(subjects), pid)
        try:
            report = extract_timeseries_single(
                pid,
                fmriprep_dir=fmriprep_dir,
                output_dir=output_dir,
                ses=args.ses,
                task=args.task,
                run=args.run if args.run != "none" else None,
                overwrite=args.overwrite,
            )
        except Exception as e:
            report = {
                "participant_id": pid,
                "status": "error",
                "message": traceback.format_exc(),
            }
        results.append(report)
        logger.info("  -> %s: %s", report["status"], report["message"])

    # Summary
    df = pd.DataFrame(results)
    for status, count in df["status"].value_counts().items():
        logger.info("  %s: %d", status, count)

    # Write extraction report
    report_path = output_dir / "extraction_report.tsv"
    df[["participant_id", "status", "message"]].to_csv(
        report_path, sep="\t", index=False,
    )
    logger.info("Extraction report: %s", report_path)


if __name__ == "__main__":
    main()
