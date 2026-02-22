"""Shared utilities for ABIDE connectivity analysis pipeline."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import h5py
import numpy as np
import pandas as pd

logger = logging.getLogger("abide_analysis")

# -- Project layout constants --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
INPUTS_DIR = PROJECT_ROOT / "inputs"
ABIDE_BOTH = INPUTS_DIR / "abide-both"
FMRIPREP_DIR = PROJECT_ROOT / "derivatives" / "fmriprep-25.2"
CONNECTIVITY_DIR = PROJECT_ROOT / "derivatives" / "connectivity"
PARTICIPANTS_TSV = ABIDE_BOTH / "participants.tsv"

# MSDL atlas parameters
MSDL_N_REGIONS = 39
MSDL_N_FEATURES = MSDL_N_REGIONS * (MSDL_N_REGIONS - 1) // 2  # 741

# fMRIPrep output space
SPACE = "MNI152NLin2009cAsym"

# QC thresholds
MAX_MEAN_FD = 0.5  # mm
MIN_VOLUMES_AFTER_SCRUB = 120
MIN_ATLAS_COVERAGE = 0.80


# -- Phenotypic data ----------------------------------------------------------

def load_participants(
    phenotypic_tsv: Path | None = None,
) -> pd.DataFrame:
    """Load the analysis-ready phenotypic table.

    Parameters
    ----------
    phenotypic_tsv : Path, optional
        Explicit path. Defaults to ``derivatives/connectivity/participants.tsv``.

    Returns
    -------
    pd.DataFrame
        Indexed by ``participant_id``.
    """
    if phenotypic_tsv is None:
        phenotypic_tsv = CONNECTIVITY_DIR / "participants.tsv"
    df = pd.read_csv(phenotypic_tsv, sep="\t")
    df = df.set_index("participant_id")
    return df


# -- BIDS / BEP017 path builders ----------------------------------------------

def fmriprep_bold_path(
    participant_id: str,
    ses: str = "1",
    task: str = "rest",
    run: str | None = "1",
    space: str = SPACE,
    fmriprep_dir: Path = FMRIPREP_DIR,
) -> Path:
    """Return the expected fMRIPrep preprocessed BOLD NIfTI path."""
    sub = participant_id.replace("sub-", "")
    entities = f"sub-{sub}_ses-{ses}_task-{task}"
    if run is not None:
        entities += f"_run-{run}"
    entities += f"_space-{space}_desc-preproc_bold.nii.gz"
    return fmriprep_dir / f"sub-{sub}" / f"ses-{ses}" / "func" / entities


def fmriprep_bold_json(bold_path: Path) -> Path:
    """Return the JSON sidecar path for a BOLD NIfTI."""
    return bold_path.with_name(bold_path.name.replace(".nii.gz", ".json"))


def fmriprep_confounds_path(
    participant_id: str,
    ses: str = "1",
    task: str = "rest",
    run: str | None = "1",
    fmriprep_dir: Path = FMRIPREP_DIR,
) -> Path:
    """Return the fMRIPrep confounds TSV path."""
    sub = participant_id.replace("sub-", "")
    entities = f"sub-{sub}_ses-{ses}_task-{task}"
    if run is not None:
        entities += f"_run-{run}"
    entities += "_desc-confounds_timeseries.tsv"
    return fmriprep_dir / f"sub-{sub}" / f"ses-{ses}" / "func" / entities


def fmriprep_brain_mask_path(
    participant_id: str,
    ses: str = "1",
    task: str = "rest",
    run: str | None = "1",
    space: str = SPACE,
    fmriprep_dir: Path = FMRIPREP_DIR,
) -> Path:
    """Return the fMRIPrep brain mask path."""
    sub = participant_id.replace("sub-", "")
    entities = f"sub-{sub}_ses-{ses}_task-{task}"
    if run is not None:
        entities += f"_run-{run}"
    entities += f"_space-{space}_desc-brain_mask.nii.gz"
    return fmriprep_dir / f"sub-{sub}" / f"ses-{ses}" / "func" / entities


def connectivity_subject_dir(
    participant_id: str,
    ses: str = "1",
    conn_dir: Path = CONNECTIVITY_DIR,
) -> Path:
    """Return the per-subject connectivity output directory."""
    sub = participant_id.replace("sub-", "")
    return conn_dir / f"sub-{sub}" / f"ses-{ses}" / "func"


def bep017_stem(
    participant_id: str,
    ses: str = "1",
    task: str = "rest",
    run: str | None = "1",
    space: str = SPACE,
    atlas: str = "MSDL",
) -> str:
    """Build the BEP017 entity stem (no stat/suffix)."""
    sub = participant_id.replace("sub-", "")
    parts = [f"sub-{sub}", f"ses-{ses}", f"task-{task}"]
    if run is not None:
        parts.append(f"run-{run}")
    parts.extend([f"space-{space}", f"atlas-{atlas}"])
    return "_".join(parts)


def timeseries_path(participant_id: str, **kwargs: Any) -> Path:
    """BEP017 path for MSDL time series (parquet)."""
    conn_dir = kwargs.pop("conn_dir", CONNECTIVITY_DIR)
    stem = bep017_stem(participant_id, **kwargs)
    return (
        connectivity_subject_dir(participant_id, ses=kwargs.get("ses", "1"), conn_dir=conn_dir)
        / f"{stem}_stat-mean_timeseries.parquet"
    )


def timeseries_json_path(participant_id: str, **kwargs: Any) -> Path:
    """BEP017 path for time series JSON sidecar."""
    conn_dir = kwargs.pop("conn_dir", CONNECTIVITY_DIR)
    stem = bep017_stem(participant_id, **kwargs)
    return (
        connectivity_subject_dir(participant_id, ses=kwargs.get("ses", "1"), conn_dir=conn_dir)
        / f"{stem}_stat-mean_timeseries.json"
    )


def coverage_path(participant_id: str, **kwargs: Any) -> Path:
    """BEP017 path for atlas coverage TSV."""
    conn_dir = kwargs.pop("conn_dir", CONNECTIVITY_DIR)
    stem = bep017_stem(participant_id, **kwargs)
    return (
        connectivity_subject_dir(participant_id, ses=kwargs.get("ses", "1"), conn_dir=conn_dir)
        / f"{stem}_stat-coverage_bold.tsv"
    )


def pearson_relmat_path(participant_id: str, **kwargs: Any) -> Path:
    """BEP017 path for per-subject Pearson correlation matrix (HDF5)."""
    conn_dir = kwargs.pop("conn_dir", CONNECTIVITY_DIR)
    stem = bep017_stem(participant_id, **kwargs)
    return (
        connectivity_subject_dir(participant_id, ses=kwargs.get("ses", "1"), conn_dir=conn_dir)
        / f"{stem}_stat-pearsoncorrelation_relmat.h5"
    )


def tangent_relmat_path(participant_id: str, **kwargs: Any) -> Path:
    """BEP017 path for per-subject tangent projection matrix (HDF5)."""
    conn_dir = kwargs.pop("conn_dir", CONNECTIVITY_DIR)
    stem = bep017_stem(participant_id, **kwargs)
    return (
        connectivity_subject_dir(participant_id, ses=kwargs.get("ses", "1"), conn_dir=conn_dir)
        / f"{stem}_stat-tangent_relmat.h5"
    )


def find_timeseries_path(participant_id: str, **kwargs: Any) -> Path | None:
    """Find existing time series file, trying run="1" then run=None."""
    # Try with run="1" (default)
    path = timeseries_path(participant_id, **{**kwargs, "run": kwargs.get("run", "1")})
    if path.exists():
        return path
    # Try without run entity
    path = timeseries_path(participant_id, **{**kwargs, "run": None})
    if path.exists():
        return path
    return None


# -- I/O helpers ---------------------------------------------------------------

def write_timeseries_parquet(
    path: Path,
    data: np.ndarray,
    region_labels: list[str],
) -> None:
    """Write ROI time series to Parquet (T x n_regions)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(data, columns=region_labels)
    df.to_parquet(path, index=False, engine="pyarrow")


def read_timeseries_parquet(path: Path) -> np.ndarray:
    """Read ROI time series from Parquet and return as numpy array."""
    df = pd.read_parquet(path, engine="pyarrow")
    return df.values


def write_relmat_h5(
    path: Path,
    matrix: np.ndarray,
    region_labels: list[str],
    measure: str,
    extra_attrs: dict[str, Any] | None = None,
) -> None:
    """Write a relationship matrix to HDF5 with metadata attributes."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with h5py.File(path, "w") as f:
        ds = f.create_dataset(
            "matrix",
            data=matrix.astype(np.float64),
            compression="gzip",
            compression_opts=4,
        )
        ds.attrs["regions"] = region_labels
        ds.attrs["measure"] = measure
        if extra_attrs:
            for k, v in extra_attrs.items():
                ds.attrs[k] = v


def read_relmat_h5(path: Path) -> tuple[np.ndarray, dict[str, Any]]:
    """Read a relationship matrix from HDF5. Returns (matrix, attrs_dict)."""
    with h5py.File(path, "r") as f:
        matrix = f["matrix"][:]
        attrs = dict(f["matrix"].attrs)
    return matrix, attrs


def write_coverage_tsv(
    path: Path,
    coverage: np.ndarray,
    region_labels: list[str],
) -> None:
    """Write per-ROI coverage fractions to TSV."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame([coverage], columns=region_labels)
    df.to_csv(path, sep="\t", index=False, float_format="%.4f")


def write_json(path: Path, data: dict[str, Any]) -> None:
    """Write a JSON sidecar."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)
        f.write("\n")


def read_json(path: Path) -> dict[str, Any]:
    """Read a JSON file."""
    with open(path) as f:
        return json.load(f)


# -- QC helpers ----------------------------------------------------------------

def get_tr_from_sidecar(bold_path: Path) -> float:
    """Extract RepetitionTime from fMRIPrep BOLD JSON sidecar."""
    json_path = fmriprep_bold_json(bold_path)
    if not json_path.exists():
        raise FileNotFoundError(f"BOLD JSON sidecar not found: {json_path}")
    meta = read_json(json_path)
    tr = meta.get("RepetitionTime")
    if tr is None:
        raise ValueError(f"RepetitionTime not found in {json_path}")
    return float(tr)


def compute_atlas_coverage(
    brain_mask_path: Path,
    atlas_maps_img: Any,
) -> np.ndarray:
    """Compute fraction of each atlas region covered by the BOLD brain mask.

    Parameters
    ----------
    brain_mask_path : Path
        Path to the fMRIPrep brain mask NIfTI.
    atlas_maps_img : Nifti1Image
        MSDL atlas maps (4D probabilistic).

    Returns
    -------
    np.ndarray
        Coverage fraction for each region (n_regions,).
    """
    import nibabel as nib
    from nilearn.image import resample_to_img

    mask_img = nib.load(brain_mask_path)
    atlas_resampled = resample_to_img(
        atlas_maps_img, mask_img, interpolation="continuous"
    )
    mask_data = mask_img.get_fdata() > 0
    atlas_data = atlas_resampled.get_fdata()

    n_regions = atlas_data.shape[-1]
    coverage = np.zeros(n_regions)
    for i in range(n_regions):
        region_mask = atlas_data[..., i] > 0.1  # threshold prob map
        if region_mask.sum() == 0:
            coverage[i] = 0.0
        else:
            coverage[i] = (region_mask & mask_data).sum() / region_mask.sum()
    return coverage
