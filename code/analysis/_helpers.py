"""Shared utilities for the ABIDE replication analysis pipeline."""

from __future__ import annotations

import io
import json
import re
import zipfile
from pathlib import Path
from urllib.request import urlopen

import h5py
import nilearn
import numpy as np
import pandas as pd
import sklearn
from nilearn.connectome import ConnectivityMeasure
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.covariance import LedoitWolf
from sklearn.linear_model import LinearRegression

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

#: MSDL atlas has 39 regions.
N_MSDL_REGIONS = 39

#: Upper-triangle features for 39-region atlas (39 * 38 / 2).
N_TANGENT_FEATURES = N_MSDL_REGIONS * (N_MSDL_REGIONS - 1) // 2

#: QC thresholds (from Methods section).
MAX_MEAN_FD = 0.5        # mm
MIN_USABLE_VOLUMES = 120
MIN_ATLAS_COVERAGE = 0.80  # fraction

#: fMRIPrep output space used for the analysis.
SPACE = "MNI152NLin2009cAsym"

#: Fixed random seed for reproducibility.
RANDOM_STATE = 42

#: URL for Abraham et al. (2017) cross-validation splits.
CV_SPLITS_URL = "https://team.inria.fr/parietal/files/2016/04/cv_abide.zip"

#: Confounds strategy for nilearn's load_confounds.
CONFOUND_STRATEGY = ("motion", "compcor", "high_pass")
CONFOUND_MOTION = "full"  # 24 Friston parameters
CONFOUND_COMPCOR = "anat_combined"
CONFOUND_N_COMPCOR = 5

#: Low-pass filter (Hz) applied in NiftiMapsMasker.
LOW_PASS = 0.1

# --------------------------------------------------------------------------- #
# Path helpers
# --------------------------------------------------------------------------- #


def project_root() -> Path:
    """Return the project root (two levels up from code/analysis/)."""
    return Path(__file__).resolve().parent.parent.parent


def derivatives_fmriprep(root: Path | None = None) -> Path:
    """Return the fMRIPrep derivatives overlay path."""
    return (root or project_root()) / "derivatives" / "fmriprep-25.2"


def derivatives_connectivity(root: Path | None = None, variant: str = "v1") -> Path:
    """Return the connectivity derivatives path.

    Parameters
    ----------
    root : Path, optional
        Project root directory. Defaults to auto-detected root.
    variant : str
        Connectivity variant suffix (default ``"v1"``).
    """
    return (root or project_root()) / "derivatives" / f"connectivity-{variant}"


def participants_tsv(root: Path | None = None) -> Path:
    """Return the phenotypic participants.tsv path."""
    return (root or project_root()) / "inputs" / "abide-both" / "participants.tsv"


def exclusions_tsv(root: Path | None = None) -> Path:
    """Return the preprocessing exclusions list."""
    return (root or project_root()) / "lists" / "exclusions.tsv"


# --------------------------------------------------------------------------- #
# Subject / run discovery
# --------------------------------------------------------------------------- #

# Pattern: sub-{id}_ses-1_task-rest[_acq-*]_run-{N}_desc-confounds_timeseries.tsv
# The acq- entity is optional and present for some ABIDE II sites
# (e.g., acq-pedj for ONRC_2, acq-rc8chan/rc32chan for KKI_1).
_CONFOUNDS_RE = re.compile(
    r"^(?P<sub>sub-[^_]+)_ses-1_task-rest"
    r"(?:_(?P<acq>acq-[^_]+))?"
    r"_(?P<run>run-\d+)"
    r"_desc-confounds_timeseries\.tsv$"
)


def find_confounds(
    subject_id: str,
    fmriprep_dir: Path | None = None,
) -> list[tuple[str, Path]]:
    """Find all confounds TSV files for a subject, returning (run_label, path) pairs.

    The run_label includes the acq- entity when present (e.g., ``"acq-pedj_run-1"``),
    so that it can be used directly in BEP017 stems and path derivation.

    Sorted by run label (run-1 first).
    """
    fdir = (fmriprep_dir or derivatives_fmriprep()) / subject_id / "ses-1" / "func"
    results = []
    if not fdir.is_dir():
        return results
    for fp in sorted(fdir.iterdir()):
        m = _CONFOUNDS_RE.match(fp.name)
        if m:
            acq = m.group("acq")
            run = m.group("run")
            # Composite label preserving the acq- entity for path reconstruction
            label = f"{acq}_{run}" if acq else run
            results.append((label, fp))
    return results


def bold_path_from_confounds(confounds_path: Path) -> Path:
    """Derive the preprocessed BOLD NIfTI path from a confounds TSV path."""
    name = confounds_path.name.replace(
        "_desc-confounds_timeseries.tsv",
        f"_space-{SPACE}_desc-preproc_bold.nii.gz",
    )
    return confounds_path.parent / name


def brain_mask_from_confounds(confounds_path: Path) -> Path:
    """Derive the brain mask NIfTI path from a confounds TSV path."""
    name = confounds_path.name.replace(
        "_desc-confounds_timeseries.tsv",
        f"_space-{SPACE}_desc-brain_mask.nii.gz",
    )
    return confounds_path.parent / name


def bold_json_from_confounds(confounds_path: Path) -> Path:
    """Derive the BOLD JSON sidecar path from a confounds TSV path."""
    name = confounds_path.name.replace(
        "_desc-confounds_timeseries.tsv",
        f"_space-{SPACE}_desc-preproc_bold.json",
    )
    return confounds_path.parent / name


def get_tr(confounds_path: Path) -> float:
    """Read the repetition time from the BOLD JSON sidecar."""
    json_path = bold_json_from_confounds(confounds_path)
    with open(json_path) as f:
        return float(json.load(f)["RepetitionTime"])


# --------------------------------------------------------------------------- #
# BEP017 output naming
# --------------------------------------------------------------------------- #


def bep017_stem(subject_id: str, run_label: str) -> str:
    """Return the BEP017-compliant file stem (without suffix/extension).

    Example: sub-v1s0x0050642_ses-1_task-rest_run-1_space-MNI152NLin2009cAsym_atlas-MSDL
    """
    return (
        f"{subject_id}_ses-1_task-rest_{run_label}"
        f"_space-{SPACE}_atlas-MSDL"
    )


def output_dir(
    subject_id: str,
    connectivity_dir: Path | None = None,
) -> Path:
    """Return the per-subject BEP017 output directory."""
    d = (connectivity_dir or derivatives_connectivity()) / subject_id / "ses-1" / "func"
    d.mkdir(parents=True, exist_ok=True)
    return d


# --------------------------------------------------------------------------- #
# Subject listing
# --------------------------------------------------------------------------- #


def load_participants(root: Path | None = None):
    """Load participants.tsv as a pandas DataFrame."""
    return pd.read_csv(participants_tsv(root), sep="\t")


def load_exclusions(root: Path | None = None):
    """Load exclusions.tsv as a set of participant_ids."""
    df = pd.read_csv(exclusions_tsv(root), sep="\t")
    return set(df["participant_id"])


def eligible_subjects(root: Path | None = None):
    """Return a DataFrame of subjects eligible for analysis.

    Excludes preprocessing failures and subjects without a diagnostic group.
    """
    df = load_participants(root)
    excl = load_exclusions(root)
    df = df[~df["participant_id"].isin(excl)]
    df = df[df["group"].isin(["ASD", "TC"])]
    return df.reset_index(drop=True)


# --------------------------------------------------------------------------- #
# Site prefix extraction
# --------------------------------------------------------------------------- #

_SITE_PREFIX_RE = re.compile(r"^sub-(v[12]s\d+)x")


def site_prefix(subject_id: str) -> str:
    """Extract site prefix (e.g., 'v1s0') from a subject ID."""
    m = _SITE_PREFIX_RE.match(subject_id)
    if not m:
        raise ValueError(f"Cannot extract site prefix from {subject_id!r}")
    return m.group(1)


# --------------------------------------------------------------------------- #
# Tangent embedding transformer
# --------------------------------------------------------------------------- #


class TangentEmbeddingTransformer(BaseEstimator, TransformerMixin):
    """Sklearn-compatible tangent embedding wrapper.

    Re-estimates the geometric mean from training data during fit() to
    prevent information leakage in cross-validation.

    Parameters
    ----------
    assume_centered : bool
        Passed to :class:`~sklearn.covariance.LedoitWolf`.
    """

    def __init__(self, assume_centered=False):
        self.assume_centered = assume_centered

    def fit(self, X, y=None):
        self._conn = ConnectivityMeasure(
            cov_estimator=LedoitWolf(assume_centered=self.assume_centered),
            kind="tangent",
            vectorize=True,
            discard_diagonal=True,
        )
        self._conn.fit(X)
        return self

    def transform(self, X):
        return self._conn.transform(X)


# --------------------------------------------------------------------------- #
# Confound regression
# --------------------------------------------------------------------------- #


def regress_confounds(X_train, X_test, confounds_train, confounds_test):
    """Regress out confounds (site, age, sex) from tangent features.

    Fits on training data only to avoid leakage.
    """
    reg = LinearRegression().fit(confounds_train, X_train)
    X_train_clean = X_train - reg.predict(confounds_train)
    X_test_clean = X_test - reg.predict(confounds_test)
    return X_train_clean, X_test_clean


# --------------------------------------------------------------------------- #
# Abraham CV splits
# --------------------------------------------------------------------------- #


def fetch_abraham_cv_splits(data_dir: Path | None = None) -> dict:
    """Download and parse Abraham's 10-fold CV splits.

    The file is a wide CSV with columns: subsamble, then pairs of
    (train, test) columns for each fold and CV scheme. We extract the
    ``folds_loso`` columns (inter-site leave-one-site-out, 10 folds).

    Returns a dict mapping subject_id (int) -> fold_index (0-9),
    where fold_index is the fold in which the subject is in the TEST set.
    """
    cache_path = (data_dir or Path.home() / "nilearn_data") / "cv_abide"
    csv_path = cache_path / "cv_abide.csv"

    if not csv_path.exists():
        print(f"  Downloading CV splits from {CV_SPLITS_URL}...", flush=True)
        cache_path.mkdir(parents=True, exist_ok=True)
        response = urlopen(CV_SPLITS_URL)
        # The URL serves a CSV directly (despite .zip extension in some references)
        data = response.read()
        # Try zip first, fall back to raw CSV
        try:
            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                zf.extractall(cache_path)
        except zipfile.BadZipFile:
            csv_path.write_bytes(data)
        print("  Downloaded.", flush=True)
    else:
        print("  CV splits already cached.", flush=True)

    # Parse the wide CSV
    # Row 0: header with column group names (subsamble, folds_sss, folds_loso, ...)
    # Row 1: iter numbers (0,0,1,1,...,9,9)
    # Row 2: type (train, test, train, test, ...)
    # Row 3: empty
    # Rows 4+: subject_id, 0/1, 0/1, ... (1=in this set, 0=not)
    df = pd.read_csv(csv_path, header=None)

    # Find the folds_loso columns (inter-site CV)
    header = df.iloc[0].values
    iters = df.iloc[1].values
    types = df.iloc[2].values

    # Find column indices for folds_loso test sets
    loso_test_cols = {}  # fold_idx -> column_index
    for col_idx in range(1, len(header)):
        if str(header[col_idx]) == "folds_loso" and str(types[col_idx]) == "test":
            fold_idx = int(iters[col_idx])
            loso_test_cols[fold_idx] = col_idx

    print(f"  Found {len(loso_test_cols)} LOSO test folds", flush=True)

    # Build subject -> fold mapping
    subject_to_fold = {}
    for row_idx in range(4, len(df)):
        sub_id = df.iloc[row_idx, 0]
        if pd.isna(sub_id) or str(sub_id).strip() == "":
            continue
        sub_id = int(float(sub_id))
        for fold_idx, col_idx in loso_test_cols.items():
            val = df.iloc[row_idx, col_idx]
            if not pd.isna(val) and int(float(val)) == 1:
                subject_to_fold[sub_id] = fold_idx
                break

    n_folds = len(set(subject_to_fold.values())) if subject_to_fold else 0
    print(f"  Mapped {len(subject_to_fold)} subjects to {n_folds} folds", flush=True)
    return subject_to_fold


# --------------------------------------------------------------------------- #
# Software versions
# --------------------------------------------------------------------------- #


def software_versions() -> dict:
    """Collect software version strings."""
    return {
        "nilearn": nilearn.__version__,
        "scikit-learn": sklearn.__version__,
        "numpy": np.__version__,
        "pandas": pd.__version__,
        "h5py": h5py.__version__,
    }
