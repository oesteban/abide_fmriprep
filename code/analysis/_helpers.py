"""Shared utilities for the ABIDE replication analysis pipeline."""

from __future__ import annotations

import json
import re
from pathlib import Path

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


def derivatives_connectivity(root: Path | None = None) -> Path:
    """Return the connectivity derivatives path."""
    return (root or project_root()) / "derivatives" / "connectivity"


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
    import pandas as pd

    return pd.read_csv(participants_tsv(root), sep="\t")


def load_exclusions(root: Path | None = None):
    """Load exclusions.tsv as a set of participant_ids."""
    import pandas as pd

    df = pd.read_csv(exclusions_tsv(root), sep="\t")
    return set(df["participant_id"])


def eligible_subjects(root: Path | None = None):
    """Return a DataFrame of subjects eligible for analysis.

    Excludes preprocessing failures and subjects without a diagnostic group.
    """
    import pandas as pd

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
