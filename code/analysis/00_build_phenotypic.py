#!/usr/bin/env python3
"""Aggregate phenotypic data for ABIDE I+II analysis.

Merges diagnosis, demographics, and IQ from nilearn's cached ABIDE phenotypic
tables into the project's participants.tsv.

Usage::

    python code/analysis/00_build_phenotypic.py [--project-root .]

Outputs::

    derivatives/connectivity/participants.tsv
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Ensure the analysis package is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from analysis._helpers import (
    CONNECTIVITY_DIR,
    PARTICIPANTS_TSV,
    PROJECT_ROOT,
    write_json,
)

logger = logging.getLogger(__name__)


def _fetch_abide_phenotypic() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch ABIDE I and II phenotypic tables via nilearn.

    Returns
    -------
    abide1_pheno, abide2_pheno : pd.DataFrame
        Each with at least: SUB_ID, DX_GROUP, AGE_AT_SCAN, SEX, FIQ, SITE_ID.
    """
    from nilearn.datasets import fetch_abide_pcp

    logger.info("Fetching ABIDE I phenotypic data via nilearn...")
    abide1 = fetch_abide_pcp(pipeline="cpac", quality_checked=False)
    abide1_pheno = pd.DataFrame(abide1.phenotypic)

    # ABIDE II: nilearn's fetch_abide_pcp only covers ABIDE I.
    # Try fetch_abide2 if available (nilearn >= 0.11), otherwise fall back.
    abide2_pheno = pd.DataFrame()
    logger.info("Attempting ABIDE II phenotypic fetch...")
    try:
        from nilearn.datasets import fetch_abide2
        abide2 = fetch_abide2(pipeline="cpac", quality_checked=False)
        abide2_pheno = pd.DataFrame(abide2.phenotypic)
        logger.info("Loaded ABIDE II phenotypic: %d rows", len(abide2_pheno))
    except (ImportError, AttributeError, Exception) as exc:
        logger.warning(
            "Could not fetch ABIDE II phenotypic data (%s: %s). "
            "ABIDE II subjects will have NaN phenotypic fields.",
            type(exc).__name__, exc,
        )

    return abide1_pheno, abide2_pheno


def _normalize_phenotypic(
    pheno: pd.DataFrame,
    dataset: str,
) -> pd.DataFrame:
    """Normalize phenotypic table to common schema.

    Parameters
    ----------
    pheno : pd.DataFrame
        Raw phenotypic table from nilearn.
    dataset : str
        "abide1" or "abide2".

    Returns
    -------
    pd.DataFrame
        Columns: source_subject_id, dx_group, age_at_scan, sex, fiq, site_id.
    """
    if pheno.empty:
        return pd.DataFrame(
            columns=[
                "source_subject_id",
                "dx_group",
                "age_at_scan",
                "sex",
                "fiq",
                "site_id",
            ]
        )

    # Column names vary slightly between ABIDE I and II
    col_map = {}
    for target, candidates in {
        "source_subject_id": ["SUB_ID", "sub_id"],
        "dx_group": ["DX_GROUP", "dx_group"],
        "age_at_scan": ["AGE_AT_SCAN", "age_at_scan"],
        "sex": ["SEX", "sex"],
        "fiq": ["FIQ", "fiq"],
        "site_id": ["SITE_ID", "site_id"],
    }.items():
        for c in candidates:
            if c in pheno.columns:
                col_map[c] = target
                break

    df = pheno.rename(columns=col_map)

    # Keep only needed columns (some may be missing)
    needed = ["source_subject_id", "dx_group", "age_at_scan", "sex", "fiq", "site_id"]
    for col in needed:
        if col not in df.columns:
            df[col] = np.nan

    df = df[needed].copy()
    df["source_subject_id"] = df["source_subject_id"].astype(str)

    # dx_group: 1=ASD, 2=TC (ABIDE convention)
    # sex: 1=Male, 2=Female
    for col in ("dx_group", "sex"):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    df["age_at_scan"] = pd.to_numeric(df["age_at_scan"], errors="coerce")
    df["fiq"] = pd.to_numeric(df["fiq"], errors="coerce")

    # Replace sentinel values (-9999 etc.) with NaN
    for col in ("age_at_scan", "fiq"):
        df.loc[df[col] < 0, col] = np.nan

    return df


def build_phenotypic(
    project_root: Path = PROJECT_ROOT,
    output_dir: Path | None = None,
) -> pd.DataFrame:
    """Build the analysis-ready phenotypic table.

    Reads the project's participants.tsv (ID mapping) and joins phenotypic
    data from nilearn's ABIDE tables.
    """
    if output_dir is None:
        output_dir = project_root / "derivatives" / "connectivity"

    participants_tsv = project_root / "inputs" / "abide-both" / "participants.tsv"
    participants = pd.read_csv(participants_tsv, sep="\t")
    logger.info("Loaded %d participants from %s", len(participants), participants_tsv)

    # Fetch phenotypic data
    abide1_pheno, abide2_pheno = _fetch_abide_phenotypic()
    pheno1 = _normalize_phenotypic(abide1_pheno, "abide1")
    pheno2 = _normalize_phenotypic(abide2_pheno, "abide2")

    # Merge ABIDE I
    merged = participants.copy()
    merged["source_subject_id"] = merged["source_subject_id"].astype(str)

    mask1 = merged["source_dataset"] == "abide1"
    mask2 = merged["source_dataset"] == "abide2"

    # Join ABIDE I phenotypic on source_subject_id
    if not pheno1.empty:
        pheno1_dedup = pheno1.drop_duplicates(subset="source_subject_id")
        merged = merged.merge(
            pheno1_dedup[["source_subject_id", "dx_group", "age_at_scan", "sex", "fiq"]],
            on="source_subject_id",
            how="left",
            suffixes=("", "_abide1"),
        )
        # For ABIDE II subjects, these columns will be NaN from the left join
        # We'll fill them from the ABIDE II phenotypic table
        if not pheno2.empty:
            pheno2_dedup = pheno2.drop_duplicates(subset="source_subject_id")
            pheno2_indexed = pheno2_dedup.set_index("source_subject_id")
            for col in ("dx_group", "age_at_scan", "sex", "fiq"):
                abide2_values = merged.loc[mask2, "source_subject_id"].map(
                    pheno2_indexed[col]
                )
                merged.loc[mask2, col] = abide2_values.values
    else:
        for col in ("dx_group", "age_at_scan", "sex", "fiq"):
            merged[col] = np.nan

    # Map dx_group to labels for convenience
    dx_map = {1: "ASD", 2: "TC"}
    merged["dx_label"] = merged["dx_group"].map(dx_map)

    sex_map = {1: "M", 2: "F"}
    merged["sex_label"] = merged["sex"].map(sex_map)

    # Report coverage
    n_total = len(merged)
    n_dx = merged["dx_group"].notna().sum()
    n_age = merged["age_at_scan"].notna().sum()
    logger.info(
        "Phenotypic coverage: dx_group=%d/%d, age=%d/%d",
        n_dx, n_total, n_age, n_total,
    )
    for ds in ("abide1", "abide2"):
        mask = merged["source_dataset"] == ds
        n_ds = mask.sum()
        n_dx_ds = merged.loc[mask, "dx_group"].notna().sum()
        logger.info("  %s: %d subjects, %d with dx_group", ds, n_ds, n_dx_ds)

    # Write output
    output_dir.mkdir(parents=True, exist_ok=True)
    out_tsv = output_dir / "participants.tsv"
    merged.to_csv(out_tsv, sep="\t", index=False)
    logger.info("Wrote %s (%d rows)", out_tsv, len(merged))

    # Summary JSON
    summary = {
        "n_total": n_total,
        "n_abide1": int((merged["source_dataset"] == "abide1").sum()),
        "n_abide2": int((merged["source_dataset"] == "abide2").sum()),
        "n_asd": int((merged["dx_group"] == 1).sum()),
        "n_tc": int((merged["dx_group"] == 2).sum()),
        "n_dx_missing": int(merged["dx_group"].isna().sum()),
        "n_sites": int(merged["source_site"].nunique()),
        "sites": sorted(merged["source_site"].unique().tolist()),
    }
    write_json(output_dir / "phenotypic_summary.json", summary)

    return merged


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project-root",
        type=Path,
        default=PROJECT_ROOT,
        help="Project root directory (default: auto-detected)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory (default: derivatives/connectivity)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    build_phenotypic(
        project_root=args.project_root,
        output_dir=args.output_dir,
    )


if __name__ == "__main__":
    main()
