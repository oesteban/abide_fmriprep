#!/usr/bin/env python3
"""Pre-screen QC: read confounds TSVs (git-tracked, no datalad get needed).

For each eligible subject, compute mean FD and count usable volumes per run.
Select the run with lowest mean FD (multi-run handling).  Exclude subjects
whose best run exceeds the FD or volume thresholds.

Usage::

    python code/analysis/01_prescreen_qc.py --project-root .

Output: derivatives/connectivity/qc_prescreen.tsv
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd


def _setup_path():
    """Ensure code/analysis is on sys.path."""
    here = Path(__file__).resolve().parent
    if str(here) not in sys.path:
        sys.path.insert(0, str(here))


_setup_path()

from _helpers import (
    MAX_MEAN_FD,
    MIN_USABLE_VOLUMES,
    derivatives_connectivity,
    derivatives_fmriprep,
    eligible_subjects,
    find_confounds,
)


def _compute_run_qc(confounds_path: Path) -> dict:
    """Compute QC metrics for a single run from its confounds TSV.

    Returns a dict with: mean_fd, total_volumes, usable_volumes, n_cosines,
    n_motion_outliers.
    """
    df = pd.read_csv(confounds_path, sep="\t")

    # Mean framewise displacement (first volume is NaN)
    fd = df["framewise_displacement"]
    mean_fd = float(fd.mean(skipna=True)) if not fd.isna().all() else np.nan

    total_volumes = len(df)

    # Count non-steady-state outlier columns
    nss_cols = [c for c in df.columns if c.startswith("non_steady_state_outlier")]
    n_nss = 0
    for c in nss_cols:
        n_nss += int(df[c].sum())

    # Count motion outlier columns
    mo_cols = [c for c in df.columns if c.startswith("motion_outlier")]
    n_motion_outliers = len(mo_cols)

    # Usable volumes = total - non-steady-state - motion outliers
    usable_volumes = total_volumes - n_nss - n_motion_outliers

    # Count cosine regressors (for reference)
    n_cosines = sum(1 for c in df.columns if c.startswith("cosine"))

    return {
        "mean_fd": round(mean_fd, 6) if not np.isnan(mean_fd) else np.nan,
        "total_volumes": total_volumes,
        "usable_volumes": usable_volumes,
        "n_cosines": n_cosines,
        "n_motion_outliers": n_motion_outliers,
    }


def prescreen(project_root: Path) -> pd.DataFrame:
    """Run pre-screen QC on all eligible subjects."""
    subjects = eligible_subjects(project_root)
    fmriprep_dir = derivatives_fmriprep(project_root)

    records = []
    for _, row in subjects.iterrows():
        sub_id = row["participant_id"]
        source_dataset = row["source_dataset"]
        source_site = row["source_site"]
        group = row["group"]

        runs = find_confounds(sub_id, fmriprep_dir)
        if not runs:
            records.append({
                "participant_id": sub_id,
                "source_dataset": source_dataset,
                "source_site": source_site,
                "group": group,
                "selected_run": "n/a",
                "n_runs_available": 0,
                "mean_fd": np.nan,
                "total_volumes": 0,
                "usable_volumes": 0,
                "n_cosines": 0,
                "n_motion_outliers": 0,
                "excluded_reason": "no_fmriprep_output",
            })
            continue

        # Compute QC for each run
        run_qc = []
        for run_label, conf_path in runs:
            try:
                qc = _compute_run_qc(conf_path)
                qc["run_label"] = run_label
                run_qc.append(qc)
            except Exception as exc:
                print(f"  WARNING: {sub_id} {run_label}: {exc}", file=sys.stderr, flush=True)

        if not run_qc:
            records.append({
                "participant_id": sub_id,
                "source_dataset": source_dataset,
                "source_site": source_site,
                "group": group,
                "selected_run": "n/a",
                "n_runs_available": len(runs),
                "mean_fd": np.nan,
                "total_volumes": 0,
                "usable_volumes": 0,
                "n_cosines": 0,
                "n_motion_outliers": 0,
                "excluded_reason": "confounds_unreadable",
            })
            continue

        # Select the run with lowest mean FD
        best = min(run_qc, key=lambda r: r["mean_fd"] if not np.isnan(r["mean_fd"]) else np.inf)

        # Determine exclusion reason
        reasons = []
        if np.isnan(best["mean_fd"]) or best["mean_fd"] > MAX_MEAN_FD:
            reasons.append("high_fd")
        if best["usable_volumes"] < MIN_USABLE_VOLUMES:
            reasons.append("low_volumes")

        records.append({
            "participant_id": sub_id,
            "source_dataset": source_dataset,
            "source_site": source_site,
            "group": group,
            "selected_run": best["run_label"],
            "n_runs_available": len(runs),
            "mean_fd": best["mean_fd"],
            "total_volumes": best["total_volumes"],
            "usable_volumes": best["usable_volumes"],
            "n_cosines": best["n_cosines"],
            "n_motion_outliers": best["n_motion_outliers"],
            "excluded_reason": "+".join(reasons) if reasons else "pass",
        })

    return pd.DataFrame(records)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project-root",
        type=Path,
        default=Path("."),
        help="Path to the YODA superdataset root.",
    )
    parser.add_argument(
        "--variant",
        default="v1",
        help="Connectivity variant suffix (default: v1).",
    )
    args = parser.parse_args()
    root = args.project_root.resolve()

    print(f"Pre-screening subjects in {root}", flush=True)
    df = prescreen(root)

    # Write output
    out_dir = derivatives_connectivity(root, variant=args.variant)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "qc_prescreen.tsv"
    df.to_csv(out_path, sep="\t", index=False)

    # Summary
    n_pass = (df["excluded_reason"] == "pass").sum()
    n_excluded = len(df) - n_pass
    n_high_fd = df["excluded_reason"].str.contains("high_fd", na=False).sum()
    n_low_vol = df["excluded_reason"].str.contains("low_volumes", na=False).sum()
    n_no_output = (df["excluded_reason"] == "no_fmriprep_output").sum()

    # ABIDE I / II breakdown
    df_pass = df[df["excluded_reason"] == "pass"]
    n_abide1 = (df_pass["source_dataset"] == "abide1").sum()
    n_abide2 = (df_pass["source_dataset"] == "abide2").sum()

    print(f"\nPre-screen results written to {out_path}", flush=True)
    print(f"  Eligible subjects screened: {len(df)}", flush=True)
    print(f"  Passed: {n_pass} (ABIDE I: {n_abide1}, ABIDE II: {n_abide2})", flush=True)
    print(f"  Excluded: {n_excluded}", flush=True)
    print(f"    - No fMRIPrep output: {n_no_output}", flush=True)
    print(f"    - High mean FD (>{MAX_MEAN_FD} mm): {n_high_fd}", flush=True)
    print(f"    - Low usable volumes (<{MIN_USABLE_VOLUMES}): {n_low_vol}", flush=True)

    # Multi-run stats
    multi_run = df[df["n_runs_available"] > 1]
    if len(multi_run) > 0:
        print(f"\n  Multi-run subjects: {len(multi_run)}", flush=True)
        print(f"    Selected non-run-1: {(multi_run['selected_run'] != 'run-1').sum()}", flush=True)


if __name__ == "__main__":
    main()
