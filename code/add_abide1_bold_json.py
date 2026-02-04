#!/usr/bin/env python3
"""
Create minimal BIDS JSON sidecars for ABIDE I BOLD images in inputs/abide-both.

Why?
ABIDE I does not ship functional JSON sidecars, but fMRIPrep/BIDS tooling needs
at least RepetitionTime. The TR is available in the NIfTI header (pixdim[4]
with xyzt_units time scaling).

We *do not* modify the original inputs/abide1 dataset. Instead, we write new
JSON files next to the (symlinked) BOLD files in inputs/abide-both/sub-v1*/.
"""

from __future__ import annotations

import argparse
import gzip
import json
import re
import struct
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


_TASK_RE = re.compile(r"_task-([^_]+)")


def _parse_task_name(fname: str) -> Optional[str]:
    m = _TASK_RE.search(fname)
    if not m:
        return None
    return m.group(1)


def _nifti_tr_seconds(nifti_path: Path) -> float:
    """Extract TR from a NIfTI-1 header (nii or nii.gz) in seconds."""

    if nifti_path.name.endswith(".nii.gz"):
        opener = gzip.open
    elif nifti_path.name.endswith(".nii"):
        opener = open
    else:
        raise ValueError(f"Not a NIfTI file: {nifti_path}")

    with opener(nifti_path, "rb") as f:
        hdr = f.read(348)
    if len(hdr) != 348:
        raise ValueError(f"Short NIfTI header ({len(hdr)} bytes): {nifti_path}")

    sizeof_hdr_le = struct.unpack("<i", hdr[0:4])[0]
    if sizeof_hdr_le == 348:
        endian = "<"
    else:
        sizeof_hdr_be = struct.unpack(">i", hdr[0:4])[0]
        if sizeof_hdr_be != 348:
            raise ValueError(f"Not a NIfTI-1 header (sizeof_hdr != 348): {nifti_path}")
        endian = ">"

    pixdim = struct.unpack(f"{endian}8f", hdr[76:108])
    tr = float(pixdim[4])

    # xyzt_units is a bitfield. Time units are in bits 3..5.
    # 8=sec, 16=msec, 24=usec (combined with spatial units in bits 0..2).
    xyzt_units = hdr[123]
    time_unit = xyzt_units & 0x38
    factor = {8: 1.0, 16: 0.001, 24: 1e-6}.get(time_unit, 1.0)

    tr_sec = tr * factor
    if tr_sec <= 0:
        raise ValueError(f"Invalid TR extracted ({tr_sec}) from {nifti_path}")
    return tr_sec


def _sidecar_path(nifti_path: Path) -> Path:
    name = nifti_path.name
    if name.endswith(".nii.gz"):
        return nifti_path.with_name(name.removesuffix(".nii.gz") + ".json")
    if name.endswith(".nii"):
        return nifti_path.with_suffix(".json")
    raise ValueError(f"Not a NIfTI file: {nifti_path}")


def _iter_abide1_bold_niftis(bids_root: Path, participant_id: str) -> Iterable[Path]:
    # ABIDE I is normalized to a synthetic ses-1 level.
    func_dir = bids_root / participant_id / "ses-1" / "func"
    if not func_dir.exists():
        return
    yield from sorted(func_dir.glob("*_bold.nii.gz"))
    yield from sorted(func_dir.glob("*_bold.nii"))


def _load_participants_abide1(participants_tsv: Path) -> List[str]:
    # participants.tsv:
    # participant_id  source_dataset  source_site  site_index  source_subject_id
    rows: List[str] = []
    with participants_tsv.open("r", encoding="utf-8") as f:
        header = f.readline().rstrip("\n").split("\t")
        try:
            idx_pid = header.index("participant_id")
            idx_ds = header.index("source_dataset")
        except ValueError as e:
            raise RuntimeError(f"Unexpected participants.tsv header: {header}") from e

        for line in f:
            cols = line.rstrip("\n").split("\t")
            if len(cols) <= max(idx_pid, idx_ds):
                continue
            if cols[idx_ds] != "abide1":
                continue
            rows.append(cols[idx_pid])
    return rows


def _update_json(
    sidecar: Path,
    new_meta: Dict[str, object],
    overwrite: bool,
    dry_run: bool,
) -> bool:
    """Write/merge sidecar JSON. Returns True if a write would happen."""
    if sidecar.exists() and not overwrite:
        try:
            existing = json.loads(sidecar.read_text(encoding="utf-8"))
        except Exception:
            existing = {}
        # If already has RepetitionTime, keep it as-is.
        if isinstance(existing, dict) and "RepetitionTime" in existing:
            return False

        merged = dict(existing) if isinstance(existing, dict) else {}
        merged.update(new_meta)
        if dry_run:
            return True
        sidecar.write_text(json.dumps(merged, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return True

    if dry_run:
        return True
    sidecar.write_text(json.dumps(new_meta, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return True


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Create minimal BIDS JSON sidecars for ABIDE I BOLD images in inputs/abide-both."
    )
    p.add_argument(
        "--project-root",
        default=".",
        help="Repo root containing inputs/ (default: .).",
    )
    p.add_argument(
        "--participant-id",
        action="append",
        default=[],
        help="Process only this participant (repeatable). Accepts 'sub-...'' or 'v1...'.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write files; just report what would be done.",
    )
    p.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing JSON sidecars (default: only create if missing RepetitionTime).",
    )
    p.add_argument(
        "--fail-on-missing-nifti",
        action="store_true",
        help="Exit non-zero if any BOLD NIfTI is not readable (default: skip).",
    )
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    project_root = Path(args.project_root).resolve()
    bids_root = project_root / "inputs" / "abide-both"
    participants_tsv = bids_root / "participants.tsv"

    if not bids_root.exists():
        print(f"[FATAL] Missing BIDS root: {bids_root}", file=sys.stderr)
        return 2
    if not participants_tsv.exists():
        print(f"[FATAL] Missing participants.tsv: {participants_tsv}", file=sys.stderr)
        return 2

    if args.participant_id:
        participants: List[str] = []
        for pid in args.participant_id:
            pid = pid.strip()
            if not pid:
                continue
            if not pid.startswith("sub-"):
                pid = f"sub-{pid}"
            participants.append(pid)
    else:
        participants = _load_participants_abide1(participants_tsv)

    wrote = 0
    skipped = 0
    missing = 0
    for pid in participants:
        for bold in _iter_abide1_bold_niftis(bids_root, pid):
            sidecar = _sidecar_path(bold)
            try:
                tr = _nifti_tr_seconds(bold)
            except Exception as e:
                missing += 1
                msg = f"[WARN] Cannot read TR from {bold}: {e}"
                if args.fail_on_missing_nifti:
                    print(msg, file=sys.stderr)
                else:
                    print(msg)
                if args.fail_on_missing_nifti:
                    continue
                else:
                    continue

            task = _parse_task_name(bold.name)
            meta: Dict[str, object] = {"RepetitionTime": round(float(tr), 6)}
            if task:
                meta["TaskName"] = task

            if args.dry_run:
                print(f"[DRYRUN] {sidecar} <- {meta}")
                wrote += 1
                continue

            did_write = _update_json(sidecar, meta, overwrite=args.overwrite, dry_run=False)
            if did_write:
                wrote += 1
            else:
                skipped += 1

    print(f"[INFO] Sidecars written/updated: {wrote}; skipped: {skipped}; unreadable NIfTI: {missing}")
    if args.fail_on_missing_nifti and missing:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

