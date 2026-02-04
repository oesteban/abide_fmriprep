#!/usr/bin/env python3
import argparse
import gzip
import json
import os
import shutil
import struct
import subprocess
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


def list_sites(dataset_dir: Path) -> List[str]:
    sites = []
    for entry in sorted(dataset_dir.iterdir()):
        if not entry.is_dir():
            continue
        name = entry.name
        if name.startswith("."):
            continue
        if name in {".git", ".datalad"}:
            continue
        sites.append(name)
    return sites


def list_subjects(site_dir: Path) -> List[Path]:
    subjects = []
    for entry in sorted(site_dir.iterdir()):
        if not entry.is_dir():
            continue
        if entry.name.startswith("sub-"):
            subjects.append(entry)
    return subjects


def datalad_get(path_in_dataset: Path, dataset_dir: Path, dry_run: bool) -> None:
    rel = path_in_dataset.relative_to(dataset_dir)
    cmd = ["datalad", "-C", str(dataset_dir), "get", str(rel)]
    if dry_run:
        print("[DRYRUN]", " ".join(cmd))
        return
    subprocess.run(cmd, check=True)


def datalad_drop(
    path_in_dataset: Path,
    dataset_dir: Path,
    dry_run: bool,
    reckless_availability: bool,
) -> None:
    rel = path_in_dataset.relative_to(dataset_dir)
    cmd = ["datalad", "-C", str(dataset_dir), "drop"]
    if reckless_availability:
        cmd += ["--reckless", "availability"]
    cmd += [str(rel)]

    if dry_run:
        print("[DRYRUN]", " ".join(cmd))
        return

    # Drop is a best-effort cleanup: if it fails (e.g., no known copies), keep
    # the file rather than failing the whole build.
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"[WARN] datalad drop failed for {path_in_dataset}: {e}")


def nifti_tr_seconds(nifti_path: Path) -> float:
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


def parse_task_name(fname: str) -> Optional[str]:
    # Example: ..._task-rest_run-1_bold.nii.gz
    for part in fname.split("_"):
        if part.startswith("task-") and len(part) > 5:
            return part[len("task-"):]
    return None


def is_bold_nifti(path: Path) -> bool:
    name = path.name
    return name.endswith("_bold.nii.gz") or name.endswith("_bold.nii")


def is_bold_json(path: Path) -> bool:
    return path.name.endswith("_bold.json")


def sidecar_json_path(nifti_path: Path) -> Path:
    name = nifti_path.name
    if name.endswith(".nii.gz"):
        return nifti_path.with_name(name.removesuffix(".nii.gz") + ".json")
    if name.endswith(".nii"):
        return nifti_path.with_suffix(".json")
    raise ValueError(f"Not a NIfTI file: {nifti_path}")


def map_abide1_relpath(relpath: Path, orig_id: str, new_id: str) -> Path:
    rel_str = str(relpath).replace(f"sub-{orig_id}", f"sub-{new_id}")
    rel_str = os.path.join("ses-1", rel_str)
    dest_rel = Path(rel_str)

    filename = dest_rel.name
    if filename.startswith(f"sub-{new_id}") and "_ses-" not in filename:
        rest = filename[len(f"sub-{new_id}"):]
        filename = f"sub-{new_id}_ses-1{rest}"
        dest_rel = dest_rel.with_name(filename)
    return dest_rel


def map_abide2_relpath(relpath: Path, orig_id: str, new_id: str) -> Path:
    rel_str = str(relpath).replace(f"sub-{orig_id}", f"sub-{new_id}")
    return Path(rel_str)


def safe_symlink(src: Path, dest: Path, link_type: str, dry_run: bool) -> None:
    if link_type == "relative":
        target = os.path.relpath(src, dest.parent)
    elif link_type == "absolute":
        target = str(src)
    else:
        raise ValueError(f"Unsupported link type: {link_type}")

    if dest.exists() or dest.is_symlink():
        if dest.is_symlink():
            existing = os.readlink(dest)
            if existing == target:
                return
            if not dry_run:
                dest.unlink()
        else:
            raise RuntimeError(f"Destination exists and is not a symlink: {dest}")

    if not dry_run:
        dest.parent.mkdir(parents=True, exist_ok=True)
        os.symlink(target, dest)


def iter_source_files(subject_dir: Path) -> Iterable[Path]:
    for root, dirs, files in os.walk(subject_dir):
        dirs[:] = [
            d for d in dirs
            if not d.startswith(".") and d not in {".git", ".datalad"}
        ]
        dirs.sort()
        files.sort()
        for fname in files:
            if fname.startswith("."):
                continue
            yield Path(root) / fname


def ensure_bold_sidecar(
    src_bold: Path,
    dest_bold: Path,
    dataset_dir: Path,
    link_type: str,
    dry_run: bool,
    reckless_availability_drop: bool,
    overwrite: bool,
) -> None:
    dest_json = sidecar_json_path(dest_bold)

    if dest_json.exists() and not overwrite:
        try:
            existing = json.loads(dest_json.read_text(encoding="utf-8"))
        except Exception:
            existing = {}
        if isinstance(existing, dict) and "RepetitionTime" in existing:
            return

    src_json = sidecar_json_path(src_bold)

    # If the source JSON exists and already has RepetitionTime, just link it.
    if src_json.exists() and not overwrite and not dry_run:
        try:
            datalad_get(src_json, dataset_dir, dry_run=dry_run)
            src_meta = json.loads(src_json.read_text(encoding="utf-8"))
        except Exception:
            src_meta = {}

        rt = src_meta.get("RepetitionTime") if isinstance(src_meta, dict) else None
        if isinstance(rt, (int, float)) and rt > 0:
            # Create a symlink so we don't duplicate metadata.
            safe_symlink(src_json, dest_json, link_type=link_type, dry_run=dry_run)
            # Best-effort cleanup.
            datalad_drop(
                src_json,
                dataset_dir,
                dry_run=dry_run,
                reckless_availability=reckless_availability_drop,
            )
            return
    elif src_json.exists() and not overwrite and dry_run:
        # In dry-run mode we don't attempt to read JSON (might not be present).
        print(f"[DRYRUN] would inspect {src_json} for existing RepetitionTime")

    # Otherwise, extract TR from the NIfTI header (requires file content).
    datalad_get(src_bold, dataset_dir, dry_run=dry_run)
    if dry_run:
        print(f"[DRYRUN] would read TR from {src_bold} and write {dest_json}")
        print(f"[DRYRUN] would drop {src_bold}")
        if src_json.exists():
            print(f"[DRYRUN] would drop {src_json}")
        return

    tr_sec = nifti_tr_seconds(src_bold)

    meta: Dict[str, object] = {"RepetitionTime": round(float(tr_sec), 6)}
    task = parse_task_name(dest_bold.name)
    if task:
        meta["TaskName"] = task

    # Merge existing source JSON, if any.
    if src_json.exists():
        try:
            datalad_get(src_json, dataset_dir, dry_run=dry_run)
            src_meta = json.loads(src_json.read_text(encoding="utf-8"))
        except Exception:
            src_meta = {}
        if isinstance(src_meta, dict):
            merged = dict(src_meta)
            merged.update(meta)
            meta = merged

    if dry_run:
        print(f"[DRYRUN] write {dest_json} <- keys={sorted(meta.keys())}")
    else:
        dest_json.parent.mkdir(parents=True, exist_ok=True)
        dest_json.write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    # Best-effort cleanup: free raw file content we only needed for the header.
    datalad_drop(
        src_bold,
        dataset_dir,
        dry_run=dry_run,
        reckless_availability=reckless_availability_drop,
    )
    if src_json.exists():
        datalad_drop(
            src_json,
            dataset_dir,
            dry_run=dry_run,
            reckless_availability=reckless_availability_drop,
        )


def build_abide(
    project_root: Path,
    out_dir: Path,
    dataset_name: str,
    version_tag: str,
    link_type: str,
    dry_run: bool,
    participants: List[Tuple[str, str, str, int, str]],
    create_bold_sidecars: bool,
    reckless_availability_drop: bool,
    overwrite_sidecars: bool,
    sidecar_participant_ids: Optional[set],
) -> int:
    dataset_dir = project_root / "inputs" / dataset_name
    if not dataset_dir.exists():
        raise FileNotFoundError(f"Missing dataset directory: {dataset_dir}")

    sites = list_sites(dataset_dir)
    site_index = {site: idx for idx, site in enumerate(sites)}
    total_links = 0

    for site in sites:
        site_dir = dataset_dir / site
        subjects = list_subjects(site_dir)
        for subject_dir in subjects:
            orig_id = subject_dir.name[len("sub-"):]
            # NOTE: BIDS participant labels must be strictly alphanumeric.
            # We encode provenance info (ABIDE version + site index + original ID)
            # using only letters/digits: v1s0x0050642, v2s3x29006, ...
            new_id = f"{version_tag}s{site_index[site]}x{orig_id}"
            participant_id = f"sub-{new_id}"
            do_sidecars = create_bold_sidecars and (
                not sidecar_participant_ids or participant_id in sidecar_participant_ids
            )

            participants.append(
                (participant_id, dataset_name, site, site_index[site], orig_id)
            )

            for src in iter_source_files(subject_dir):
                relpath = src.relative_to(subject_dir)

                # Never symlink BOLD sidecars directly; we either symlink them
                # explicitly (if already valid) or create a new JSON with TR.
                if do_sidecars and is_bold_json(relpath):
                    continue

                if dataset_name == "abide1":
                    dest_rel = map_abide1_relpath(relpath, orig_id, new_id)
                else:
                    dest_rel = map_abide2_relpath(relpath, orig_id, new_id)

                dest = out_dir / f"sub-{new_id}" / dest_rel
                safe_symlink(src, dest, link_type, dry_run)
                total_links += 1

                if do_sidecars and is_bold_nifti(relpath):
                    ensure_bold_sidecar(
                        src_bold=src,
                        dest_bold=dest,
                        dataset_dir=dataset_dir,
                        link_type=link_type,
                        dry_run=dry_run,
                        reckless_availability_drop=reckless_availability_drop,
                        overwrite=overwrite_sidecars,
                    )

    return total_links


def clean_subject_tree(out_dir: Path, dry_run: bool) -> None:
    """Remove all existing BIDS subject directories (sub-*) in the merged view.

    This keeps the build idempotent and avoids stale subject IDs if the
    encoding scheme changes.
    """
    if not out_dir.exists():
        return

    for entry in out_dir.iterdir():
        if not entry.name.startswith("sub-"):
            continue
        if dry_run:
            continue
        if entry.is_symlink() or entry.is_file():
            entry.unlink()
        elif entry.is_dir():
            shutil.rmtree(entry)


def write_participants_tsv(
    out_dir: Path,
    participants: List[Tuple[str, str, str, int, str]],
    dry_run: bool,
) -> None:
    header = ["participant_id", "source_dataset", "source_site", "site_index", "source_subject_id"]
    rows = sorted(participants, key=lambda r: r[0])
    lines = ["\t".join(header)]
    for row in rows:
        lines.append("\t".join([str(col) for col in row]))

    if not dry_run:
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "participants.tsv").write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_dataset_description(out_dir: Path, dry_run: bool) -> None:
    data = {
        "Name": "ABIDE I+II merged view",
        "BIDSVersion": "1.10.0",
        "DatasetType": "raw",
        "GeneratedBy": [
            {"Name": "abide_preproc build_abide_both", "Version": "1.0"}
        ],
    }
    if not dry_run:
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "dataset_description.json").write_text(
            json.dumps(data, indent=2) + "\n",
            encoding="utf-8",
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a unified ABIDE I+II view using symlinks.")
    parser.add_argument(
        "--project-root",
        default=os.getcwd(),
        help="Project root containing inputs/ and code/ (default: cwd).",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Delete existing sub-* trees in inputs/abide-both before rebuilding (DANGEROUS).",
    )
    parser.add_argument(
        "--link-type",
        choices=["relative", "absolute"],
        default="relative",
        help="Symlink style (default: relative).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned operations without creating links or files.",
    )
    parser.add_argument(
        "--bold-sidecars",
        choices=["none", "tr"],
        default="tr",
        help="Create/update BOLD JSON sidecars with RepetitionTime (default: tr).",
    )
    parser.add_argument(
        "--sidecar-participant-id",
        action="append",
        default=[],
        help="Limit sidecar generation to these merged participant IDs (repeatable). "
        "Example: --sidecar-participant-id sub-v1s0x0050642",
    )
    parser.add_argument(
        "--overwrite-sidecars",
        action="store_true",
        help="Overwrite existing BOLD JSON sidecars (default: only add if missing RepetitionTime).",
    )
    parser.add_argument(
        "--safe-drop",
        dest="reckless_drop_availability",
        action="store_false",
        help="Use safe 'datalad drop' checks (default: use '--reckless availability' for speed).",
    )
    parser.add_argument(
        "--datasets",
        default="abide1,abide2",
        help="Comma-separated list of datasets to include (default: abide1,abide2).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = Path(args.project_root).resolve()
    out_dir = project_root / "inputs" / "abide-both"
    datasets = [d.strip() for d in args.datasets.split(",") if d.strip()]

    participants: List[Tuple[str, str, str, int, str]] = []
    total_links = 0

    sidecar_participant_ids = None
    if args.sidecar_participant_id:
        sidecar_participant_ids = set()
        for pid in args.sidecar_participant_id:
            pid = pid.strip()
            if not pid:
                continue
            if not pid.startswith("sub-"):
                pid = f"sub-{pid}"
            sidecar_participant_ids.add(pid)

    if args.clean:
        clean_subject_tree(out_dir, args.dry_run)

    if "abide1" in datasets:
        total_links += build_abide(
            project_root=project_root,
            out_dir=out_dir,
            dataset_name="abide1",
            version_tag="v1",
            link_type=args.link_type,
            dry_run=args.dry_run,
            participants=participants,
            create_bold_sidecars=(args.bold_sidecars == "tr"),
            reckless_availability_drop=args.reckless_drop_availability,
            overwrite_sidecars=args.overwrite_sidecars,
            sidecar_participant_ids=sidecar_participant_ids,
        )

    if "abide2" in datasets:
        total_links += build_abide(
            project_root=project_root,
            out_dir=out_dir,
            dataset_name="abide2",
            version_tag="v2",
            link_type=args.link_type,
            dry_run=args.dry_run,
            participants=participants,
            create_bold_sidecars=(args.bold_sidecars == "tr"),
            reckless_availability_drop=args.reckless_drop_availability,
            overwrite_sidecars=args.overwrite_sidecars,
            sidecar_participant_ids=sidecar_participant_ids,
        )

    write_participants_tsv(out_dir, participants, args.dry_run)
    write_dataset_description(out_dir, args.dry_run)

    print(f"[INFO] Created/updated {total_links} symlinks in {out_dir}")
    print(f"[INFO] Participants: {len(participants)}")


if __name__ == "__main__":
    main()
