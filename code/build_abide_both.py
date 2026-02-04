#!/usr/bin/env python3
import argparse
import json
import os
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


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
        for fname in files:
            if fname.startswith("."):
                continue
            yield Path(root) / fname


def build_abide(
    project_root: Path,
    out_dir: Path,
    dataset_name: str,
    version_tag: str,
    link_type: str,
    dry_run: bool,
    participants: List[Tuple[str, str, str, int, str]],
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
            new_id = f"{version_tag}+s{site_index[site]}+{orig_id}"

            participants.append(
                (f"sub-{new_id}", dataset_name, site, site_index[site], orig_id)
            )

            for src in iter_source_files(subject_dir):
                relpath = src.relative_to(subject_dir)
                if dataset_name == "abide1":
                    dest_rel = map_abide1_relpath(relpath, orig_id, new_id)
                else:
                    dest_rel = map_abide2_relpath(relpath, orig_id, new_id)

                dest = out_dir / f"sub-{new_id}" / dest_rel
                safe_symlink(src, dest, link_type, dry_run)
                total_links += 1

    return total_links


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

    if "abide1" in datasets:
        total_links += build_abide(
            project_root=project_root,
            out_dir=out_dir,
            dataset_name="abide1",
            version_tag="v1",
            link_type=args.link_type,
            dry_run=args.dry_run,
            participants=participants,
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
        )

    write_participants_tsv(out_dir, participants, args.dry_run)
    write_dataset_description(out_dir, args.dry_run)

    print(f"[INFO] Created/updated {total_links} symlinks in {out_dir}")
    print(f"[INFO] Participants: {len(participants)}")


if __name__ == "__main__":
    main()
