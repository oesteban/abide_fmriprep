#!/usr/bin/env python3
import argparse
import fnmatch
import gzip
import json
import os
import shutil
import struct
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


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


def run_cmd(
    cmd: List[str],
    cwd: Optional[Path] = None,
    *,
    capture_stdout: bool = False,
    dry_run: bool = False,
) -> str:
    if dry_run:
        prefix = f"[DRYRUN]{' (cwd=' + str(cwd) + ')' if cwd else ''}"
        print(prefix, " ".join(cmd))
        return ""

    res = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        check=True,
        stdout=subprocess.PIPE if capture_stdout else None,
        stderr=None,
        text=True,
    )
    return res.stdout if capture_stdout else ""


def path_looks_tracked(path: Path) -> bool:
    # Path.exists() is False for dangling symlinks; we still want to treat those
    # as "present in the dataset tree".
    return path.exists() or path.is_symlink()


def annex_get(repo_dir: Path, relpath: Path, dry_run: bool) -> None:
    cmd = ["git", "annex", "get", "-q", str(relpath)]
    run_cmd(cmd, cwd=repo_dir, dry_run=dry_run)


def annex_drop(repo_dir: Path, relpath: Path, dry_run: bool, force: bool) -> None:
    cmd = ["git", "annex", "drop", str(relpath)]
    if force:
        cmd.insert(3, "--force")

    if dry_run:
        run_cmd(cmd, cwd=repo_dir, dry_run=True)
        return

    # Drop is a best-effort cleanup: if it fails (e.g., no known copies),
    # keep the file rather than failing the whole build.
    try:
        subprocess.run(cmd, cwd=str(repo_dir), check=True)
    except subprocess.CalledProcessError as e:
        print(f"[WARN] git-annex drop failed for {repo_dir}/{relpath}: {e}")


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


def parse_bids_entity(fname: str, entity: str) -> Optional[str]:
    """Extract a BIDS entity value from a filename.

    Example:
      parse_bids_entity("..._acq-rc8chan_run-1_bold.nii.gz", "acq") -> "rc8chan"
    """
    prefix = f"{entity}-"
    for part in fname.split("_"):
        if part.startswith(prefix) and len(part) > len(prefix):
            return part[len(prefix) :]
    return None


def is_bold_nifti(path: Path) -> bool:
    name = path.name
    return name.endswith("_bold.nii.gz") or name.endswith("_bold.nii")


def is_bold_json(path: Path) -> bool:
    return path.name.endswith("_bold.json")


def is_t1w_nifti(path: Path) -> bool:
    name = path.name
    return name.endswith("_T1w.nii.gz") or name.endswith("_T1w.nii")


def is_t1w_json(path: Path) -> bool:
    return path.name.endswith("_T1w.json")


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


def annex_whereis_key_urls(repo_dir: Path, relpath: Path, dry_run: bool) -> Tuple[str, List[str]]:
    """Return (key, urls) for a file tracked in a git-annex repo."""
    cmd = ["git", "annex", "whereis", "--json", str(relpath)]
    out = run_cmd(cmd, cwd=repo_dir, capture_stdout=True, dry_run=dry_run)
    if dry_run:
        return ("<KEY>", ["<URL>"])
    data = json.loads(out.strip().splitlines()[-1])
    key = data.get("key")
    if not isinstance(key, str) or not key:
        raise RuntimeError(f"Could not determine annex key for {repo_dir}/{relpath}")

    urls: List[str] = []
    for entry in data.get("whereis", []) or []:
        for url in entry.get("urls", []) or []:
            if isinstance(url, str) and url:
                urls.append(url)

    # Stable order, no duplicates
    seen = set()
    uniq = []
    for u in urls:
        if u in seen:
            continue
        seen.add(u)
        uniq.append(u)
    return key, uniq


def annex_fromkey(dest_repo_dir: Path, key: str, dest_relpath: Path, dry_run: bool) -> None:
    dest_abs = dest_repo_dir / dest_relpath
    if os.path.lexists(dest_abs):
        # If it's already the same key, keep; otherwise refuse to overwrite.
        cmd = ["git", "annex", "lookupkey", str(dest_relpath)]
        out = run_cmd(cmd, cwd=dest_repo_dir, capture_stdout=True, dry_run=dry_run).strip()
        if dry_run:
            return
        if out == key:
            return
        raise RuntimeError(f"Destination exists with different key: {dest_abs} (have {out}, want {key})")

    if dry_run:
        run_cmd(["git", "annex", "fromkey", "--force", key, str(dest_relpath)], cwd=dest_repo_dir, dry_run=True)
        return

    dest_abs.parent.mkdir(parents=True, exist_ok=True)
    # Newer git-annex versions can sanity-check keys against the current backend
    # and refuse to add "foreign" keys without --force. Since we are reusing keys
    # reported by the source datasets, override the check.
    run_cmd(["git", "annex", "fromkey", "--force", key, str(dest_relpath)], cwd=dest_repo_dir, dry_run=False)


def annex_registerurls(dest_repo_dir: Path, key: str, urls: List[str], dry_run: bool) -> None:
    if not urls:
        print(f"[WARN] No URLs found for key {key}; file may not be retrievable via 'web' remote.")
        return
    for url in urls:
        run_cmd(
            ["git", "annex", "registerurl", "--remote", "web", key, url],
            cwd=dest_repo_dir,
            dry_run=dry_run,
        )


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


def load_site_template_json(
    src_repo_dir: Path,
    template_name: str,
    *,
    dry_run: bool,
    force_drop: bool,
    site_template_cache: Dict[Tuple[Path, str], Dict[str, Any]],
) -> Dict[str, Any]:
    """Load a site-level JSON template from a source dataset (best-effort).

    ABIDE stores most metadata in the site root (BIDS inheritance), e.g.:
      - task-rest_bold.json
      - task-rest_acq-rc8chan_bold.json
      - T1w.json
      - acq-rc8chan_T1w.json

    These files are typically annexed. We fetch them on-demand, read JSON, and
    then drop to avoid accumulating content during large builds.
    """
    cache_key = (src_repo_dir, template_name)
    if cache_key in site_template_cache:
        return dict(site_template_cache[cache_key])

    meta: Dict[str, Any] = {}
    template_path = src_repo_dir / template_name
    if path_looks_tracked(template_path):
        if dry_run:
            print(f"[DRYRUN] would git-annex get {src_repo_dir}/{template_name} (site template)")
        else:
            try:
                annex_get(src_repo_dir, Path(template_name), dry_run=False)
                loaded = json.loads(template_path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    meta = loaded
            except Exception:
                meta = {}
            annex_drop(src_repo_dir, Path(template_name), dry_run=False, force=force_drop)

    site_template_cache[cache_key] = dict(meta)
    return dict(meta)


def ensure_bold_sidecar(
    src_repo_dir: Path,
    src_bold_rel: Path,
    dest_repo_dir: Path,
    dest_bold_rel: Path,
    dataset_name: str,
    site: str,
    dry_run: bool,
    force_drop: bool,
    overwrite: bool,
    ensure_tr: bool,
    site_template_cache: Dict[Tuple[Path, str], Dict[str, Any]],
) -> None:
    dest_bold_abs = dest_repo_dir / dest_bold_rel
    dest_json = sidecar_json_path(dest_bold_abs)

    if dest_json.exists() and not overwrite:
        try:
            existing = json.loads(dest_json.read_text(encoding="utf-8"))
        except Exception:
            existing = {}
        if isinstance(existing, dict) and (not ensure_tr or "RepetitionTime" in existing):
            return

    src_bold_abs = src_repo_dir / src_bold_rel
    src_json_rel = sidecar_json_path(src_bold_rel)
    src_json_abs = src_repo_dir / src_json_rel

    # Start from a site-level template (e.g., task-rest_bold.json), then overlay
    # any per-file JSON (if it exists).
    task = parse_task_name(dest_bold_abs.name) or parse_task_name(src_bold_rel.name)
    acq = parse_bids_entity(dest_bold_abs.name, "acq") or parse_bids_entity(src_bold_rel.name, "acq")

    # Apply BIDS inheritance within the source site:
    # 1) task-level template (task-<task>_bold.json)
    # 2) task+acq template (task-<task>_acq-<acq>_bold.json), if present
    template_meta: Dict[str, Any] = {}
    if task:
        template_meta.update(
            load_site_template_json(
                src_repo_dir,
                f"task-{task}_bold.json",
                dry_run=dry_run,
                force_drop=force_drop,
                site_template_cache=site_template_cache,
            )
        )
    if task and acq:
        template_meta.update(
            load_site_template_json(
                src_repo_dir,
                f"task-{task}_acq-{acq}_bold.json",
                dry_run=dry_run,
                force_drop=force_drop,
                site_template_cache=site_template_cache,
            )
        )
    elif acq:
        # Rare fallback: acquisition-level template without a task entity.
        template_meta.update(
            load_site_template_json(
                src_repo_dir,
                f"acq-{acq}_bold.json",
                dry_run=dry_run,
                force_drop=force_drop,
                site_template_cache=site_template_cache,
            )
        )

    src_meta: Dict[str, Any] = {}
    if path_looks_tracked(src_json_abs):
        if dry_run:
            print(f"[DRYRUN] would git-annex get {src_repo_dir}/{src_json_rel} (per-file JSON)")
        else:
            try:
                annex_get(src_repo_dir, src_json_rel, dry_run=False)
                loaded = json.loads(src_json_abs.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    src_meta = loaded
            except Exception:
                src_meta = {}
            annex_drop(src_repo_dir, src_json_rel, dry_run=False, force=force_drop)

    # Prefer an existing RepetitionTime in source JSON (if present and numeric).
    tr_sec: Optional[float] = None
    if not overwrite:
        rt = src_meta.get("RepetitionTime") or template_meta.get("RepetitionTime")
        if isinstance(rt, (int, float)) and rt > 0:
            tr_sec = float(rt)

    if tr_sec is None and ensure_tr:
        # Extract TR from the NIfTI header (requires file content).
        if dry_run:
            annex_get(src_repo_dir, src_bold_rel, dry_run=True)
            print(f"[DRYRUN] would read TR from {src_repo_dir}/{src_bold_rel} (dataset={dataset_name} site={site})")
            print(f"[DRYRUN] would drop {src_repo_dir}/{src_bold_rel}")
        else:
            try:
                annex_get(src_repo_dir, src_bold_rel, dry_run=False)
                tr_sec = nifti_tr_seconds(src_bold_abs)
            except Exception as e:
                print(f"[WARN] Could not extract TR for {src_repo_dir}/{src_bold_rel}: {e}")
            finally:
                annex_drop(src_repo_dir, src_bold_rel, dry_run=False, force=force_drop)

    meta: Dict[str, Any] = dict(template_meta)
    meta.update(src_meta)
    if tr_sec is not None:
        meta["RepetitionTime"] = round(float(tr_sec), 6)
    if task and "TaskName" not in meta:
        meta["TaskName"] = task

    if dry_run:
        print(f"[DRYRUN] write {dest_json} <- keys={sorted(meta.keys())}")
        return

    dest_json.parent.mkdir(parents=True, exist_ok=True)
    dest_json.write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def ensure_t1w_sidecar(
    src_repo_dir: Path,
    src_t1w_rel: Path,
    dest_repo_dir: Path,
    dest_t1w_rel: Path,
    dataset_name: str,
    site: str,
    dry_run: bool,
    force_drop: bool,
    overwrite: bool,
    site_template_cache: Dict[Tuple[Path, str], Dict[str, Any]],
) -> None:
    dest_t1w_abs = dest_repo_dir / dest_t1w_rel
    dest_json = sidecar_json_path(dest_t1w_abs)
    if dest_json.exists() and not overwrite:
        return

    acq = parse_bids_entity(dest_t1w_abs.name, "acq") or parse_bids_entity(src_t1w_rel.name, "acq")

    # Apply BIDS inheritance within the source site:
    # 1) site-level template (T1w.json)
    # 2) acq-level template (acq-<acq>_T1w.json), if present
    template_meta: Dict[str, Any] = {}
    template_meta.update(
        load_site_template_json(
            src_repo_dir,
            "T1w.json",
            dry_run=dry_run,
            force_drop=force_drop,
            site_template_cache=site_template_cache,
        )
    )
    if acq:
        template_meta.update(
            load_site_template_json(
                src_repo_dir,
                f"acq-{acq}_T1w.json",
                dry_run=dry_run,
                force_drop=force_drop,
                site_template_cache=site_template_cache,
            )
        )

    src_json_rel = sidecar_json_path(src_t1w_rel)
    src_json_abs = src_repo_dir / src_json_rel
    src_meta: Dict[str, Any] = {}
    if path_looks_tracked(src_json_abs):
        if dry_run:
            print(f"[DRYRUN] would git-annex get {src_repo_dir}/{src_json_rel} (per-file JSON)")
        else:
            try:
                annex_get(src_repo_dir, src_json_rel, dry_run=False)
                loaded = json.loads(src_json_abs.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    src_meta = loaded
            except Exception:
                src_meta = {}
            annex_drop(src_repo_dir, src_json_rel, dry_run=False, force=force_drop)

    meta: Dict[str, Any] = dict(template_meta)
    meta.update(src_meta)

    if dry_run:
        print(f"[DRYRUN] write {dest_json} <- keys={sorted(meta.keys())} (dataset={dataset_name} site={site})")
        return

    if not meta:
        print(f"[WARN] No T1w metadata available for {src_repo_dir}/{src_t1w_rel}; not writing {dest_json}")
        return

    dest_json.parent.mkdir(parents=True, exist_ok=True)
    dest_json.write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def build_abide(
    project_root: Path,
    out_dir: Path,
    dataset_name: str,
    version_tag: str,
    dry_run: bool,
    participants: List[Tuple[str, str, str, int, str]],
    create_sidecars: bool,
    ensure_tr: bool,
    force_drop: bool,
    overwrite_sidecars: bool,
    sidecar_participant_ids: Optional[set],
) -> int:
    dataset_dir = project_root / "inputs" / dataset_name
    if not dataset_dir.exists():
        raise FileNotFoundError(f"Missing dataset directory: {dataset_dir}")

    sites = list_sites(dataset_dir)
    site_index = {site: idx for idx, site in enumerate(sites)}
    created_files = 0
    skipped_files = 0

    site_template_cache: Dict[Tuple[Path, str], Dict[str, Any]] = {}

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
            do_sidecars = create_sidecars and (
                not sidecar_participant_ids or participant_id in sidecar_participant_ids
            )

            participants.append(
                (participant_id, dataset_name, site, site_index[site], orig_id)
            )

            for src in iter_source_files(subject_dir):
                relpath = src.relative_to(subject_dir)

                if dataset_name == "abide1":
                    dest_rel = map_abide1_relpath(relpath, orig_id, new_id)
                else:
                    dest_rel = map_abide2_relpath(relpath, orig_id, new_id)

                dest_repo_rel = Path(participant_id) / dest_rel

                # If we are generating sidecars, skip copying/adding any source
                # per-file BOLD/T1w sidecars. We'll generate a new JSON in the
                # merged dataset (and keep it in Git).
                if do_sidecars and is_bold_json(relpath):
                    continue
                if do_sidecars and is_t1w_json(relpath):
                    continue

                # Create an annex pointer in inputs/abide-both with the same key,
                # and register the original URL(s) so 'datalad get' can retrieve it.
                dest_abs = out_dir / dest_repo_rel
                if os.path.lexists(dest_abs):
                    if dest_abs.is_dir():
                        raise RuntimeError(f"Destination exists and is a directory: {dest_abs}")
                    skipped_files += 1
                else:
                    if dry_run:
                        print(f"[DRYRUN] add {out_dir.name}/{dest_repo_rel} <- {site_dir.name}/{subject_dir.name}/{relpath}")
                    else:
                        src_rel_in_site = src.relative_to(site_dir)
                        key, urls = annex_whereis_key_urls(site_dir, src_rel_in_site, dry_run=False)
                        annex_fromkey(out_dir, key, dest_repo_rel, dry_run=False)
                        annex_registerurls(out_dir, key, urls, dry_run=False)
                    created_files += 1

                if do_sidecars and is_bold_nifti(relpath):
                    ensure_bold_sidecar(
                        src_repo_dir=site_dir,
                        src_bold_rel=src.relative_to(site_dir),
                        dest_repo_dir=out_dir,
                        dest_bold_rel=dest_repo_rel,
                        dataset_name=dataset_name,
                        site=site,
                        dry_run=dry_run,
                        force_drop=force_drop,
                        overwrite=overwrite_sidecars,
                        ensure_tr=ensure_tr,
                        site_template_cache=site_template_cache,
                    )

                if do_sidecars and is_t1w_nifti(relpath):
                    ensure_t1w_sidecar(
                        src_repo_dir=site_dir,
                        src_t1w_rel=src.relative_to(site_dir),
                        dest_repo_dir=out_dir,
                        dest_t1w_rel=dest_repo_rel,
                        dataset_name=dataset_name,
                        site=site,
                        dry_run=dry_run,
                        force_drop=force_drop,
                        overwrite=overwrite_sidecars,
                        site_template_cache=site_template_cache,
                    )

    print(
        f"[INFO] {dataset_name}: created {created_files} files, skipped {skipped_files} existing files."
    )
    return created_files + skipped_files


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


METADATA_PATTERNS = [
    "*.json",
    "*.tsv",
    "*.bval",
    "*.bvec",
    "*.bvec*",
    "*.txt",
    "*.csv",
]


def iter_metadata_candidate_relpaths(repo_dir: Path) -> List[Path]:
    """Return repo-relative paths of potential BIDS metadata files.

    The merged dataset uses git-annex keys for everything we add via `fromkey`.
    For small metadata files we want *content* in Git. This function identifies
    those paths so they can be fetched and `unannex`ed.
    """
    relpaths: List[Path] = []
    for root, dirs, files in os.walk(repo_dir):
        # Never touch internal dataset/metadata.
        dirs[:] = [
            d
            for d in sorted(dirs)
            if not d.startswith(".") and d not in {".git", ".datalad"}
        ]
        files.sort()
        for fname in files:
            if fname.startswith("."):
                continue
            if not any(fnmatch.fnmatch(fname, pat) for pat in METADATA_PATTERNS):
                continue
            abspath = Path(root) / fname
            try:
                relpaths.append(abspath.relative_to(repo_dir))
            except ValueError:
                # Should never happen, but keep the walk robust.
                continue
    return sorted(relpaths)


def chunked(items: List[Path], n: int) -> Iterable[List[Path]]:
    if n <= 0:
        raise ValueError("chunk size must be > 0")
    for i in range(0, len(items), n):
        yield items[i : i + n]


def run_annex_json(
    cmd: List[str],
    cwd: Path,
    *,
    dry_run: bool,
) -> Tuple[int, List[Dict[str, Any]], List[str], str]:
    """Run a git-annex command producing JSON lines.

    Returns: (returncode, parsed_records, parse_errors, stderr)
    """
    if dry_run:
        print(f"[DRYRUN] (cwd={cwd})", " ".join(cmd))
        return 0, [], [], ""

    res = subprocess.run(
        cmd,
        cwd=str(cwd),
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    records: List[Dict[str, Any]] = []
    parse_errors: List[str] = []
    for line in res.stdout.splitlines():
        s = line.strip()
        if not s:
            continue
        try:
            obj = json.loads(s)
        except json.JSONDecodeError:
            parse_errors.append(s)
            continue
        if isinstance(obj, dict):
            records.append(obj)
    return res.returncode, records, parse_errors, res.stderr


def materialize_metadata(
    repo_dir: Path,
    *,
    dry_run: bool,
    jobs: int,
    max_mb: float,
    report_path: Path,
) -> None:
    """Fetch candidate metadata and move it out of annex into Git."""
    candidates = iter_metadata_candidate_relpaths(repo_dir)
    annexed = [p for p in candidates if (repo_dir / p).is_symlink()]
    already_in_git = len(candidates) - len(annexed)

    # Best-effort report, always written (unless dry-run).
    report: Dict[str, Any] = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "repo": str(repo_dir),
        "patterns": list(METADATA_PATTERNS),
        "total_candidates": len(candidates),
        "already_in_git": already_in_git,
        "annexed_candidates": len(annexed),
        "jobs": int(jobs),
        "max_mb": float(max_mb),
        "converted_to_git": [],
        "too_large_keep_annexed": [],
        "get_failures": {},
        "unannex_failures": {},
        "parse_errors": [],
        "stderr_snippets": [],
    }

    if not annexed:
        print("[INFO] No annexed metadata candidates found; nothing to materialize.")
        if not dry_run:
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return

    # Keep chunks modest; macOS has a relatively small max argv size.
    for chunk in chunked(annexed, n=400):
        # 1) Fetch metadata content (best-effort).
        cmd_get = [
            "git",
            "annex",
            "get",
            "--json",
            "--json-error-messages",
            "--jobs",
            str(max(1, int(jobs))),
            "--",
            *[str(p) for p in chunk],
        ]
        rc, records, parse_errors, stderr = run_annex_json(cmd_get, cwd=repo_dir, dry_run=dry_run)
        report["parse_errors"].extend(parse_errors)
        if stderr.strip():
            report["stderr_snippets"].append(stderr.strip()[:4000])

        ok: set = set()
        for rec in records:
            f = rec.get("file")
            if not isinstance(f, str) or not f:
                continue
            rel = Path(f.lstrip("./"))
            if rec.get("success") is True:
                ok.add(rel)
            elif rec.get("success") is False:
                report["get_failures"][str(rel)] = rec.get("error-messages") or rec.get("error-message") or ""

        if dry_run:
            ok = set(chunk)
        elif rc != 0 and not records:
            # No JSON to interpret; fall back to assuming all failed.
            for p in chunk:
                report["get_failures"].setdefault(str(p), f"git-annex get returned {rc} (no JSON output)")
        else:
            # Some git-annex operations may not emit per-file JSON records for
            # "notneeded"/already-present paths. Treat any file with present
            # content as OK unless we recorded an explicit failure.
            for rel in chunk:
                if rel in ok or str(rel) in report["get_failures"]:
                    continue
                try:
                    (repo_dir / rel).stat()
                except FileNotFoundError:
                    report["get_failures"].setdefault(
                        str(rel),
                        "content not present after git-annex get (and no JSON record was emitted)",
                    )
                    continue
                ok.add(rel)

        # 2) Filter by size (safety) and unannex.
        to_unannex: List[Path] = []
        for rel in chunk:
            if rel not in ok:
                continue
            p = repo_dir / rel
            if dry_run:
                to_unannex.append(rel)
                continue
            try:
                size_mb = p.stat().st_size / (1024 * 1024)
            except FileNotFoundError:
                report["get_failures"].setdefault(str(rel), "content not present after successful get")
                continue

            if size_mb > max_mb:
                report["too_large_keep_annexed"].append(str(rel))
                continue
            to_unannex.append(rel)

        if not to_unannex:
            continue

        cmd_unannex = [
            "git",
            "annex",
            "unannex",
            "--json",
            "--json-error-messages",
            "--",
            *[str(p) for p in to_unannex],
        ]
        rc2, records2, parse_errors2, stderr2 = run_annex_json(cmd_unannex, cwd=repo_dir, dry_run=dry_run)
        report["parse_errors"].extend(parse_errors2)
        if stderr2.strip():
            report["stderr_snippets"].append(stderr2.strip()[:4000])

        converted: set = set()
        for rec in records2:
            f = rec.get("file")
            if not isinstance(f, str) or not f:
                continue
            rel = Path(f.lstrip("./"))
            if rec.get("success") is True:
                converted.add(rel)
            elif rec.get("success") is False:
                report["unannex_failures"][str(rel)] = rec.get("error-messages") or rec.get("error-message") or ""

        if dry_run:
            converted = set(to_unannex)
        elif rc2 != 0 and not records2:
            for p in to_unannex:
                report["unannex_failures"].setdefault(str(p), f"git-annex unannex returned {rc2} (no JSON output)")
        else:
            # Similar to get: ensure we mark files converted if they are no
            # longer annexed, even if no JSON record was emitted.
            for rel in to_unannex:
                if rel in converted or str(rel) in report["unannex_failures"]:
                    continue
                if not (repo_dir / rel).is_symlink():
                    converted.add(rel)
                else:
                    report["unannex_failures"].setdefault(
                        str(rel),
                        "still annexed after git-annex unannex (and no JSON record was emitted)",
                    )

        report["converted_to_git"].extend(sorted([str(p) for p in converted]))

    # Recompute how many are still annexed.
    if not dry_run:
        still_annexed = 0
        for p in candidates:
            if (repo_dir / p).is_symlink():
                still_annexed += 1
        report["still_annexed_after"] = still_annexed

        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(
        "[INFO] Metadata materialization: "
        f"candidates={len(candidates)} annexed={len(annexed)} already_in_git={already_in_git} "
        f"converted={len(report['converted_to_git'])} too_large={len(report['too_large_keep_annexed'])} "
        f"get_fail={len(report['get_failures'])} unannex_fail={len(report['unannex_failures'])}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a unified ABIDE I+II view as a self-contained git-annex dataset.\n"
            "Files are registered in inputs/abide-both with the *same* annex keys as the\n"
            "source datasets, and their original HTTP URL(s) are registered in the 'web'\n"
            "remote so the merged view can 'get' content directly.\n"
            "\n"
            "Additionally, BIDS sidecars are generated by copying site-level templates\n"
            "(e.g., task-rest_bold.json, T1w.json) and (optionally) adding RepetitionTime\n"
            "from the NIfTI header."
        )
    )
    parser.add_argument(
        "--project-root",
        default=os.getcwd(),
        help="Project root containing inputs/ and code/ (default: cwd).",
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help=(
            "Skip building pointers/participants/dataset_description from source datasets. "
            "Useful for running post-processing steps (e.g., --materialize-metadata) on an existing "
            "inputs/abide-both dataset without requiring inputs/abide1 or inputs/abide2."
        ),
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Delete existing sub-* trees in inputs/abide-both before rebuilding (DANGEROUS).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned operations without creating links or files.",
    )
    parser.add_argument(
        "--sidecars",
        choices=["none", "template", "tr"],
        default="template",
        help=(
            "Sidecar generation mode (default: template). "
            "'template' copies site-level JSON templates; "
            "'tr' also ensures RepetitionTime for BOLD by reading NIfTI headers."
        ),
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
        dest="force_drop",
        action="store_false",
        help="Use safe 'git annex drop' checks (default: drop with --force to free space quickly).",
    )
    parser.set_defaults(force_drop=True)
    parser.add_argument(
        "--datasets",
        default="abide1,abide2",
        help="Comma-separated list of datasets to include (default: abide1,abide2).",
    )
    parser.add_argument(
        "--materialize-metadata",
        action="store_true",
        help=(
            "Fetch BIDS metadata files in inputs/abide-both (json/tsv/bval/bvec/txt/csv) and move them "
            "out of annex into Git (best-effort, writes a report)."
        ),
    )
    parser.add_argument(
        "--metadata-jobs",
        type=int,
        default=4,
        help="Parallel jobs for 'git annex get' during metadata materialization (default: 4).",
    )
    parser.add_argument(
        "--metadata-max-mb",
        type=float,
        default=50.0,
        help="Skip unannexing a metadata file if it exceeds this size in MB (default: 50).",
    )
    parser.add_argument(
        "--metadata-report",
        default="",
        help=(
            "Path to write the JSON report (default: inputs/abide-both/.datalad/metadata_materialization_report.json)."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = Path(args.project_root).resolve()
    out_dir = project_root / "inputs" / "abide-both"
    datasets = [d.strip() for d in args.datasets.split(",") if d.strip()]

    participants: List[Tuple[str, str, str, int, str]] = []
    total_files = 0

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

    if not args.skip_build:
        if args.clean:
            clean_subject_tree(out_dir, args.dry_run)

        if "abide1" in datasets:
            total_files += build_abide(
                project_root=project_root,
                out_dir=out_dir,
                dataset_name="abide1",
                version_tag="v1",
                dry_run=args.dry_run,
                participants=participants,
                create_sidecars=(args.sidecars != "none"),
                ensure_tr=(args.sidecars == "tr"),
                force_drop=args.force_drop,
                overwrite_sidecars=args.overwrite_sidecars,
                sidecar_participant_ids=sidecar_participant_ids,
            )

        if "abide2" in datasets:
            total_files += build_abide(
                project_root=project_root,
                out_dir=out_dir,
                dataset_name="abide2",
                version_tag="v2",
                dry_run=args.dry_run,
                participants=participants,
                create_sidecars=(args.sidecars != "none"),
                ensure_tr=(args.sidecars == "tr"),
                force_drop=args.force_drop,
                overwrite_sidecars=args.overwrite_sidecars,
                sidecar_participant_ids=sidecar_participant_ids,
            )

        write_participants_tsv(out_dir, participants, args.dry_run)
        write_dataset_description(out_dir, args.dry_run)
    else:
        if args.clean:
            raise RuntimeError("--clean requires a rebuild; refuse to run with --skip-build")
        if not out_dir.exists():
            raise FileNotFoundError(f"Missing output dataset directory: {out_dir}")

    if args.materialize_metadata:
        report_path_str = (args.metadata_report or "").strip()
        if not report_path_str:
            report_path = out_dir / ".datalad" / "metadata_materialization_report.json"
        else:
            report_path = Path(report_path_str).expanduser()
            if not report_path.is_absolute():
                report_path = (project_root / report_path).resolve()

        materialize_metadata(
            out_dir,
            dry_run=args.dry_run,
            jobs=args.metadata_jobs,
            max_mb=args.metadata_max_mb,
            report_path=report_path,
        )

    if not args.skip_build:
        print(f"[INFO] Created/updated {total_files} annexed files in {out_dir}")
        print(f"[INFO] Participants: {len(participants)}")


if __name__ == "__main__":
    main()
