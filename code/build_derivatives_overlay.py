#!/usr/bin/env python3
"""Build a merged BIDS derivatives overlay dataset from per-site fMRIPrep outputs.

Creates a single flat BIDS-Derivatives root containing all ~2,149 subjects by
registering git-annex keys from the 43 site-level derivative datasets.  Content
retrieval is configured via autoenable GIN special remotes so that consumers
can ``datalad clone`` from GitHub and ``datalad get`` from GIN transparently.

The script follows the same stdlib-only, dry-run-capable pattern as
``build_abide_both.py``.
"""
import argparse
import json
import os
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MICROMAMBA_ENV = "datalad"
_ENV_PREFIX = ["micromamba", "run", "-n", MICROMAMBA_ENV]


def _wrap(cmd: List[str]) -> List[str]:
    """Prepend the micromamba environment wrapper to a command."""
    return _ENV_PREFIX + cmd


def run_cmd(
    cmd: List[str],
    cwd: Optional[Path] = None,
    *,
    capture_stdout: bool = False,
    dry_run: bool = False,
) -> str:
    wrapped = _wrap(cmd)
    if dry_run:
        prefix = f"[DRYRUN]{' (cwd=' + str(cwd) + ')' if cwd else ''}"
        print(prefix, " ".join(wrapped))
        return ""

    res = subprocess.run(
        wrapped,
        cwd=str(cwd) if cwd else None,
        check=True,
        stdout=subprocess.PIPE if capture_stdout else None,
        stderr=None,
        text=True,
    )
    return res.stdout if capture_stdout else ""


def info(msg: str) -> None:
    print(f"\033[36m[INFO]\033[0m {msg}")


def warn(msg: str) -> None:
    print(f"\033[33m[WARN]\033[0m {msg}")


def success(msg: str) -> None:
    print(f"\033[32m[OK]\033[0m {msg}")


# ---------------------------------------------------------------------------
# Site discovery
# ---------------------------------------------------------------------------

SITE_PREFIX_RE_PATTERN = "v[12]s"  # matches v1s0 .. v2s18


def discover_site_prefixes(deriv_dir: Path) -> List[str]:
    """Return sorted list of site prefix directory names under derivatives/."""
    prefixes = []
    for entry in sorted(deriv_dir.iterdir()):
        if not entry.is_dir():
            continue
        name = entry.name
        if (name.startswith("v1s") or name.startswith("v2s")) and \
                name[3:].isdigit():
            prefixes.append(name)
    return sorted(prefixes, key=_site_sort_key)


def _site_sort_key(prefix: str) -> Tuple[int, int]:
    """Sort v1s0..v1s23 then v2s0..v2s18."""
    version = int(prefix[1])
    index = int(prefix[3:])
    return (version, index)


# ---------------------------------------------------------------------------
# Git tree enumeration
# ---------------------------------------------------------------------------

def ls_tree(
    repo_dir: Path,
    ref: str,
    paths: Optional[List[str]] = None,
) -> List[Tuple[str, str, str, str]]:
    """Run ``git ls-tree -r`` and return (mode, type, hash, path) tuples."""
    cmd = ["git", "ls-tree", "-r", ref]
    if paths:
        cmd.append("--")
        cmd.extend(paths)

    out = subprocess.run(
        _wrap(cmd), cwd=str(repo_dir), check=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    ).stdout

    entries = []
    for line in out.splitlines():
        if not line:
            continue
        # format: "<mode> <type> <hash>\t<path>"
        meta, path = line.split("\t", 1)
        parts = meta.split()
        entries.append((parts[0], parts[1], parts[2], path))
    return entries


def list_subjects_on_ref(repo_dir: Path, ref: str) -> List[str]:
    """Return subject IDs (sub-XXX) present on a git ref."""
    cmd = ["git", "ls-tree", "--name-only", ref]
    out = subprocess.run(
        _wrap(cmd), cwd=str(repo_dir), check=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    ).stdout

    subjects = set()
    for name in out.splitlines():
        name = name.strip()
        if name.startswith("sub-") and not name.endswith(".html"):
            subjects.add(name)
    return sorted(subjects)


# ---------------------------------------------------------------------------
# File classification
# ---------------------------------------------------------------------------

# Extensions that are always git-tracked in the overlay
GIT_EXTENSIONS = frozenset({
    ".json", ".tsv", ".bval", ".bvec", ".txt", ".csv", ".stats",
    ".log", ".dat", ".annot", ".label", ".lta", ".cmd", ".done",
    ".touch",
})

# Extensions that are always annexed in the overlay
ANNEX_EXTENSIONS = frozenset({
    ".nii.gz", ".gii", ".h5", ".x5", ".svg", ".html",
    ".parquet", ".env",
})


def should_annex_in_overlay(relpath: str) -> bool:
    """Decide whether a file should be annexed in the overlay dataset."""
    if relpath.endswith(".nii.gz"):
        return True
    _, ext = os.path.splitext(relpath)
    if ext in ANNEX_EXTENSIONS:
        return True
    if ext in GIT_EXTENSIONS:
        return False
    # Default: annex unknown extensions
    return True


def should_include(relpath: str, mode: str) -> bool:
    """Filter out files we never want in the fMRIPrep overlay."""
    # Exclude sourcedata/freesurfer/
    if relpath.startswith("sourcedata/freesurfer/"):
        return False
    # Exclude log directories
    parts = relpath.split("/")
    if "log" in parts or "logs" in parts:
        return False
    return True


def should_include_freesurfer(relpath: str, mode: str) -> bool:
    """Filter for the FreeSurfer overlay: only sourcedata/freesurfer/."""
    if not relpath.startswith("sourcedata/freesurfer/"):
        return False
    # Exclude fsaverage (shared across all subjects)
    if relpath.startswith("sourcedata/freesurfer/fsaverage/"):
        return False
    return True


# ---------------------------------------------------------------------------
# Batch git-annex operations
# ---------------------------------------------------------------------------

def batch_lookupkey(
    repo_dir: Path,
    relpaths: List[str],
) -> List[str]:
    """Look up annex keys for a list of paths using --batch mode."""
    if not relpaths:
        return []

    proc = subprocess.run(
        _wrap(["git", "annex", "lookupkey", "--batch"]),
        cwd=str(repo_dir),
        input="\n".join(relpaths) + "\n",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    keys = proc.stdout.strip().splitlines()
    if len(keys) != len(relpaths):
        raise RuntimeError(
            f"lookupkey returned {len(keys)} keys for {len(relpaths)} paths "
            f"in {repo_dir}"
        )
    return keys


def batch_fromkey(
    overlay_dir: Path,
    pairs: List[Tuple[str, str]],
    dry_run: bool = False,
) -> None:
    """Create annex pointers from (key, dest_relpath) pairs using --batch."""
    if not pairs:
        return

    if dry_run:
        print(f"[DRYRUN] git annex fromkey --force --batch ({len(pairs)} files)")
        return

    # Ensure parent directories exist
    for _, dest in pairs:
        parent = overlay_dir / Path(dest).parent
        parent.mkdir(parents=True, exist_ok=True)

    stdin_data = "\n".join(f"{key} {dest}" for key, dest in pairs) + "\n"
    proc = subprocess.run(
        _wrap(["git", "annex", "fromkey", "--force", "--batch"]),
        cwd=str(overlay_dir),
        input=stdin_data,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if proc.returncode != 0:
        failed_lines = [
            line for line in proc.stderr.splitlines() if line.strip()
        ]
        if failed_lines:
            warn(f"fromkey errors: {failed_lines[:5]}")


def batch_setpresentkey(
    overlay_dir: Path,
    triples: List[Tuple[str, str, str]],
    dry_run: bool = False,
) -> None:
    """Mark keys as present on a remote UUID using --batch mode.

    Each triple is (key, uuid, "1"|"0").
    """
    if not triples:
        return

    if dry_run:
        print(f"[DRYRUN] git annex setpresentkey --batch ({len(triples)} keys)")
        return

    stdin_data = "\n".join(f"{k} {u} {v}" for k, u, v in triples) + "\n"
    subprocess.run(
        _wrap(["git", "annex", "setpresentkey", "--batch"]),
        cwd=str(overlay_dir),
        input=stdin_data,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )


def batch_registerurl(
    overlay_dir: Path,
    pairs: List[Tuple[str, str]],
    dry_run: bool = False,
) -> None:
    """Register URLs for keys using --batch mode.

    Each pair is (key, url).
    """
    if not pairs:
        return

    if dry_run:
        print(f"[DRYRUN] git annex registerurl --batch ({len(pairs)} URLs)")
        return

    stdin_data = "\n".join(f"{k} {u}" for k, u in pairs) + "\n"
    subprocess.run(
        _wrap(["git", "annex", "registerurl", "--batch"]),
        cwd=str(overlay_dir),
        input=stdin_data,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )


# ---------------------------------------------------------------------------
# Reading git-tracked content from source
# ---------------------------------------------------------------------------

def git_show(repo_dir: Path, ref: str, relpath: str) -> bytes:
    """Read file content from a git ref without checking out."""
    proc = subprocess.run(
        _wrap(["git", "show", f"{ref}:{relpath}"]),
        cwd=str(repo_dir),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    )
    return proc.stdout


# ---------------------------------------------------------------------------
# Site lookup (prefix → dataset + site name)
# ---------------------------------------------------------------------------

def load_site_lookup(project_root: Path) -> Dict[str, Tuple[str, str]]:
    """Parse inputs/abide-both/participants.tsv → {prefix: (source_dataset, source_site)}.

    Extracts the prefix from participant_id (e.g., ``sub-v1s0x0050642`` → ``v1s0``)
    and pairs it with the ``source_dataset`` (column 2) and ``source_site`` (column 3).
    """
    tsv = project_root / "inputs" / "abide-both" / "participants.tsv"
    lookup: Dict[str, Tuple[str, str]] = {}
    with open(tsv, encoding="utf-8") as fh:
        header = fh.readline()  # skip header
        for line in fh:
            cols = line.rstrip("\n").split("\t")
            if len(cols) < 3:
                continue
            pid = cols[0]  # e.g., sub-v1s0x0050642
            prefix = pid.removeprefix("sub-").split("x", 1)[0]  # v1s0
            if prefix not in lookup:
                lookup[prefix] = (cols[1], cols[2])
    return lookup


# ---------------------------------------------------------------------------
# Overlay metadata generation
# ---------------------------------------------------------------------------

def append_participants_tsv(
    overlay_dir: Path,
    site_dir: Path,
    ref: str,
    dry_run: bool,
) -> int:
    """Append data rows from a site's participants.tsv to the overlay's.

    Returns the number of rows appended.
    """
    try:
        content = git_show(site_dir, ref, "participants.tsv").decode("utf-8")
    except subprocess.CalledProcessError:
        warn(f"  No participants.tsv on {ref} in {site_dir.name}")
        return 0

    lines = content.splitlines()
    if not lines:
        return 0

    header = lines[0]
    data_rows = lines[1:]
    if not data_rows:
        return 0

    dest = overlay_dir / "participants.tsv"

    if dry_run:
        print(f"[DRYRUN] Append {len(data_rows)} rows to {dest.name}")
        return len(data_rows)

    if not dest.exists():
        dest.write_text(header + "\n", encoding="utf-8")

    with open(dest, "a", encoding="utf-8") as fh:
        for row in data_rows:
            fh.write(row + "\n")

    return len(data_rows)


def copy_participants_json(
    overlay_dir: Path,
    site_dir: Path,
    ref: str,
    dry_run: bool,
) -> None:
    """Copy participants.json from the first site to the overlay (once)."""
    dest = overlay_dir / "participants.json"
    if dest.exists():
        return

    try:
        content = git_show(site_dir, ref, "participants.json")
    except subprocess.CalledProcessError:
        warn(f"  No participants.json on {ref} in {site_dir.name}")
        return

    if dry_run:
        print(f"[DRYRUN] Copy participants.json from {site_dir.name}")
        return

    dest.write_bytes(content)


def write_readme(
    overlay_dir: Path,
    site_prefixes: List[str],
    site_lookup: Dict[str, Tuple[str, str]],
    gin_org: str,
    dry_run: bool,
) -> None:
    """Write/rewrite README.md listing all integrated sites with GIN URLs."""
    abide1 = []
    abide2 = []
    for prefix in site_prefixes:
        ds, site_name = site_lookup.get(prefix, ("unknown", "unknown"))
        url = f"https://gin.g-node.org/{gin_org}/{prefix}"
        row = f"| {site_name} | `{prefix}` | <{url}> |"
        if ds == "abide1":
            abide1.append(row)
        else:
            abide2.append(row)

    table_header = "| Site | Prefix | GIN repository |\n|------|--------|----------------|"
    sections = []
    if abide1:
        sections.append(
            "### ABIDE I\n\n"
            + table_header + "\n"
            + "\n".join(abide1)
        )
    if abide2:
        sections.append(
            "### ABIDE II\n\n"
            + table_header + "\n"
            + "\n".join(abide2)
        )

    text = (
        "# ABIDE I+II fMRIPrep Derivatives (Merged View)\n\n"
        "Merged overlay of fMRIPrep 25.2.4 outputs from the "
        "ABIDE I and ABIDE II datasets.\n\n"
        "Content is stored in per-site DataLad datasets on GIN.\n"
        "To retrieve file content after cloning, run "
        "`git annex init` followed by `datalad get`.\n\n"
        "## Source datasets\n\n"
        + "\n\n".join(sections) + "\n"
    )

    dest = overlay_dir / "README.md"
    if dry_run:
        print(f"[DRYRUN] Write {dest.name} ({len(site_prefixes)} sites)")
        return

    dest.write_text(text, encoding="utf-8")


def prepend_changes(
    overlay_dir: Path,
    entries: List[Tuple[str, str, str, int, int]],
    dry_run: bool,
) -> None:
    """Prepend new entries to CHANGES (most recent first).

    Each entry is (site_prefix, dataset_label, site_name, n_subjects, n_files).
    """
    if not entries:
        return

    today = date.today().isoformat()
    lines = [today, ""]
    for prefix, ds_label, site_name, n_subj, n_files in entries:
        lines.append(
            f"  - Integrated site {prefix} "
            f"({ds_label} / {site_name}): "
            f"{n_subj} subjects, {n_files} files"
        )
    lines.append("")
    new_block = "\n".join(lines)

    dest = overlay_dir / "CHANGES"
    if dry_run:
        print(f"[DRYRUN] Prepend {len(entries)} entries to {dest.name}")
        return

    existing = dest.read_text(encoding="utf-8") if dest.exists() else ""
    dest.write_text(new_block + "\n" + existing if existing else new_block + "\n",
                    encoding="utf-8")


# ---------------------------------------------------------------------------
# GIN remote configuration
# ---------------------------------------------------------------------------

def get_gin_annex_uuid(site_dir: Path) -> Optional[str]:
    """Get the git-annex UUID of the 'gin' remote in a site dataset."""
    try:
        out = subprocess.run(
            _wrap(["git", "config", "--get", "remote.gin.annex-uuid"]),
            cwd=str(site_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        ).stdout.strip()
        return out if out else None
    except subprocess.CalledProcessError:
        return None


def register_gin_remote(
    overlay_dir: Path,
    site_prefix: str,
    gin_org: str,
    uuid: str,
    dry_run: bool = False,
) -> None:
    """Register a GIN repo as an autoenable special remote in the overlay."""
    remote_name = f"gin-{site_prefix}"
    location = f"https://gin.g-node.org/{gin_org}/{site_prefix}"

    # Check if already registered
    try:
        existing = subprocess.run(
            _wrap(["git", "annex", "enableremote", remote_name]),
            cwd=str(overlay_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )
        info(f"  Remote {remote_name} already enabled")
        return
    except subprocess.CalledProcessError:
        pass

    cmd = [
        "git", "annex", "initremote", remote_name,
        "type=git",
        f"location={location}",
        "autoenable=true",
    ]
    run_cmd(cmd, cwd=overlay_dir, dry_run=dry_run)


# ---------------------------------------------------------------------------
# Overlay metadata files
# ---------------------------------------------------------------------------

OVERLAY_GITATTRIBUTES = """\
* annex.backend=MD5E
**/.git* annex.largefiles=nothing

# Metadata -> git
*.json annex.largefiles=nothing
*.tsv annex.largefiles=nothing
*.bval annex.largefiles=nothing
*.bvec annex.largefiles=nothing
*.txt annex.largefiles=nothing
*.csv annex.largefiles=nothing
*.stats annex.largefiles=nothing
*.log annex.largefiles=nothing
*.dat annex.largefiles=nothing
*.annot annex.largefiles=nothing
*.label annex.largefiles=nothing
*.lta annex.largefiles=nothing
*.cmd annex.largefiles=nothing
*.done annex.largefiles=nothing
*.touch annex.largefiles=nothing
.bidsignore annex.largefiles=nothing
dataset_description.json annex.largefiles=nothing
CHANGES annex.largefiles=nothing
LICENSE annex.largefiles=nothing
README* annex.largefiles=nothing

# Large files -> annex (includes HTML, unlike site datasets)
*.env annex.largefiles=anything
*.gii annex.largefiles=anything
*.h5 annex.largefiles=anything
*.html annex.largefiles=anything
*.nii.gz annex.largefiles=anything
*.parquet annex.largefiles=anything
*.svg annex.largefiles=anything
*.x5 annex.largefiles=anything
"""

OVERLAY_DATASET_DESCRIPTION = {
    "Name": "ABIDE I+II fMRIPrep derivatives (merged view)",
    "BIDSVersion": "1.10.0",
    "DatasetType": "derivative",
    "GeneratedBy": [
        {
            "Name": "fMRIPrep",
            "Version": "25.2.4",
            "CodeURL": "https://github.com/nipreps/fmriprep/archive/25.2.4.tar.gz",
        },
        {
            "Name": "build_derivatives_overlay",
            "Description": "Merged overlay built from per-site DataLad datasets",
        },
    ],
    "HowToAcknowledge": "Please cite https://doi.org/10.1038/s41592-018-0235-4",
}

OVERLAY_BIDSIGNORE = """\
logs/
figures/
*_xfm.*
*from-*_to-*
*space-fsLR*
*space-fsnative*
*space-fsaverage*
"""


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------

def init_overlay(
    project_root: Path,
    overlay_path: Path,
    github_org: str,
    dry_run: bool,
) -> None:
    """Create the overlay DataLad dataset and optional GitHub sibling."""
    overlay_abs = project_root / overlay_path

    if overlay_abs.exists() and (overlay_abs / ".datalad" / "config").exists():
        info(f"Overlay dataset already exists at {overlay_abs}")
        return

    info(f"Creating overlay dataset: {overlay_abs}")
    run_cmd(
        ["datalad", "create", "-d", str(project_root), str(overlay_abs)],
        dry_run=dry_run,
    )

    if dry_run:
        info("[DRYRUN] Would write .gitattributes, dataset_description.json, .bidsignore")
        return

    # Write .gitattributes
    (overlay_abs / ".gitattributes").write_text(
        OVERLAY_GITATTRIBUTES, encoding="utf-8"
    )

    # Write dataset_description.json
    (overlay_abs / "dataset_description.json").write_text(
        json.dumps(OVERLAY_DATASET_DESCRIPTION, indent=4) + "\n",
        encoding="utf-8",
    )

    # Write .bidsignore
    (overlay_abs / ".bidsignore").write_text(
        OVERLAY_BIDSIGNORE, encoding="utf-8"
    )

    # Save
    run_cmd(
        ["datalad", "save", "-d", str(overlay_abs),
         "-m", "Initialize merged derivatives overlay"],
        dry_run=False,
    )

    # Create GitHub sibling
    repo_name = overlay_path.name
    info(f"Creating GitHub sibling: {github_org}/{repo_name}")
    try:
        run_cmd(
            ["datalad", "create-sibling-github",
             "-d", str(overlay_abs),
             "--name", "github",
             "--access-protocol", "https-ssh",
             "--existing", "skip",
             "--publish-depends", "gin",
             f"{github_org}/{repo_name}"],
            dry_run=False,
        )
    except subprocess.CalledProcessError as e:
        warn(f"GitHub sibling creation failed (may need manual setup): {e}")

    # Save superdataset
    run_cmd(
        ["datalad", "save", "-d", str(project_root),
         "-m", f"Register overlay dataset {overlay_path}"],
        dry_run=False,
    )

    success(f"Overlay dataset initialized: {overlay_abs}")


# ---------------------------------------------------------------------------
# Core build logic
# ---------------------------------------------------------------------------

def process_site(
    project_root: Path,
    overlay_dir: Path,
    site_prefix: str,
    ref: str,
    gin_org: str,
    gin_uuid: Optional[str],
    mode: str,
    dry_run: bool,
    batch_size: int,
) -> Tuple[int, int, int, List[str]]:
    """Process one site dataset: enumerate files, transfer to overlay.

    Returns (annexed_count, git_count, skipped_count, new_subjects).
    """
    site_dir = project_root / "derivatives" / site_prefix

    # Discover subjects on the ref
    subjects = list_subjects_on_ref(site_dir, ref)
    if not subjects:
        return (0, 0, 0, [])

    # Find subjects already in the overlay
    existing: set = set()
    if overlay_dir.exists() and (overlay_dir / ".git").exists():
        try:
            existing = set(list_subjects_on_ref(overlay_dir, "HEAD"))
        except subprocess.CalledProcessError:
            pass

    new_subjects = [s for s in subjects if s not in existing]
    if not new_subjects:
        info(f"  {site_prefix}: all {len(subjects)} subjects already in overlay")
        return (0, 0, 0, [])

    info(
        f"  {site_prefix}: {len(new_subjects)} new subjects "
        f"(of {len(subjects)} total)"
    )

    # Build path list for ls-tree: subject dirs + HTML reports
    ls_paths = []
    for subj in new_subjects:
        ls_paths.append(subj)
        ls_paths.append(f"{subj}.html")

    # Enumerate all files
    entries = ls_tree(site_dir, ref, ls_paths)

    # Apply mode filter
    if mode == "fmriprep":
        filter_fn = should_include
    elif mode == "freesurfer":
        filter_fn = should_include_freesurfer
    else:
        filter_fn = should_include

    entries = [(m, t, h, p) for m, t, h, p in entries if filter_fn(p, m)]

    if not entries:
        return (0, 0, 0, new_subjects)

    # Classify: annexed (symlink, mode 120000) vs git-tracked (mode 100644)
    annexed_entries = []
    git_entries = []
    for file_mode, _, _, relpath in entries:
        if file_mode == "120000":
            annexed_entries.append(relpath)
        elif file_mode == "100644":
            # Decide whether this should be annexed in the overlay
            if should_annex_in_overlay(relpath):
                # Git-tracked in source but annexed in overlay (e.g., HTML)
                git_entries.append((relpath, True))
            else:
                # Git-tracked in both source and overlay
                git_entries.append((relpath, False))

    annexed_count = 0
    git_count = 0
    skipped_count = 0

    # --- Process annexed files in batches ---
    for i in range(0, len(annexed_entries), batch_size):
        batch = annexed_entries[i:i + batch_size]

        if dry_run:
            print(
                f"[DRYRUN] batch lookupkey + fromkey: "
                f"{len(batch)} annexed files from {site_prefix}"
            )
            annexed_count += len(batch)
            continue

        # Look up keys in source
        keys = batch_lookupkey(site_dir, batch)

        # Skip files already in overlay
        fromkey_pairs = []
        present_triples = []
        for relpath, key in zip(batch, keys):
            dest = overlay_dir / relpath
            if os.path.lexists(dest):
                skipped_count += 1
                continue
            fromkey_pairs.append((key, relpath))
            if gin_uuid:
                present_triples.append((key, gin_uuid, "1"))

        if fromkey_pairs:
            batch_fromkey(overlay_dir, fromkey_pairs)
            annexed_count += len(fromkey_pairs)

        if present_triples:
            batch_setpresentkey(overlay_dir, present_triples)

    # --- Process git-tracked files ---
    for i in range(0, len(git_entries), batch_size):
        batch = git_entries[i:i + batch_size]

        for relpath, annex_in_overlay in batch:
            dest = overlay_dir / relpath
            if os.path.lexists(dest):
                skipped_count += 1
                continue

            if dry_run:
                print(
                    f"[DRYRUN] git show {ref}:{relpath} -> "
                    f"{'annex' if annex_in_overlay else 'git'}"
                )
                git_count += 1
                continue

            # Read content from source
            content = git_show(site_dir, ref, relpath)

            # Write to overlay
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(content)
            git_count += 1

    return (annexed_count, git_count, skipped_count, new_subjects)


def fetch_site_ref(site_dir: Path, ref: str) -> str:
    """Ensure the requested ref is available locally. Returns the ref to use."""
    # If ref is "master" or "main", try to fast-forward from remotes
    if ref in ("master", "main"):
        for remote in ("gin", "github"):
            try:
                subprocess.run(
                    _wrap(["git", "fetch", remote, ref]),
                    cwd=str(site_dir),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    check=True,
                )
                # Merge if we're on the same branch
                current = subprocess.run(
                    _wrap(["git", "branch", "--show-current"]),
                    cwd=str(site_dir),
                    stdout=subprocess.PIPE,
                    text=True,
                    check=True,
                ).stdout.strip()
                if current == ref:
                    subprocess.run(
                        _wrap(["git", "merge", "--ff-only", f"{remote}/{ref}"]),
                        cwd=str(site_dir),
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        check=False,
                    )
                return ref
            except subprocess.CalledProcessError:
                continue
    return ref


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a merged BIDS derivatives overlay from per-site fMRIPrep "
            "datasets. Registers git-annex keys and configures GIN remotes "
            "for transparent content retrieval."
        ),
    )
    parser.add_argument(
        "--project-root",
        default=os.getcwd(),
        help="YODA superdataset root (default: cwd).",
    )
    parser.add_argument(
        "--init",
        action="store_true",
        help="Create overlay DataLad dataset and GitHub sibling (first run).",
    )
    parser.add_argument(
        "--mode",
        choices=["fmriprep", "freesurfer"],
        default="fmriprep",
        help="Content scope: fmriprep (default) or freesurfer.",
    )
    parser.add_argument(
        "--site",
        action="append",
        dest="sites",
        default=[],
        help="Process only these site prefixes (repeatable). Default: all.",
    )
    parser.add_argument(
        "--overlay-path",
        default=None,
        help=(
            "Overlay dataset path relative to project root "
            "(default: derivatives/derivatives-fmriprep or "
            "derivatives/derivatives-freesurfer based on --mode)."
        ),
    )
    parser.add_argument(
        "--gin-org",
        default="abide-fmriprep",
        help="GIN organization for site repos (default: abide-fmriprep).",
    )
    parser.add_argument(
        "--github-org",
        default="abide-fmriprep",
        help="GitHub organization for overlay repo (default: abide-fmriprep).",
    )
    parser.add_argument(
        "--ref",
        default="master",
        help="Git ref to read from site datasets (default: master).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print actions without executing.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Batch size for git-annex operations (default: 500).",
    )
    parser.add_argument(
        "--fetch",
        action="store_true",
        help="Fetch latest master from remotes before processing each site.",
    )
    parser.add_argument(
        "--register-gin-remotes",
        action="store_true",
        help="Register GIN repos as autoenable special remotes in the overlay.",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    project_root = Path(args.project_root).resolve()
    deriv_dir = project_root / "derivatives"

    # Determine overlay path
    if args.overlay_path:
        overlay_path = Path(args.overlay_path)
    elif args.mode == "freesurfer":
        overlay_path = Path("derivatives/derivatives-freesurfer")
    else:
        overlay_path = Path("derivatives/derivatives-fmriprep")

    overlay_dir = project_root / overlay_path

    # --- Init mode ---
    if args.init:
        init_overlay(project_root, overlay_path, args.github_org, args.dry_run)
        if not args.sites and not args.register_gin_remotes:
            return

    # Verify overlay exists (skip check in dry-run mode)
    overlay_exists = overlay_dir.exists() and (overlay_dir / ".git").exists()
    if not args.dry_run and not overlay_exists:
        print(
            f"Error: overlay dataset not found at {overlay_dir}\n"
            f"Run with --init first.",
            file=sys.stderr,
        )
        sys.exit(1)

    # Discover sites
    if args.sites:
        site_prefixes = sorted(args.sites, key=_site_sort_key)
    else:
        site_prefixes = discover_site_prefixes(deriv_dir)

    info(f"Processing {len(site_prefixes)} site(s), mode={args.mode}")

    # --- Phase: Register GIN remotes ---
    if args.register_gin_remotes:
        info("Registering GIN special remotes in overlay...")
        for site_prefix in site_prefixes:
            site_dir = project_root / "derivatives" / site_prefix
            uuid = get_gin_annex_uuid(site_dir)
            if not uuid:
                warn(f"  {site_prefix}: no GIN annex UUID found, skipping")
                continue
            register_gin_remote(
                overlay_dir, site_prefix, args.gin_org, uuid, args.dry_run
            )
            success(f"  {site_prefix}: registered (UUID={uuid[:8]}...)")

    # --- Phase: Collect GIN UUIDs for setpresentkey ---
    gin_uuids: Dict[str, Optional[str]] = {}
    for site_prefix in site_prefixes:
        site_dir = project_root / "derivatives" / site_prefix
        gin_uuids[site_prefix] = get_gin_annex_uuid(site_dir)

    # --- Phase: Load site lookup ---
    site_lookup = load_site_lookup(project_root)

    # --- Phase: Process each site ---
    total_annexed = 0
    total_git = 0
    total_skipped = 0
    sites_processed = 0
    changes_entries: List[Tuple[str, str, str, int, int]] = []

    for site_prefix in site_prefixes:
        site_dir = project_root / "derivatives" / site_prefix

        if not site_dir.exists():
            warn(f"  {site_prefix}: directory not found, skipping")
            continue

        # Optionally fetch latest from remote
        if args.fetch:
            info(f"  {site_prefix}: fetching {args.ref}...")
            fetch_site_ref(site_dir, args.ref)

        annexed, git_tracked, skipped, new_subjects = process_site(
            project_root=project_root,
            overlay_dir=overlay_dir,
            site_prefix=site_prefix,
            ref=args.ref,
            gin_org=args.gin_org,
            gin_uuid=gin_uuids.get(site_prefix),
            mode=args.mode,
            dry_run=args.dry_run,
            batch_size=args.batch_size,
        )
        total_annexed += annexed
        total_git += git_tracked
        total_skipped += skipped
        if annexed + git_tracked > 0:
            sites_processed += 1

            # Append participants.tsv rows
            n_rows = append_participants_tsv(
                overlay_dir, site_dir, args.ref, args.dry_run,
            )
            if n_rows:
                info(f"  {site_prefix}: appended {n_rows} participants.tsv rows")

            # Copy participants.json (first time only)
            copy_participants_json(overlay_dir, site_dir, args.ref, args.dry_run)

            # Collect entry for CHANGES
            ds_label, site_name = site_lookup.get(
                site_prefix, ("unknown", "unknown"),
            )
            ds_label = {"abide1": "ABIDE I", "abide2": "ABIDE II"}.get(
                ds_label, ds_label,
            )
            changes_entries.append((
                site_prefix, ds_label, site_name,
                len(new_subjects), annexed + git_tracked,
            ))

    # --- Phase: Write overlay metadata files ---
    if sites_processed > 0:
        write_readme(
            overlay_dir, site_prefixes, site_lookup, args.gin_org, args.dry_run,
        )
        prepend_changes(overlay_dir, changes_entries, args.dry_run)

    # --- Phase: Save overlay ---
    if not args.dry_run and (total_annexed + total_git) > 0:
        info("Saving overlay dataset...")
        # Stage everything
        subprocess.run(
            _wrap(["git", "add", "."]),
            cwd=str(overlay_dir),
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        # Commit
        msg = (
            f"Build merged {args.mode} overlay "
            f"({total_annexed + total_git} files from "
            f"{sites_processed} site(s))"
        )
        subprocess.run(
            _wrap(["git", "commit", "-m", msg]),
            cwd=str(overlay_dir),
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    # --- Summary ---
    print()
    info("===== Summary =====")
    info(f"Mode:    {args.mode}")
    info(f"Overlay: {overlay_dir}")
    info(f"Sites:   {len(site_prefixes)}")
    success(f"Annexed: {total_annexed}")
    success(f"Git:     {total_git}")
    if total_skipped:
        warn(f"Skipped: {total_skipped} (already present)")
    info(f"Total:   {total_annexed + total_git}")


if __name__ == "__main__":
    main()
