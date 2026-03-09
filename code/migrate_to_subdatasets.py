#!/usr/bin/env python3
"""
Migrate legacy fMRIPrep derivatives to site-level DataLad subdatasets.

Cherry-picks [DATALAD RUNCMD] commits from the monolithic legacy dataset
into the 43 site-level datasets, preserving provenance (commit message,
author, timestamps). Transfers annex content via git-annex.

Usage:
    micromamba run -n datalad code/migrate_to_subdatasets.py \
        --project-root . \
        --legacy-path /scratch/oesteban/_legacy-fmriprep-25.2 \
        [--site v1s0] [--subject sub-v1s0x0050642] \
        [--dry-run] [--resume] [--skip-push] [--ensure-content]

Prerequisites:
    - Legacy dataset accessible at --legacy-path
    - Site datasets exist under derivatives/ (created by create_site_datasets.sh)
    - Both repos use MD5E annex backend (content-addressed, so keys match)
"""

import argparse
import os
import subprocess
import sys
import time


def run(cmd, *, cwd=None, check=True, dry_run=False, capture=False):
    """Run a shell command, optionally in dry-run mode."""
    if dry_run:
        print(f"  [DRY-RUN] {' '.join(cmd)}")
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
    print(f"  $ {' '.join(cmd)}")
    return subprocess.run(
        cmd,
        cwd=cwd,
        check=check,
        capture_output=capture,
        text=True,
    )


def run_capture(cmd, *, cwd=None, check=True):
    """Run a command and return its stdout (stripped)."""
    result = subprocess.run(
        cmd, cwd=cwd, check=check, capture_output=True, text=True,
    )
    return result.stdout.strip()


def extract_site_prefix(subject_id):
    """Extract site prefix from subject ID: sub-v1s0x0050642 → v1s0."""
    bare = subject_id.removeprefix("sub-")
    return bare.split("x")[0]


# --------------------------------------------------------------------------
# Phase 0 helpers
# --------------------------------------------------------------------------

def load_submission_list(project_root):
    """Auto-discover and load the most recent submission list.

    Looks for lists/curnagl-*.txt files and loads all subject IDs.
    Returns the set of subject IDs and the path used.
    """
    lists_dir = os.path.join(project_root, "lists")
    if not os.path.isdir(lists_dir):
        return set(), None

    # Find the most recent curnagl list (sorted by name → date suffix)
    candidates = sorted(
        f for f in os.listdir(lists_dir)
        if f.startswith("curnagl-") and f.endswith(".txt")
    )
    if not candidates:
        return set(), None

    list_file = os.path.join(lists_dir, candidates[-1])
    subjects = set()
    with open(list_file) as f:
        for line in f:
            sid = line.strip()
            if sid and not sid.startswith("#"):
                if not sid.startswith("sub-"):
                    sid = f"sub-{sid}"
                subjects.add(sid)
    return subjects, list_file


def check_subject_on_master(site_path, subject_id):
    """Check if a subject directory exists on master/HEAD in the site dataset."""
    try:
        output = run_capture(
            ["git", "ls-tree", "--name-only", "HEAD", "--", subject_id],
            cwd=site_path,
        )
        return subject_id in output.splitlines()
    except subprocess.CalledProcessError:
        return False


def check_job_branch_exists(site_path, subject_id):
    """Check if a job branch exists for this subject (local or remote)."""
    branch = f"job/{subject_id}"
    try:
        # Check local branches
        output = run_capture(
            ["git", "branch", "--list", branch],
            cwd=site_path,
        )
        if output.strip():
            return True
        # Check remote branches (gin, origin, etc.)
        output = run_capture(
            ["git", "branch", "-r", "--list", f"*/{branch}"],
            cwd=site_path,
        )
        return bool(output.strip())
    except subprocess.CalledProcessError:
        return False


def identify_submitted_subjects(project_root, deriv_path, site_subjects):
    """Phase 0: identify subjects from the submission list that must be excluded.

    Returns a dict with:
        submitted: set of all subjects in the submission list
        merged: set of submitted subjects already on master in site datasets
        running: set of submitted subjects with job branches (running or awaiting merge)
        list_file: path to the submission list used
    """
    submitted, list_file = load_submission_list(project_root)
    if not submitted:
        return {
            "submitted": set(),
            "merged": set(),
            "running": set(),
            "list_file": None,
        }

    # Only check subjects that are also in the legacy inventory
    all_legacy = set()
    for subs in site_subjects.values():
        all_legacy.update(subs)
    overlap = submitted & all_legacy

    merged = set()
    running = set()
    for subject_id in sorted(overlap):
        site_prefix = extract_site_prefix(subject_id)
        site_path = os.path.join(deriv_path, site_prefix)
        if not os.path.isdir(site_path):
            continue

        if check_subject_on_master(site_path, subject_id):
            merged.add(subject_id)
        elif check_job_branch_exists(site_path, subject_id):
            running.add(subject_id)

    return {
        "submitted": submitted,
        "merged": merged,
        "running": running,
        "list_file": list_file,
    }


# --------------------------------------------------------------------------
# Phase 1–2 helpers
# --------------------------------------------------------------------------

def inventory_legacy(legacy_path):
    """List all subject directories on master in the legacy dataset.

    Returns a dict mapping site_prefix → list of subject_ids.
    """
    output = run_capture(
        ["git", "ls-tree", "--name-only", "-d", "master"],
        cwd=legacy_path,
    )
    site_subjects = {}
    for entry in output.splitlines():
        if not entry.startswith("sub-"):
            continue
        subject_id = entry
        site_prefix = extract_site_prefix(subject_id)
        site_subjects.setdefault(site_prefix, []).append(subject_id)

    for site in site_subjects:
        site_subjects[site].sort()
    return site_subjects


def load_porting_list(tsv_path):
    """Read a porting-list TSV → dict[site_prefix, list[subject_id]].

    Expected columns: participant_id, site_prefix, source_dataset.
    The third column (source_dataset) is informational and ignored here.
    """
    site_subjects = {}
    with open(tsv_path) as f:
        f.readline()  # skip header
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) < 2:
                continue
            subject_id, site_prefix = parts[0], parts[1]
            site_subjects.setdefault(site_prefix, []).append(subject_id)
    for site in site_subjects:
        site_subjects[site].sort()
    return site_subjects


def inventory_freesurfer(legacy_path, subject_id):
    """Find FreeSurfer session dirs for a subject in the legacy dataset."""
    prefix = f"{subject_id}_ses-"
    try:
        output = run_capture(
            ["git", "ls-tree", "--name-only", "-d", "master",
             "--", "sourcedata/freesurfer/"],
            cwd=legacy_path,
        )
    except subprocess.CalledProcessError:
        return []

    fs_dirs = []
    for entry in output.splitlines():
        name = entry.split("/")[-1]  # strip sourcedata/freesurfer/ prefix
        if name.startswith(prefix):
            fs_dirs.append(name)
    return sorted(fs_dirs)


def find_runcmd_commit(legacy_path, subject_id):
    """Find the [DATALAD RUNCMD] commit for a subject in the legacy history.

    Searches for commits that touched the subject's files and contain
    the RUNCMD marker in the commit message.
    """
    search_paths = [f"{subject_id}/", f"{subject_id}.html"]
    fs_dirs = inventory_freesurfer(legacy_path, subject_id)
    for fs_dir in fs_dirs:
        search_paths.append(f"sourcedata/freesurfer/{fs_dir}/")

    try:
        output = run_capture(
            ["git", "log", "--all", "--format=%H", "--"] + search_paths,
            cwd=legacy_path,
        )
    except subprocess.CalledProcessError:
        return None

    for commit_hash in output.splitlines():
        if not commit_hash:
            continue
        msg = run_capture(
            ["git", "show", "--format=%B", "-s", commit_hash],
            cwd=legacy_path,
        )
        if "[DATALAD RUNCMD]" in msg:
            return commit_hash

    return None


# --------------------------------------------------------------------------
# Phase 3 helpers
# --------------------------------------------------------------------------

def setup_legacy_remote(site_path, legacy_path, *, dry_run=False):
    """Add the legacy repo as a git remote.

    The legacy path is a git repo (not a directory special remote).
    After ``git remote add`` + ``git fetch``, git-annex discovers
    the remote's UUID from the fetched ``git-annex`` branch and can
    transfer content directly — no ``initremote`` / ``enableremote``
    needed.
    """
    # Check if remote already exists
    try:
        existing = run_capture(
            ["git", "remote", "get-url", "legacy"],
            cwd=site_path,
        )
        if existing:
            print(f"  [INFO] Legacy remote already configured: {existing}")
            run(["git", "fetch", "legacy"],
                cwd=site_path, dry_run=dry_run)
            return
    except subprocess.CalledProcessError:
        pass

    run(["git", "remote", "add", "legacy", legacy_path],
        cwd=site_path, dry_run=dry_run)
    run(["git", "fetch", "legacy"],
        cwd=site_path, dry_run=dry_run)


def cleanup_legacy_remote(site_path, *, dry_run=False):
    """Remove the legacy git remote."""
    run(["git", "remote", "remove", "legacy"],
        cwd=site_path, check=False, dry_run=dry_run)


def abort_cherry_pick(site_path):
    """Abort any in-progress cherry-pick."""
    subprocess.run(
        ["git", "cherry-pick", "--abort"],
        cwd=site_path, capture_output=True,
    )


def reset_dirty_index(site_path):
    """Reset a dirty index/worktree back to HEAD."""
    subprocess.run(
        ["git", "reset", "--hard", "HEAD"],
        cwd=site_path, capture_output=True,
    )
    subprocess.run(
        ["git", "clean", "-fd"],
        cwd=site_path, capture_output=True,
    )


def cherry_pick_subject(site_path, legacy_path, subject_id, commit_hash,
                         *, dry_run=False):
    """Cherry-pick a subject's RUNCMD commit, keeping only subject files.

    Cherry-pick will typically conflict on shared files (dataset_description,
    CITATION, fsaverage, etc.) because those have different base content in
    the site dataset vs the legacy monolithic dataset.  Subject-specific
    files (new files) are cleanly staged.  We tolerate conflicts, discard
    all non-subject paths (staged *and* unmerged), and commit only subject
    files with the original message/author/timestamp.

    Returns True on success, False on failure.
    """
    if dry_run:
        print(f"  [DRY-RUN] cherry-pick {commit_hash[:12]} for {subject_id}")
        return True

    # Abort any prior interrupted cherry-pick
    abort_cherry_pick(site_path)

    # Cherry-pick --no-commit.  Conflicts on shared files are EXPECTED.
    result = subprocess.run(
        ["git", "cherry-pick", "--no-commit", commit_hash],
        cwd=site_path,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"  [INFO] Cherry-pick had conflicts (expected for shared files)")

    # Determine which paths belong to this subject
    fs_dirs = inventory_freesurfer(legacy_path, subject_id)
    keep_prefixes = [
        f"{subject_id}/",
        f"{subject_id}.html",
    ]
    for d in fs_dirs:
        keep_prefixes.append(f"sourcedata/freesurfer/{d}/")

    # Collect ALL changed files: staged, modified, and unmerged
    staged = run_capture(
        ["git", "diff", "--cached", "--name-only", "--diff-filter=ACDMR"],
        cwd=site_path,
    ).splitlines()
    unmerged = run_capture(
        ["git", "diff", "--name-only", "--diff-filter=U"],
        cwd=site_path,
    ).splitlines()
    all_changed = set(staged + unmerged)

    discard = []
    for path in all_changed:
        if not path:
            continue
        keep = any(
            path == p.rstrip("/") or path.startswith(p) for p in keep_prefixes
        )
        if not keep:
            discard.append(path)

    if discard:
        print(f"  [INFO] Discarding {len(discard)} shared/unwanted files "
              f"from cherry-pick")
        # Unstage all discarded files first
        run(["git", "reset", "HEAD", "--"] + discard,
            cwd=site_path, check=False)

        # Split discard list: files that exist in HEAD can be restored
        # with checkout; files that are new (not in HEAD) must be cleaned.
        # Running checkout HEAD on files not in HEAD fails silently with
        # check=False but skips restoration of files that DO exist.
        in_head = run_capture(
            ["git", "ls-tree", "--name-only", "-r", "HEAD"],
            cwd=site_path,
        ).splitlines()
        in_head_set = set(in_head)

        restore = [f for f in discard if f in in_head_set]
        remove = [f for f in discard if f not in in_head_set]

        if restore:
            run(["git", "checkout", "HEAD", "--"] + restore,
                cwd=site_path, check=False)
        if remove:
            subprocess.run(
                ["git", "clean", "-fd", "--"] + remove,
                cwd=site_path, capture_output=True,
            )

    # Check we still have something staged
    remaining = run_capture(
        ["git", "diff", "--cached", "--name-only"],
        cwd=site_path,
    )
    if not remaining.strip():
        print(f"  [WARN] No files remaining after filtering for {subject_id}")
        reset_dirty_index(site_path)
        return False

    # Commit with original commit message, author, and date
    run(["git", "commit", "-C", commit_hash, "--no-verify"],
        cwd=site_path)

    return True


def transfer_annex_content(site_path, *, dry_run=False):
    """Transfer annex content from the legacy remote."""
    run(["git", "annex", "get", "--from", "legacy"],
        cwd=site_path, dry_run=dry_run, check=False)


def push_with_backoff(site_path, remote="gin", data="anything",
                      max_attempts=5, *, dry_run=False):
    """Push to remote with exponential backoff on failure."""
    delay = 15
    for attempt in range(1, max_attempts + 1):
        print(f"  [INFO] Push attempt {attempt}/{max_attempts}")
        result = run(
            ["datalad", "push", "-d", site_path,
             "--to", remote, "--data", data],
            dry_run=dry_run, check=False,
        )
        if dry_run or result.returncode == 0:
            return True

        if attempt < max_attempts:
            print(f"  [WARN] Push failed — retrying in {delay}s")
            subprocess.run(
                ["git", "-C", site_path, "fetch", remote, "git-annex"],
                capture_output=True,
            )
            subprocess.run(
                ["git", "-C", site_path, "annex", "merge"],
                capture_output=True,
            )
            time.sleep(delay)
            delay *= 2

    print(f"  [ERROR] All {max_attempts} push attempts failed for {site_path}")
    return False


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Migrate legacy fMRIPrep derivatives to site-level datasets",
    )
    parser.add_argument(
        "--project-root", required=True,
        help="Path to the YODA superdataset root",
    )
    parser.add_argument(
        "--legacy-path", required=True,
        help="Path to the legacy monolithic derivatives dataset",
    )
    parser.add_argument(
        "--porting-list", default=None,
        help="TSV file with columns: participant_id, site_prefix, source_dataset. "
             "Overrides Phase 0/1 (no inventory or submission-list check).",
    )
    parser.add_argument(
        "--site", default=None,
        help="Migrate only a single site (e.g., v1s0)",
    )
    parser.add_argument(
        "--subject", default=None,
        help="Migrate a single subject (e.g., sub-v1s0x0050642)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print actions without executing them",
    )
    parser.add_argument(
        "--resume", action="store_true",
        help="Skip subjects already present in the site dataset",
    )
    parser.add_argument(
        "--skip-push", action="store_true",
        help="Skip pushing to GIN after migration",
    )
    parser.add_argument(
        "--ensure-content", action="store_true",
        help="Run datalad get on legacy data before cherry-picking",
    )
    parser.add_argument(
        "--keep-legacy-remote", action="store_true",
        help="Keep the legacy git remote after migration (default: remove)",
    )
    args = parser.parse_args()

    project_root = os.path.abspath(args.project_root)
    legacy_path = os.path.abspath(args.legacy_path)
    deriv_path = os.path.join(project_root, "derivatives")

    if not os.path.isdir(legacy_path):
        print(f"[FATAL] Legacy path not found: {legacy_path}", file=sys.stderr)
        sys.exit(1)
    if not os.path.isdir(deriv_path):
        print(f"[FATAL] Derivatives path not found: {deriv_path}",
              file=sys.stderr)
        sys.exit(1)

    # =====================================================================
    # Phase 1 — Inventory
    # =====================================================================
    print("=" * 60)
    print("Phase 1: Inventory")
    print("=" * 60)

    if args.porting_list:
        # --porting-list supersedes legacy inventory and Phase 0 checks
        print(f"[INFO] Loading porting list: {args.porting_list}")
        site_subjects = load_porting_list(args.porting_list)
    else:
        site_subjects = inventory_legacy(legacy_path)

    # Filter by --site
    if args.site:
        if args.site not in site_subjects:
            print(f"[FATAL] Site {args.site} not found in "
                  f"{'porting list' if args.porting_list else 'legacy dataset'}",
                  file=sys.stderr)
            sys.exit(1)
        site_subjects = {args.site: site_subjects[args.site]}

    # Filter by --subject
    if args.subject:
        subject_id = args.subject
        if not subject_id.startswith("sub-"):
            subject_id = f"sub-{subject_id}"
        site_prefix = extract_site_prefix(subject_id)
        if site_prefix not in site_subjects:
            print(f"[FATAL] Site {site_prefix} not found in "
                  f"{'porting list' if args.porting_list else 'legacy dataset'}",
                  file=sys.stderr)
            sys.exit(1)
        if subject_id not in site_subjects[site_prefix]:
            print(f"[FATAL] Subject {subject_id} not found in "
                  f"{'porting list' if args.porting_list else 'legacy dataset'} "
                  f"(site {site_prefix})", file=sys.stderr)
            sys.exit(1)
        site_subjects = {site_prefix: [subject_id]}

    # Validate target site datasets exist
    missing_sites = []
    for site in sorted(site_subjects):
        site_path = os.path.join(deriv_path, site)
        if not os.path.isdir(site_path):
            missing_sites.append(site)

    if missing_sites:
        print(f"[FATAL] Missing site datasets: {', '.join(missing_sites)}",
              file=sys.stderr)
        sys.exit(1)

    total_subjects = sum(len(subs) for subs in site_subjects.values())
    print(f"\n[INFO] Legacy dataset: {legacy_path}")
    print(f"[INFO] {total_subjects} subjects across {len(site_subjects)} sites")
    for site in sorted(site_subjects):
        print(f"  {site}: {len(site_subjects[site])} subjects")

    # =====================================================================
    # Phase 0 — Identify subjects submitted (and already processed)
    # =====================================================================
    if args.porting_list:
        # --porting-list already excludes submitted subjects; skip Phase 0
        print("\n" + "=" * 60)
        print("Phase 0: Skipped (--porting-list provided)")
        print("=" * 60)
        submitted = set()
    else:
        # Subjects in the active submission list (lists/curnagl-*.txt) must not
        # be ported.  They are either:
        #   - already re-processed and merged into site datasets, or
        #   - currently running / awaiting merge on job branches.
        # Porting them would create conflicts with the sbatch workflow.
        print("\n" + "=" * 60)
        print("Phase 0: Identify submitted subjects")
        print("=" * 60)

        phase0 = identify_submitted_subjects(project_root, deriv_path,
                                              site_subjects)
        submitted = phase0["submitted"]
        merged = phase0["merged"]
        running = phase0["running"]
        list_file = phase0["list_file"]

        if list_file:
            print(f"\n[INFO] Submission list: {list_file}")
            print(f"[INFO] Total submitted subjects: {len(submitted)}")

            # Count overlap with legacy inventory
            all_legacy = set()
            for subs in site_subjects.values():
                all_legacy.update(subs)
            overlap = submitted & all_legacy

            print(f"[INFO] Overlap with legacy inventory: {len(overlap)}")
            print(f"[INFO]   Already merged into site datasets: {len(merged)}")
            print(f"[INFO]   Running / job branch exists: {len(running)}")
            print(f"[INFO]   Pending (not yet started or no branch): "
                  f"{len(overlap) - len(merged) - len(running)}")
            print(f"[INFO] All {len(overlap)} overlapping subjects will be "
                  f"EXCLUDED from migration")
        else:
            print("\n[INFO] No submission list found — no automatic exclusions")

    # =====================================================================
    # Phase 2 — Map subjects to RUNCMD commits
    # =====================================================================
    print("\n" + "=" * 60)
    print("Phase 2: Map subjects to RUNCMD commits")
    print("=" * 60)

    commit_map = {}  # subject_id → commit_hash
    no_commit = []

    for site in sorted(site_subjects):
        for subject_id in site_subjects[site]:
            commit_hash = find_runcmd_commit(legacy_path, subject_id)
            if commit_hash:
                commit_map[subject_id] = commit_hash
            else:
                no_commit.append(subject_id)
                print(f"  [WARN] No RUNCMD commit found for {subject_id}")

    print(f"\n[INFO] Found RUNCMD commits for "
          f"{len(commit_map)}/{total_subjects} subjects")
    if no_commit:
        print(f"[WARN] {len(no_commit)} subjects without RUNCMD commits "
              f"(will be skipped)")

    # =====================================================================
    # Build final migration list (apply all exclusions)
    # =====================================================================
    skipped_submitted = 0
    skipped_resume = 0
    subjects_to_migrate = {}  # site → [(subject_id, commit_hash)]

    for site in sorted(site_subjects):
        site_path = os.path.join(deriv_path, site)
        subjects_for_site = []

        for subject_id in site_subjects[site]:
            if subject_id not in commit_map:
                continue

            # Phase 0 exclusion: in the active submission list
            if subject_id in submitted:
                skipped_submitted += 1
                continue

            # Resume exclusion: already present in site dataset
            if args.resume and check_subject_on_master(site_path, subject_id):
                print(f"  [SKIP] {subject_id} — already migrated (--resume)")
                skipped_resume += 1
                continue

            subjects_for_site.append((subject_id, commit_map[subject_id]))

        if subjects_for_site:
            subjects_to_migrate[site] = subjects_for_site

    migrate_total = sum(len(subs) for subs in subjects_to_migrate.values())
    print(f"\n[INFO] Subjects to migrate: {migrate_total}")
    print(f"[INFO] Excluded (in submission list): {skipped_submitted}")
    print(f"[INFO] Already migrated (--resume): {skipped_resume}")
    print(f"[INFO] No RUNCMD commit: {len(no_commit)}")

    if args.dry_run:
        print("\n[DRY-RUN] Would migrate:")
        for site in sorted(subjects_to_migrate):
            subjects = subjects_to_migrate[site]
            print(f"  {site}: {len(subjects)} subjects")
            for sid, chash in subjects:
                print(f"    {sid} ← {chash[:12]}")
        print("\n[DRY-RUN] No changes made.")
        return

    if migrate_total == 0:
        print("\n[INFO] Nothing to migrate.")
        return

    # =====================================================================
    # Phase 3 — Per-site migration
    # =====================================================================
    print("\n" + "=" * 60)
    print("Phase 3: Per-site migration")
    print("=" * 60)

    migrated = 0
    failed = 0
    sites_modified = []

    for site in sorted(subjects_to_migrate):
        site_path = os.path.join(deriv_path, site)
        subjects = subjects_to_migrate[site]

        print(f"\n--- Site {site}: {len(subjects)} subjects ---")

        # 3a. Set up legacy remote
        print("[INFO] Setting up legacy remote...")
        setup_legacy_remote(site_path, legacy_path)

        # 3b. Ensure content (optional)
        if args.ensure_content:
            print("[INFO] Ensuring annex content is available in legacy...")
            for subject_id, _ in subjects:
                get_paths = [f"{subject_id}/", f"{subject_id}.html"]
                for fs_dir in inventory_freesurfer(legacy_path, subject_id):
                    get_paths.append(f"sourcedata/freesurfer/{fs_dir}/")
                run(["datalad", "get"] + get_paths,
                    cwd=legacy_path, check=False)

        # 3c. Cherry-pick each subject
        for i, (subject_id, commit_hash) in enumerate(subjects, 1):
            print(f"\n[{i}/{len(subjects)}] Cherry-picking {subject_id} "
                  f"(commit {commit_hash[:12]})")

            ok = cherry_pick_subject(
                site_path, legacy_path, subject_id, commit_hash,
            )
            if ok:
                migrated += 1
            else:
                print(f"  [ERROR] Failed to cherry-pick {subject_id}")
                failed += 1

        # 3d. Transfer annex content
        print(f"\n[INFO] Transferring annex content for site {site}...")
        transfer_annex_content(site_path)

        # 3e. Clean up legacy remote (unless --keep-legacy-remote)
        if not args.keep_legacy_remote:
            print(f"[INFO] Removing legacy remote from {site}")
            cleanup_legacy_remote(site_path)

        sites_modified.append(site)

    # =====================================================================
    # Phase 4 — Push to GIN
    # =====================================================================
    if not args.skip_push and sites_modified:
        print("\n" + "=" * 60)
        print("Phase 4: Push to GIN")
        print("=" * 60)

        for site in sites_modified:
            site_path = os.path.join(deriv_path, site)
            print(f"\n[INFO] Pushing {site} to GIN...")
            push_with_backoff(site_path)

    # =====================================================================
    # Phase 5 — Save superdataset
    # =====================================================================
    if sites_modified:
        print("\n" + "=" * 60)
        print("Phase 5: Save superdataset")
        print("=" * 60)

        site_paths = [
            os.path.join("derivatives", s) for s in sites_modified
        ]
        run(["datalad", "save", "-d", project_root,
             "-m", f"Migrate {migrated} subjects from legacy dataset "
                   f"across {len(sites_modified)} sites"]
            + site_paths,
            cwd=project_root)

    # =====================================================================
    # Summary
    # =====================================================================
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"  Migrated:           {migrated}")
    print(f"  Failed:             {failed}")
    print(f"  Excluded (submitted): {skipped_submitted}")
    print(f"  Skipped (--resume): {skipped_resume}")
    print(f"  No RUNCMD commit:   {len(no_commit)}")
    print(f"  Sites modified:     {', '.join(sites_modified) or 'none'}")

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
