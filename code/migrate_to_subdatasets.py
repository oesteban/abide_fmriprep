#!/usr/bin/env python3
"""
Migrate existing fMRIPrep derivatives to per-subject DataLad subdatasets.

For each processed subject on master, this script:
1. Wraps sub-XXX/ as a fMRIPrep subdataset
2. Wraps sourcedata/freesurfer/sub-XXX_ses-Y/ as FreeSurfer subdataset(s)
3. Creates GIN siblings (abide-fmriprep org) for both
4. Pushes annex content to GIN
5. Saves parent datasets (registers subdatasets in .gitmodules)

Usage:
    python3 code/migrate_to_subdatasets.py --project-root .
    python3 code/migrate_to_subdatasets.py --project-root . --subject sub-v1s0x0050642
    python3 code/migrate_to_subdatasets.py --project-root . --dry-run
    python3 code/migrate_to_subdatasets.py --project-root . --resume

Prerequisites:
    - cfg_fmriprep procedure must be installed (symlinked into DataLad procedures)
    - GIN SSH access must be configured
    - Must run from within the derivatives/fmriprep-25.2 directory or specify --project-root
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


def is_subdataset(path):
    """Check if a path is already a DataLad subdataset (.datalad/config exists)."""
    return os.path.isfile(os.path.join(path, ".datalad", "config"))


def list_subject_dirs(deriv_path):
    """List subject directories (sub-*) in the derivatives root."""
    subjects = []
    for entry in sorted(os.listdir(deriv_path)):
        if entry.startswith("sub-") and os.path.isdir(
            os.path.join(deriv_path, entry)
        ):
            subjects.append(entry)
    return subjects


def list_freesurfer_subject_dirs(fs_path):
    """List FreeSurfer subject directories (sub-*_ses-*) under sourcedata/freesurfer/."""
    dirs = []
    if not os.path.isdir(fs_path):
        return dirs
    for entry in sorted(os.listdir(fs_path)):
        if entry.startswith("sub-") and os.path.isdir(
            os.path.join(fs_path, entry)
        ):
            dirs.append(entry)
    return dirs


def get_freesurfer_dirs_for_subject(fs_path, subject_id):
    """Get FreeSurfer directories matching a subject (sub-XXX_ses-Y)."""
    prefix = f"{subject_id}_ses-"
    dirs = []
    if not os.path.isdir(fs_path):
        return dirs
    for entry in sorted(os.listdir(fs_path)):
        if entry.startswith(prefix) and os.path.isdir(
            os.path.join(fs_path, entry)
        ):
            dirs.append(entry)
    return dirs


def migrate_fmriprep_subject(deriv_path, subject_id, *, dry_run=False, gin_delay=2):
    """Wrap a subject directory as a DataLad subdataset and push to GIN.

    Returns True if successful, False if skipped or failed.
    """
    sub_path = os.path.join(deriv_path, subject_id)
    if not os.path.isdir(sub_path):
        print(f"  [WARN] Directory not found: {sub_path} — skipping")
        return False

    if is_subdataset(sub_path):
        print(f"  [SKIP] {subject_id} is already a subdataset")
        return False

    subject_short = subject_id.removeprefix("sub-")
    gin_repo = f"fmriprep-{subject_short}"

    # 1. Create subdataset
    run(
        ["datalad", "create", "--force", "-d", deriv_path, sub_path],
        dry_run=dry_run,
    )

    # 2. Apply cfg_fmriprep procedure
    run(
        ["datalad", "run-procedure", "-d", sub_path, "cfg_fmriprep"],
        dry_run=dry_run,
    )

    # 3. Save all content inside the subdataset
    run(
        [
            "datalad", "save", "-d", sub_path,
            "-m", f"Initialize fMRIPrep subdataset for {subject_id}",
        ],
        dry_run=dry_run,
    )

    # 4. Create GIN sibling
    run(
        [
            "datalad", "create-sibling-gin", "-d", sub_path,
            "--name", "gin", "--access-protocol", "ssh",
            "--existing", "skip",
            "--org", "abide-fmriprep", "--repo-name", gin_repo,
        ],
        dry_run=dry_run,
    )
    if not dry_run:
        time.sleep(gin_delay)

    # 5. Push annex content to GIN
    run(
        ["datalad", "push", "-d", sub_path, "--to", "gin", "--data", "anything"],
        dry_run=dry_run,
    )

    # 6. Save parent (registers subdataset in .gitmodules)
    run(
        [
            "datalad", "save", "-d", deriv_path,
            "-m", f"Register fMRIPrep subdataset for {subject_id}",
        ],
        dry_run=dry_run,
    )

    return True


def migrate_freesurfer_subject(
    deriv_path, fs_path, fs_dir_name, *, dry_run=False, gin_delay=2
):
    """Wrap a FreeSurfer subject directory as a subdataset and push to GIN.

    Parameters
    ----------
    deriv_path : str
        Path to derivatives/fmriprep-25.2
    fs_path : str
        Path to sourcedata/freesurfer within the derivatives
    fs_dir_name : str
        FreeSurfer directory name (e.g., sub-v1s0x0050642_ses-1)

    Returns True if successful, False if skipped or failed.
    """
    fs_sub_path = os.path.join(fs_path, fs_dir_name)
    if not os.path.isdir(fs_sub_path):
        print(f"  [WARN] Directory not found: {fs_sub_path} — skipping")
        return False

    if is_subdataset(fs_sub_path):
        print(f"  [SKIP] {fs_dir_name} is already a FreeSurfer subdataset")
        return False

    # Extract subject short ID from dir name (e.g., v1s0x0050642_ses-1 from sub-v1s0x0050642_ses-1)
    subject_short = fs_dir_name.removeprefix("sub-")
    gin_repo = f"freesurfer-{subject_short}"

    # 1. Create subdataset under sourcedata/freesurfer
    run(
        ["datalad", "create", "--force", "-d", fs_path, fs_sub_path],
        dry_run=dry_run,
    )

    # 2. Apply cfg_fmriprep procedure
    run(
        ["datalad", "run-procedure", "-d", fs_sub_path, "cfg_fmriprep"],
        dry_run=dry_run,
    )

    # 3. Save content
    run(
        [
            "datalad", "save", "-d", fs_sub_path,
            "-m", f"Initialize FreeSurfer subdataset for {fs_dir_name}",
        ],
        dry_run=dry_run,
    )

    # 4. Create GIN sibling
    run(
        [
            "datalad", "create-sibling-gin", "-d", fs_sub_path,
            "--name", "gin", "--access-protocol", "ssh",
            "--existing", "skip",
            "--org", "abide-fmriprep", "--repo-name", gin_repo,
        ],
        dry_run=dry_run,
    )
    if not dry_run:
        time.sleep(gin_delay)

    # 5. Push annex content to GIN
    run(
        [
            "datalad", "push", "-d", fs_sub_path,
            "--to", "gin", "--data", "anything",
        ],
        dry_run=dry_run,
    )

    # 6. Save intermediate dataset (registers subdataset)
    run(
        [
            "datalad", "save", "-d", fs_path,
            "-m", f"Register FreeSurfer subdataset for {fs_dir_name}",
        ],
        dry_run=dry_run,
    )

    return True


def main():
    parser = argparse.ArgumentParser(
        description="Migrate fMRIPrep derivatives to per-subject subdatasets",
    )
    parser.add_argument(
        "--project-root",
        required=True,
        help="Path to the YODA superdataset root",
    )
    parser.add_argument(
        "--subject",
        default=None,
        help="Migrate a single subject (e.g., sub-v1s0x0050642)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip already-migrated subjects (those with .datalad/config)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print commands without executing them",
    )
    parser.add_argument(
        "--gin-delay",
        type=float,
        default=2.0,
        help="Delay (seconds) between GIN API calls to avoid rate limiting (default: 2)",
    )
    parser.add_argument(
        "--skip-freesurfer",
        action="store_true",
        help="Skip FreeSurfer subdataset migration (do fMRIPrep only)",
    )
    parser.add_argument(
        "--skip-fmriprep",
        action="store_true",
        help="Skip fMRIPrep subdataset migration (do FreeSurfer only)",
    )
    args = parser.parse_args()

    project_root = os.path.abspath(args.project_root)
    deriv_path = os.path.join(project_root, "derivatives", "fmriprep-25.2")
    fs_path = os.path.join(deriv_path, "sourcedata", "freesurfer")

    if not os.path.isdir(deriv_path):
        print(f"[FATAL] Derivatives path not found: {deriv_path}", file=sys.stderr)
        sys.exit(1)

    # Verify sourcedata/freesurfer is itself a DataLad dataset
    if not args.skip_freesurfer and not is_subdataset(fs_path):
        print(
            f"[FATAL] sourcedata/freesurfer is not a DataLad dataset.\n"
            f"  Run Step 1 first:\n"
            f"    cd {deriv_path}\n"
            f"    datalad create --force -d . sourcedata/freesurfer\n"
            f"    datalad run-procedure -d sourcedata/freesurfer cfg_fmriprep\n"
            f"    datalad save -d sourcedata/freesurfer "
            f'-m "Initialize freesurfer intermediate dataset"\n'
            f'    datalad save -d . -m "Register sourcedata/freesurfer as subdataset"',
            file=sys.stderr,
        )
        sys.exit(1)

    # Determine subjects to migrate
    if args.subject:
        subject_id = args.subject
        if not subject_id.startswith("sub-"):
            subject_id = f"sub-{subject_id}"
        subjects = [subject_id]
    else:
        subjects = list_subject_dirs(deriv_path)

    if not subjects:
        print("[INFO] No subject directories found to migrate.")
        sys.exit(0)

    print(f"[INFO] Derivatives path: {deriv_path}")
    print(f"[INFO] FreeSurfer path: {fs_path}")
    print(f"[INFO] Subjects to process: {len(subjects)}")
    if args.dry_run:
        print("[INFO] DRY RUN — no changes will be made")
    print()

    migrated_fmriprep = 0
    migrated_freesurfer = 0
    skipped = 0
    failed = 0

    for i, subject_id in enumerate(subjects, 1):
        print(f"[{i}/{len(subjects)}] Processing {subject_id}...")

        # --- fMRIPrep subdataset ---
        if not args.skip_fmriprep:
            sub_path = os.path.join(deriv_path, subject_id)

            if args.resume and is_subdataset(sub_path):
                print(f"  [SKIP] {subject_id} already migrated (--resume)")
                skipped += 1
            else:
                try:
                    ok = migrate_fmriprep_subject(
                        deriv_path, subject_id,
                        dry_run=args.dry_run,
                        gin_delay=args.gin_delay,
                    )
                    if ok:
                        migrated_fmriprep += 1
                    else:
                        skipped += 1
                except subprocess.CalledProcessError as exc:
                    print(f"  [ERROR] fMRIPrep migration failed for {subject_id}: {exc}")
                    failed += 1

        # --- FreeSurfer subdataset(s) ---
        if not args.skip_freesurfer:
            fs_dirs = get_freesurfer_dirs_for_subject(fs_path, subject_id)
            if not fs_dirs:
                print(f"  [WARN] No FreeSurfer directories found for {subject_id}")
            for fs_dir in fs_dirs:
                fs_sub_path = os.path.join(fs_path, fs_dir)

                if args.resume and is_subdataset(fs_sub_path):
                    print(f"  [SKIP] {fs_dir} already migrated (--resume)")
                    skipped += 1
                    continue

                try:
                    ok = migrate_freesurfer_subject(
                        deriv_path, fs_path, fs_dir,
                        dry_run=args.dry_run,
                        gin_delay=args.gin_delay,
                    )
                    if ok:
                        migrated_freesurfer += 1
                    else:
                        skipped += 1
                except subprocess.CalledProcessError as exc:
                    print(f"  [ERROR] FreeSurfer migration failed for {fs_dir}: {exc}")
                    failed += 1

        print()

    # Final save of parent datasets
    if not args.dry_run and (migrated_fmriprep > 0 or migrated_freesurfer > 0):
        print("[INFO] Final save of parent datasets...")
        if migrated_freesurfer > 0:
            run(
                [
                    "datalad", "save", "-d", fs_path,
                    "-m", "Register all new FreeSurfer subdatasets",
                ],
            )
        run(
            [
                "datalad", "save", "-d", deriv_path,
                "-m", "Register all new subdatasets after migration",
            ],
        )

    print("=" * 60)
    print(f"[SUMMARY] fMRIPrep subdatasets migrated: {migrated_fmriprep}")
    print(f"[SUMMARY] FreeSurfer subdatasets migrated: {migrated_freesurfer}")
    print(f"[SUMMARY] Skipped: {skipped}")
    print(f"[SUMMARY] Failed: {failed}")


if __name__ == "__main__":
    main()
