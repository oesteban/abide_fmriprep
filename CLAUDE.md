# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

ABIDE-fMRIPrep preprocesses ABIDE I and ABIDE II brain imaging data with fMRIPrep 25.2.4, using a YODA-compliant DataLad project on HES-SO HPC (Calypso cluster). All runs are recorded with `datalad containers-run` for full provenance.

## Architecture (YODA layout)

- `inputs/` — upstream datasets (read-only by convention)
  - `abide1/`, `abide2/` — original ABIDE site subdatasets (DataLad)
  - `abide-both/` — **self-contained** merged BIDS view (~2194 subjects); reuses annex keys with registered web URLs
  - `templateflow/` — brain template subdataset
- `code/` — all scripts (must run from repo root with relative paths)
  - `build_abide_both.py` — main build script: merges ABIDE I+II, generates sidecars, materializes metadata
  - `bootstrap_fmriprep_ARRAY.sbatch.sh` — SLURM array job for batch HPC processing
  - `one-test-subject.sh` — local Docker smoke test
  - `calypso/` — HPC-specific utilities (preflight, first-subject SLURM job, checklist)
  - `datalad/cfg_fmriprep.py` — DataLad procedure: git-annex rules (metadata in git, binaries annexed)
- `derivatives/fmriprep-25.2/` — fMRIPrep output subdataset (separate git-annex repo)
- `logs/` — SLURM logs
- `env/secrets/` — gitignored; holds `fs_license.txt` (FreeSurfer license)

## Subject ID scheme

- ABIDE I: `sub-v1s<siteindex>x<orig>` (sessions: `ses-1`)
- ABIDE II: `sub-v2s<siteindex>x<orig>`
- `siteindex` = zero-based alphabetical index within each dataset

## Key commands

All commands run from the repo root. DataLad is typically in a micromamba env called `datalad`.

**Build merged input (fast, no sidecars):**
```bash
micromamba run -n datalad python3 code/build_abide_both.py --project-root . --sidecars none
```

**Generate BOLD JSON sidecars (site templates only):**
```bash
micromamba run -n datalad python3 code/build_abide_both.py --project-root . --sidecars template --overwrite-sidecars
```

**Generate sidecars with TR from NIfTI headers (single subject):**
```bash
micromamba run -n datalad python3 code/build_abide_both.py --project-root . --sidecars tr --sidecar-participant-id sub-v1s0x0050642
```

**Materialize metadata into git:**
```bash
micromamba run -n datalad python3 code/build_abide_both.py --project-root . --skip-build --materialize-metadata --metadata-jobs 8
```

**Local one-subject smoke test (Docker):**
```bash
SUBJECT=v1s0x0050642 bash code/one-test-subject.sh
```

**HPC batch submission (SLURM):**
```bash
sbatch --array=1-N code/bootstrap_fmriprep_ARRAY.sbatch.sh \
  --project-root /path/to/abide_fmriprep \
  --container-name fmriprep-apptainer
```

## Container configuration

Two containers registered in `.datalad/config`:
- `fmriprep-docker` — local development (Docker Desktop)
- `fmriprep-apptainer` — HPC production (Apptainer/Singularity)

Both require these **absolute-path** env vars before `containers-run`:
`INPUTS_DIR_HOST`, `OUT_DIR_HOST`, `TEMPLATEFLOW_HOME_HOST`, `FMRIPREP_WORKDIR`, `FS_LICENSE_FILE`

## DataLad / git-annex conventions

- All files use `MD5E` annex backend (`.gitattributes`)
- Metadata files (JSON, TSV, bval, bvec) should be git-tracked; imaging binaries (`.nii.gz`) stay annexed
- Five git submodules managed by DataLad (see `.gitmodules`)
- `inputs/abide1` and `inputs/abide2` are never modified; all generated content goes into `inputs/abide-both`
- `derivatives/fmriprep-25.2` is a separate git-annex subdataset

## Code conventions

- Python scripts use only stdlib (no pip dependencies beyond DataLad ecosystem)
- `build_abide_both.py` supports `--dry-run` for safe testing
- Shell scripts use `set -euo pipefail`
- Scripts must be runnable from repo root using relative paths
- Branch safety: HPC scripts refuse to run on `master`
