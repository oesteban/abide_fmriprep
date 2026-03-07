# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

ABIDE-fMRIPrep preprocesses ABIDE I and ABIDE II brain imaging data (~2,194 subjects from 24 institutions) with fMRIPrep 25.2.4. The project follows a YODA-compliant DataLad layout and targets the HES-SO Calypso HPC cluster for production runs. All executions are recorded with `datalad containers-run` for full provenance.

**Repository:** `oesteban/abide_fmriprep` on GitHub.

## Architecture (YODA layout)

```
abide_preproc/                     # YODA superdataset root
├── inputs/                        # Upstream datasets (read-only by convention)
│   ├── abide1/                    # Submodule → datasets.datalad.org/abide/RawDataBIDS
│   ├── abide2/                    # Submodule → datasets.datalad.org/abide2/RawData
│   ├── abide-both/                # Submodule → oesteban/abide-merged (merged BIDS view)
│   └── templateflow/              # Submodule → templateflow/templateflow
├── code/                          # All scripts (run from repo root with relative paths)
│   ├── build_abide_both.py        # Main build script (1124 lines, stdlib-only Python)
│   ├── fmriprep-jobarray.sbatch  # SLURM array job (310 lines)
│   ├── one-test-subject.sh        # Local Docker smoke test [run/first-local branch only]
│   ├── calypso/                   # HPC utilities [run/first-local branch only]
│   │   ├── README.md              # Calypso first-run checklist
│   │   ├── first_subject.sbatch   # Single-subject SLURM job
│   │   └── preflight.sh           # Environment validation
│   └── datalad/
│       └── cfg_fmriprep.py        # DataLad procedure: .gitattributes for derivatives
├── derivatives/
│   └── fmriprep-25.2/             # Separate subdataset (git-annex, pushed to GIN)
├── docs/
│   └── paper/                     # Submodule → oesteban/abide-paper (LaTeX manuscript)
├── env/
│   ├── environment.yml            # micromamba env spec (datalad env)
│   └── secrets/                   # Gitignored; holds fs_license.txt
├── logs/                          # SLURM output logs
├── .datalad/
│   ├── config                     # Dataset ID + 2 container definitions
│   └── environments/
│       └── fmriprep-docker/image/ # OCI image layout (annex-stored layers)
├── dhub:/nipreps/fmriprep:25.2.4/ # OCI layout artifact from containers-add
└── docker:/nipreps/               # Empty artifact directory
```

**Important:** `code/one-test-subject.sh` and `code/calypso/` exist only on the `run/first-local` branch, not on `master`.

## Subject ID scheme

- ABIDE I: `sub-v1s<siteindex>x<orig>` (all under `ses-1`)
- ABIDE II: `sub-v2s<siteindex>x<orig>` (all under `ses-1`; 46 subjects have `ses-2`)
- `siteindex` = zero-based alphabetical index within each dataset
- Example: `sub-v1s0x0050642` = ABIDE I, site index 0 (CMU_a), original ID 0050642

## Git submodules (6 total)

| Path | Remote | Purpose |
|------|--------|---------|
| `inputs/abide1` | `datasets.datalad.org/abide/RawDataBIDS` | ABIDE I raw BIDS |
| `inputs/abide2` | `datasets.datalad.org/abide2/RawData` | ABIDE II raw BIDS |
| `inputs/abide-both` | `github.com/oesteban/abide-merged` | Merged self-contained BIDS view |
| `inputs/templateflow` | `github.com/templateflow/templateflow` | Brain templates |
| `derivatives/fmriprep-25.2` | `github.com/oesteban/abide-fmriprep-derivatives` | fMRIPrep outputs |
| `docs/paper` | `github.com/oesteban/abide-paper` | LaTeX manuscript |

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

**Local one-subject smoke test (Docker, requires `run/first-local` branch):**
```bash
SUBJECT=v1s0x0050642 bash code/one-test-subject.sh
```

**HPC batch submission (SLURM):**
```bash
sbatch --array=1-N code/fmriprep-jobarray.sbatch \
  --project-root /path/to/abide_fmriprep \
  --container-name fmriprep-apptainer
```

**SLURM with site/dataset filtering:**
```bash
sbatch --array=1-N code/fmriprep-jobarray.sbatch \
  --project-root /path/to/abide_fmriprep \
  --container-name fmriprep-apptainer \
  --dataset abide1 --site NYU
```

**Paper build (from docs/paper/):**
```bash
make pdf          # Minimal build (no bibliography)
make pdf-content  # Full build with bibliography
make clean        # Remove build artifacts
```

## Container configuration

Two containers registered in `.datalad/config` (image: `nipreps/fmriprep:25.2.4`):

| Name | Backend | Use case |
|------|---------|----------|
| `fmriprep-docker` | Docker (`cmdexec`) | Local development |
| `fmriprep-apptainer` | Apptainer (`call-fmt`) | HPC production |

Both require these **absolute-path** environment variables before `containers-run`:

| Variable | Bind target | Access |
|----------|-------------|--------|
| `INPUTS_DIR_HOST` | `/bids` | read-only |
| `OUT_DIR_HOST` | `/out` | read-write |
| `FMRIPREP_WORKDIR` | `/work` | read-write |
| `TEMPLATEFLOW_HOME_HOST` | `/templateflow` | **read-write** (DataLad fetches) |
| `FS_LICENSE_FILE` | `/fs/license.txt` | read-only |

Container env vars set inside: `TEMPLATEFLOW_HOME=/templateflow`, `TEMPLATEFLOW_USE_DATALAD=on`.

## fMRIPrep standard parameters

Output spaces: `MNI152NLin2009cAsym`, `fsLR` (with `--cifti-output 91k`). BIDS validation is skipped (`--skip-bids-validation`).

### Required TemplateFlow assets

When the container runs with `TEMPLATEFLOW_USE_DATALAD=off` (the apptainer configuration), all templates must be pre-fetched before job submission. The following five templates are required by fMRIPrep 25.2.4 with the parameters above:

| Template | Role | Files |
|----------|------|-------|
| `tpl-MNI152NLin2009cAsym` | Requested output space | 133 |
| `tpl-MNI152NLin6Asym` | Internal reference (brain extraction, segmentation) | 124 |
| `tpl-OASIS30ANTs` | Default `--skull-strip-template` | 19 |
| `tpl-fsLR` | CIFTI output space (`--cifti-output 91k`) | 33 |
| `tpl-fsaverage` | Surface reconstruction (FreeSurfer interop) | 205 |

**Pre-fetch command:**
```bash
cd inputs/templateflow
datalad get -r tpl-MNI152NLin2009cAsym tpl-MNI152NLin6Asym tpl-OASIS30ANTs tpl-fsLR tpl-fsaverage
```

**Note:** With `TEMPLATEFLOW_USE_DATALAD=on` (the Docker configuration), fMRIPrep auto-downloads missing templates at runtime and pre-fetching is not strictly required.

## Derivatives dataset

- **Path:** `derivatives/fmriprep-25.2/`
- **Remotes:** GitHub (`origin`) for git history; GIN (`gin`) for git-annex data storage
- **Branch convention:** Job results go on `job/abide-both/{dataset}/{site}/sub-{subject}/{jobid}_{taskid}`
- **`.bidsignore`:** Excludes `*.html`, `logs/`, `figures/`, `*_xfm.*`, surfaces, and other non-BIDS files from validation
- **`.gitattributes`:** 40 rules — metadata (JSON, TSV, FreeSurfer text) in git; imaging (`.gii`, `.h5`, `.nii.gz`) in annex

## DataLad / git-annex conventions

- All files use `MD5E` annex backend (root `.gitattributes`)
- `code/` has `* annex.largefiles=nothing` — all code is always git-tracked
- Metadata files (JSON, TSV, bval, bvec) should be git-tracked; imaging binaries stay annexed
- Six git submodules managed by DataLad (see `.gitmodules`)
- `inputs/abide1` and `inputs/abide2` are never modified; all generated content goes into `inputs/abide-both`
- `derivatives/fmriprep-25.2` is a separate git-annex subdataset with its own remotes

## Environment setup

**micromamba environment** (`env/environment.yml`):
- Python >=3.11
- datalad >=1.1
- datalad-container >=1.2
- `git-annex` must be installed separately (platform-dependent: `brew` on macOS, system package on Linux)

**Install:**
```bash
micromamba create -f env/environment.yml
micromamba activate datalad
```

## SLURM resource defaults

- CPUs: 16, Memory: 64G, Time: 24h (array job) / 12h (first-subject)
- Logs: `logs/%x_%A_%a.{out,err}`
- Clone-per-job pattern: each SLURM task clones superdataset to `$SLURM_TMPDIR`
- Results pushed to GIN remote, then scratch cleaned up

## Code conventions

- Python scripts use only stdlib (no pip dependencies beyond DataLad ecosystem)
- `build_abide_both.py` supports `--dry-run` for safe testing
- Shell scripts use `set -euo pipefail`
- Scripts must be runnable from repo root using relative paths
- Branch safety: HPC scripts refuse to run on `master`
- Commit style: conventional prefixes (`enh:`, `fix:`, `doc:`, `chore:`) trending; DataLad auto-commits use `[DATALAD]` prefix

## cfg_fmriprep.py DataLad procedure

The `code/datalad/cfg_fmriprep.py` script is a DataLad procedure that configures `.gitattributes` for fMRIPrep derivative datasets. It forces metadata formats (JSON, TSV, FreeSurfer text outputs) into git and imaging formats (`.gii`, `.h5`, `.nii.gz`) into annex. Install by symlinking into DataLad's procedures directory.
