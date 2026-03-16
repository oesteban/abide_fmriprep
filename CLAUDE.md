# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

ABIDE-fMRIPrep preprocesses ABIDE I and ABIDE II brain imaging data (~2,194 subjects from 43 sites across 24 institutions) with fMRIPrep 25.2.4. The project follows a YODA-compliant DataLad layout and targets the UNIL Curnagl HPC cluster for production runs. All executions are recorded with `datalad containers-run` for full provenance.

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
│   ├── build_abide_both.py        # Main build script (stdlib-only Python)
│   ├── fmriprep-jobarray.sbatch   # SLURM array job (site-level, job branch workflow)
│   ├── create_site_datasets.sh    # Initialize per-site DataLad derivative datasets
│   ├── reconcile_subdatasets.sh   # Post-batch: octopus-merge job branches per site (TODO)
│   ├── migrate_to_subdatasets.py  # One-time migration from monolithic derivatives (TODO)
│   ├── merge_job_branches.sh      # DEPRECATED
│   ├── one-test-subject.sh        # Local Docker smoke test [run/first-local branch only]
│   ├── calypso/                   # HPC utilities [run/first-local branch only]
│   │   ├── README.md              # Calypso first-run checklist
│   │   ├── first_subject.sbatch   # Single-subject SLURM job
│   │   └── preflight.sh           # Environment validation
│   └── datalad/
│       └── cfg_fmriprep.py        # DataLad procedure: .gitattributes for derivatives
├── derivatives/                   # Site-level DataLad subdatasets (43 total)
│   ├── v1s0/                      # ABIDE I, site 0 (CMU_a) — fMRIPrep output root
│   │   ├── dataset_description.json
│   │   ├── .bidsignore
│   │   ├── sub-v1s0x0050642/      # Standard fMRIPrep output directory
│   │   ├── sub-v1s0x0050642.html  # QC report
│   │   └── sourcedata/freesurfer/ # FreeSurfer outputs (regular directories)
│   │       ├── fsaverage/         # Shared (not versioned per-job)
│   │       └── sub-v1s0x0050642_ses-1/
│   ├── v1s1/                      # ABIDE I, site 1
│   ├── v2s0/                      # ABIDE II, site 0
│   ├── ...                        # (43 site datasets total)
│   └── fmriprep-25.2/             # LEGACY: monolithic derivatives (read-only archive)
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
- Site prefix = `v1s<N>` or `v2s<N>` (e.g., `v1s0` = CMU_a, `v2s6` = KKI_1)
- Example: `sub-v1s0x0050642` = ABIDE I, site index 0 (CMU_a), original ID 0050642

## Site prefix → site name mapping

43 site prefixes total: 24 from ABIDE I (`v1s0`–`v1s23`) and 19 from ABIDE II (`v2s0`–`v2s18`). Subjects per site range from 6 (v1s7/MaxMun_b) to 211 (v2s6/KKI_1), median ~36.

## Git submodules

The superdataset has the following fixed submodules plus 43 site-level derivative subdatasets:

| Path | Remote | Purpose |
|------|--------|---------|
| `inputs/abide1` | `datasets.datalad.org/abide/RawDataBIDS` | ABIDE I raw BIDS |
| `inputs/abide2` | `datasets.datalad.org/abide2/RawData` | ABIDE II raw BIDS |
| `inputs/abide-both` | `github.com/oesteban/abide-merged` | Merged self-contained BIDS view |
| `inputs/templateflow` | `github.com/templateflow/templateflow` | Brain templates |
| `derivatives/fmriprep-25.2` | `github.com/oesteban/abide-fmriprep-derivatives` | Legacy monolithic derivatives |
| `derivatives/v1s0` | `gin.g-node.org:/abide-fmriprep/v1s0` | Site derivatives (×43) |
| `docs/paper` | `github.com/oesteban/abide-paper` | LaTeX manuscript |
| `logs` | `github.com/oesteban/abide-fmriprep-derivatives-logs` | SLURM logs |

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

**Initialize site-level derivative datasets:**
```bash
code/create_site_datasets.sh --project-root .
code/create_site_datasets.sh --project-root . --create-siblings --gin-org abide-fmriprep
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

**Important:** `OUT_DIR_HOST` points to the site dataset directory (e.g., `derivatives/v1s0`), not a monolithic derivatives directory.

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

## Derivatives architecture (site-level datasets)

### Layout

Each of the 43 site prefixes gets its own DataLad subdataset under `derivatives/`. Each site dataset is a valid fMRIPrep BIDS derivatives root. No per-subject subdatasets — subjects are regular directories inside the site dataset.

- **Site datasets:** `derivatives/v1s0/`, `derivatives/v1s1/`, ..., `derivatives/v2s18/`
- **GIN repos:** `gin.g-node.org:/abide-fmriprep/v1s0`, etc. (one per site)
- **Subjects inside sites:** `derivatives/v1s0/sub-v1s0x0050642/` (regular directory)
- **FreeSurfer outputs:** `derivatives/v1s0/sourcedata/freesurfer/sub-v1s0x0050642_ses-1/`
- **HTML reports:** `derivatives/v1s0/sub-v1s0x0050642.html`
- **`.gitattributes`:** Applied via `cfg_fmriprep` procedure during site dataset creation
- **`.bidsignore`:** Excludes `*.html`, `logs/`, `figures/`, `*_xfm.*`, surfaces, and other non-BIDS files
- **Legacy:** `derivatives/fmriprep-25.2/` is the old monolithic dataset (kept as archive)

### SLURM job workflow (job branches)

Each SLURM job processes one subject and pushes results to the subject's site dataset on a dedicated job branch:

1. **Clone:** Lightweight clone of superdataset to `$SLURM_TMPDIR`
2. **Install:** `datalad get -n derivatives/<site_prefix>` (site subdataset, lightweight)
3. **Check:** Skip if subject is on master or job branch already on GIN
4. **Branch:** `git checkout -b job/sub-v1s0x0050642` in the site subdataset
5. **Run:** `datalad containers-run --explicit` with per-subject `--output` declarations (shared files like `dataset_description.json`, `CITATION.*`, `fsaverage/` are excluded to prevent merge conflicts)
6. **Push:** Job branch + annex content pushed to the site's GIN repo (exponential backoff, rescue-to-`$SCRATCH` on failure)
7. **Reconcile:** After a batch, octopus-merge all job branches per site (guaranteed conflict-free since outputs are disjoint directories)

### Why site-level (not per-subject subdatasets)?

- **43 `.gitmodules` entries** vs ~4,400 with per-subject subdatasets
- **43 GIN repos** vs ~4,400
- **Provenance preserved:** `containers-run` commit lives in the site dataset's git history
- **Simpler reconciliation:** fetch + octopus merge (vs discover + install subdatasets)
- **Largest site:** v2s6 (KKI_1, 211 subjects, ~95K files) — well within git-annex comfort

### GIN organization: `abide-fmriprep`

One repo per site: `abide-fmriprep/v1s0`, `abide-fmriprep/v1s1`, ..., `abide-fmriprep/v2s18`.

### Known gotcha: GIN annex UUID mismatch

When site datasets are created locally and then pushed to GIN, the GIN server assigns its own git-annex UUID — distinct from the UUID stored in the local `git config remote.gin.annex-uuid`. Content pushed from HPC compute nodes goes through GIN, so location tracking records the **server's** UUID. But the local clone retains the **stale** UUID from initial setup. This causes `datalad get` to fail: git-annex knows content exists at the server UUID but has no configured remote mapped to it.

**Diagnosis:** `git annex info` shows two entries for GIN — one labelled `gin` (wrong UUID from git config) and one with the server path `/data/repos/abide-fmriprep/<site>.git` (correct UUID). `git annex whereis` on any file shows the server UUID, not the `gin` remote UUID.

**Fix per site:**
```bash
# Fetch the git-annex branch (carries the server's UUID in location tracking)
git -C derivatives/<site> fetch gin git-annex
git -C derivatives/<site> annex merge
# Extract the actual GIN server UUID
gin_uuid=$(git -C derivatives/<site> annex info | grep '/data/repos/abide-fmriprep/' | head -1 | awk '{print $1}')
# Update the local config
git -C derivatives/<site> config remote.gin.annex-uuid "$gin_uuid"
```

**Prevention:** The overlay build script (`code/build_derivatives_overlay.py`) now reads GIN UUIDs from the overlay's own remote config (populated by `initremote type=git`, which auto-discovers the correct UUID) rather than from the site subdataset's potentially stale config.

## DataLad / git-annex conventions

- All files use `MD5E` annex backend (root `.gitattributes`)
- `code/` has `* annex.largefiles=nothing` — all code is always git-tracked
- Metadata files (JSON, TSV, bval, bvec) should be git-tracked; imaging binaries stay annexed
- Fixed submodules managed by DataLad at the superdataset level (see `.gitmodules`)
- Site-level derivative subdatasets managed under `derivatives/`
- `inputs/abide1` and `inputs/abide2` are never modified; all generated content goes into `inputs/abide-both`

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

- CPUs: 16, Memory: 64G, Time: 10h
- Logs: `logs/%x_%A_%a.{out,err}`
- Clone-per-job pattern: each SLURM task clones superdataset to `$SLURM_TMPDIR`
- Each job pushes results to the site's GIN repo on a job branch
- After a batch: reconcile job branches into master per site

## Code conventions

- Python scripts use only stdlib (no pip dependencies beyond DataLad ecosystem)
- `build_abide_both.py` supports `--dry-run` for safe testing
- Shell scripts use `set -euo pipefail`
- Scripts must be runnable from repo root using relative paths
- Commit style: conventional prefixes (`enh:`, `fix:`, `doc:`, `chore:`) trending; DataLad auto-commits use `[DATALAD]` prefix

## cfg_fmriprep.py DataLad procedure

The `code/datalad/cfg_fmriprep.py` script is a DataLad procedure that configures `.gitattributes` for fMRIPrep derivative datasets. It forces metadata formats (JSON, TSV, FreeSurfer text outputs) into git and imaging formats (`.gii`, `.h5`, `.nii.gz`) into annex. Applied to each site dataset during creation. Install by symlinking into DataLad's procedures directory.
