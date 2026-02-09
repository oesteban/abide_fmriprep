# ABIDE-fMRIPrep

Preprocess ABIDE I and ABIDE II with fMRIPrep on HES-SO, using a
[YODA-compliant](https://handbook.datalad.org/en/latest/basics/101-127-yoda.html)
data analysis project implementation, using DataLad for provenance and reproducibility.

YODA (“Yoda's Organigram on Data Analysis”) is introduced
[here](https://handbook.datalad.org/en/latest/basics/101-127-yoda.html)

## Layout (YODA)
- `inputs/` for upstream datasets and building blocks
- `code/` for project-specific scripts
- `derivatives/` for outputs
- `logs/` for SLURM log files
- `env/` for local-only secrets (ignored by Git)

## Inputs and data sources
- `inputs/abide1` (ABIDE I RawDataBIDS)
- `inputs/abide2` (ABIDE II RawData)
- `inputs/abide-both` (merged BIDS view, self-contained git-annex dataset; normalized layout + registered web URLs)
- `inputs/templateflow` (TemplateFlow subdatasets)

## Prerequisites (portable)
- Git and git-annex
- DataLad
- Python 3.9+
- Container runtime: Docker and Apptainer/Singularity
- Optional: micromamba environment for local installs

All DataLad commands below assume `datalad` is on your `PATH`. If it isn't,
either `micromamba activate datalad` first, or prefix commands with
`micromamba run -n datalad`.

## Clone and bootstrap
1. Clone with DataLad.

```bash
micromamba run -n datalad datalad clone <REPO_URL> abide_fmriprep
cd abide_fmriprep
```

2. Install subdatasets without downloading data.

```bash
micromamba run -n datalad datalad get -n inputs/abide-both inputs/templateflow derivatives/fmriprep-25.2

# Install the ABIDE site subdatasets (metadata only; required to build inputs/abide-both)
micromamba run -n datalad datalad get -n -r inputs/abide1
micromamba run -n datalad datalad get -n -r inputs/abide2
```

## Build the merged input (`inputs/abide-both`)
`inputs/abide-both` provides a unified BIDS view across ABIDE I and II as a
**self-contained** git-annex dataset (no cross-dataset symlinks). The build
reuses the original annex keys and registers the original HTTP URL(s) in the
merged dataset's `web` remote, so later runs can do:

```bash
micromamba run -n datalad datalad get -r inputs/abide-both/sub-<ID>
```

```bash
# Fast build (register files/URLs only; no sidecar creation)
micromamba run -n datalad python3 code/build_abide_both.py --project-root . --sidecars none
```

This creates:
- `inputs/abide-both/participants.tsv`
- `inputs/abide-both/dataset_description.json`
- `inputs/abide-both/sub-...` (git-annex-managed subject trees; content retrievable via `web`)

If you need to rebuild from scratch (e.g., after changing the ID scheme), use
`--clean` **with caution** (it deletes `inputs/abide-both/sub-*`):

```bash
micromamba run -n datalad python3 code/build_abide_both.py --project-root . --clean
```

## Materialize BIDS metadata into Git (recommended)
The merged dataset is built by reusing annex keys and registering original HTTP
URLs. This means **all files** initially exist as git-annex pointers, including
small metadata files (JSON/TSV/bval/bvec/...).

That works, but it can make BIDS indexing/validation slow (lots of tiny
downloads). To keep `inputs/abide-both` fast and robust for pipelines, you can
materialize BIDS metadata **into Git**:

- downloads metadata content with `git annex get`
- runs `git annex unannex` so the content becomes regular Git-tracked files
- keeps imaging binaries (`*.nii.gz`, etc.) annexed

```bash
micromamba run -n datalad python3 code/build_abide_both.py --project-root . \
  --skip-build \
  --materialize-metadata \
  --metadata-jobs 8
```

Optional knobs:
- `--metadata-max-mb 50` (skip unexpectedly huge “metadata” files)
- `--metadata-report <path>` (write a JSON report somewhere else)

The default report is written to:
`inputs/abide-both/.datalad/metadata_materialization_report.json`.

After a successful run, commit **inside the subdataset** (local only):

```bash
git -C inputs/abide-both add -A
git -C inputs/abide-both commit -m "abide-both: materialize metadata into git"
```

Important: if you plan to publish the *superdataset* to GitHub, make sure the
`inputs/abide-both` subdataset commit is also published somewhere reachable
from the URL recorded in `.gitmodules` (e.g., a dedicated GitHub repo for the
subdataset). Otherwise, fresh clones will not be able to install the subdataset.

## BOLD JSON sidecars (RepetitionTime)
ABIDE I (and some ABIDE II sites) do not ship functional JSON sidecars with
`RepetitionTime`. The TR is stored in the NIfTI header, so `code/build_abide_both.py`
can generate JSON sidecars **in `inputs/abide-both`** (we never modify
`inputs/abide1` or `inputs/abide2`).

In practice, both ABIDE I and ABIDE II provide *site-level* templates
(`task-*_bold.json`, `T1w.json`). Since `inputs/abide-both` merges all sites into
one dataset (no site folder level), we duplicate that metadata next to each
NIfTI as a per-file sidecar (BIDS inheritance no longer applies cleanly by
site).

This step copies site-level templates (e.g., `task-rest_bold.json`, `T1w.json`)
from each site dataset, and can (optionally) read the TR from the NIfTI header
when `RepetitionTime` is missing.

To (re)generate sidecars for the entire dataset (fast; no NIfTI downloads),
run:

```bash
micromamba run -n datalad python3 code/build_abide_both.py --project-root . \
  --sidecars template \
  --overwrite-sidecars
```

When TR extraction is enabled, the script temporarily downloads each needed
BOLD NIfTI **in the source site dataset** using `git-annex get`, reads the
header, writes a `*_bold.json` sidecar in `inputs/abide-both`, and then drops
the fetched NIfTI to free disk space (default: `git annex drop --force`; use
`--safe-drop` to avoid `--force`).

To limit sidecar generation to a particular merged subject (recommended for
local tests), use `--sidecar-participant-id`:

```bash
micromamba run -n datalad python3 code/build_abide_both.py --project-root . \
  --sidecars tr \
  --sidecar-participant-id sub-v1s0x0050642
```

## Container setup (DataLad containers)
This dataset uses `datalad containers-run` so that all runs are captured with
provenance. Two containers are registered in `.datalad/config`:
- `fmriprep-docker` (local, Docker Desktop)
- `fmriprep-apptainer` (HPC, Apptainer/Singularity)

```bash
micromamba run -n datalad datalad containers-list
```

Container registration is stored in `.datalad/config` and therefore comes with
the repository. You should only need `datalad containers-add` if you want to
re-register or update the image tag.

The SLURM script defaults to `fmriprep-docker`. Override with
`--container-name fmriprep-apptainer` on HPC.

## Environment variables and secrets
Host-side variables used by the container definitions:
- `INPUTS_DIR_HOST`: **absolute** path to `<repo_root>/inputs` (mounts to `/bids`)
- `OUT_DIR_HOST`: absolute path to `<repo_root>/derivatives/fmriprep-25.2` (mounts to `/out`)
- `TEMPLATEFLOW_HOME_HOST`: absolute path to `<repo_root>/inputs/templateflow` (mounts to `/templateflow`)
- `FMRIPREP_WORKDIR`: absolute path to a writable work dir (mounts to `/work`)
- `FS_LICENSE_FILE`: absolute path to `env/secrets/fs_license.txt` (mounts to `/fs/license.txt`)

With the current container definitions, the recommended setting is still:
`INPUTS_DIR_HOST="$PWD/inputs"` and BIDS root `/bids/abide-both`. Since
`inputs/abide-both` is now self-contained, you *can* mount only
`inputs/abide-both` if you also adjust the BIDS root argument passed to
fMRIPrep (e.g., `/bids` instead of `/bids/abide-both`).

Inside the container, the definitions set:
- `TEMPLATEFLOW_HOME=/templateflow`
- `TEMPLATEFLOW_USE_DATALAD=on`

Important: `/templateflow` must be mounted **writable**. Even when all template
files are already present, TemplateFlow's DataLad-based access can touch the
dataset's `.git/config` (e.g., git-annex remote settings), which will fail on a
read-only mount.

Create the secrets directory and license file (not tracked by Git):

```bash
mkdir -p env/secrets
cp /path/to/license.txt env/secrets/fs_license.txt
```

To silence DataLad's Git identity warning (recommended):

```bash
git config --global user.name "Your Name"
git config --global user.email "you@example.com"
```

## Local one-subject test (macOS, Docker)
Examples for ABIDE I (CMU_a) and ABIDE II (BNI_1). These subject IDs exist in
the current inputs.

Important: `INPUTS_DIR_HOST` must be an **absolute** path (recommended:
`"$PWD/inputs"`). Do not use `inputs/` (Docker will treat it as a named volume
and you'll end up with an empty `/bids` inside the container).

```bash
# ABIDE I example
# Fetch the merged subject directly (self-contained dataset)
micromamba run -n datalad datalad get -r inputs/abide-both/sub-v1s0x0050642

# Generate/update JSON sidecars (copies site templates; adds RepetitionTime from header)
micromamba run -n datalad python3 code/build_abide_both.py --project-root . \
  --sidecars tr \
  --sidecar-participant-id sub-v1s0x0050642

export INPUTS_DIR_HOST="$PWD/inputs"
export OUT_DIR_HOST="$PWD/derivatives/fmriprep-25.2"
export TEMPLATEFLOW_HOME_HOST="$PWD/inputs/templateflow"
export FMRIPREP_WORKDIR="$PWD/.tmp/fmriprep-work"
export FS_LICENSE_FILE="$PWD/env/secrets/fs_license.txt"

mkdir -p "$FMRIPREP_WORKDIR"

micromamba run -n datalad datalad containers-run -n fmriprep-docker \
  --explicit \
  -m "fMRIPrep abide-both ABIDE1 CMU_a sub-v1s0x0050642 (local test)" \
  --input inputs/abide-both/sub-v1s0x0050642 \
  --output derivatives/fmriprep-25.2 \
  -- \
  /bids/abide-both /out participant \
    --participant-label v1s0x0050642 \
    --skip-bids-validation \
    --output-layout bids \
    --fs-license-file /fs/license.txt \
    --cifti-output 91k \
    --output-spaces MNI152NLin2009cAsym fsLR \
    --nthreads 8 \
    --omp-nthreads 8 \
    --mem-mb 32000 \
    -w /work
```

```bash
# ABIDE II example
micromamba run -n datalad datalad get -r inputs/abide-both/sub-v2s0x29006

micromamba run -n datalad python3 code/build_abide_both.py --project-root . \
  --sidecars tr \
  --sidecar-participant-id sub-v2s0x29006

export INPUTS_DIR_HOST="$PWD/inputs"
export OUT_DIR_HOST="$PWD/derivatives/fmriprep-25.2"
export TEMPLATEFLOW_HOME_HOST="$PWD/inputs/templateflow"
export FMRIPREP_WORKDIR="$PWD/.tmp/fmriprep-work"
export FS_LICENSE_FILE="$PWD/env/secrets/fs_license.txt"
mkdir -p "$FMRIPREP_WORKDIR"
```

Then reuse the same `datalad containers-run` command, swapping the subject
label to `v2s0x29006` and the input paths to
`inputs/abide-both/sub-v2s0x29006`.

## HPC / SLURM usage (HES-SO)
Module load commands and partitions are TBD. A micromamba environment is the
recommended default on HES-SO until a shared module is provided.

If Apptainer is available on HES-SO, run the container registration there to
avoid local Singularity build issues:

```bash
micromamba run -n datalad datalad containers-list
```

Auto-discover subjects from `inputs/abide-both` (filters are optional).
For the unfiltered run, the number of subjects is:

```bash
tail -n +2 inputs/abide-both/participants.tsv | wc -l
```

Filtered example:

```bash
sbatch --array=1-N code/bootstrap_fmriprep_ARRAY.sbatch.sh \
  --project-root /path/to/abide_fmriprep \
  --dataset abide1 \
  --site CMU_a \
  --container-name fmriprep-apptainer
```

Unfiltered run across all subjects (no dataset/site filters):

```bash
sbatch --array=1-2194 code/bootstrap_fmriprep_ARRAY.sbatch.sh \
  --project-root /path/to/abide_fmriprep \
  --container-name fmriprep-apptainer
```

Explicit subjects list:

```bash
sbatch --array=1-N code/bootstrap_fmriprep_ARRAY.sbatch.sh \
  --project-root /path/to/abide_fmriprep \
  --dataset abide2 \
  --site BNI_1 \
  --subjects-file /path/to/subjects.txt \
  --container-name fmriprep-apptainer
```

Scratch behavior uses `$SLURM_TMPDIR` when available, otherwise `/tmp`.
The SLURM script fetches each subject directly from `inputs/abide-both`
(self-contained dataset with registered web URLs).

## Output naming scheme
Subjects are normalized in `inputs/abide-both` as:
- ABIDE I: `sub-v1s<siteindex>x<orig>`
- ABIDE II: `sub-v2s<siteindex>x<orig>`

The `siteindex` is the zero-based alphabetical index of the site within each
dataset (see `inputs/abide-both/participants.tsv`). Outputs keep these IDs
directly (no post-run rename).

## Derivatives and provenance
- Canonical output dataset: `derivatives/fmriprep-25.2`
- All executions use `datalad containers-run` for provenance
- The SLURM script pushes to the `gin` remote by default
