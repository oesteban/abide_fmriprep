# ABIDE-fMRIPrep

Preprocess ABIDE I and ABIDE II with fMRIPrep on HES-SO, using a YODA
dataset layout and DataLad for provenance and reproducibility.

## Layout (YODA)
- `inputs/` for upstream datasets and building blocks
- `code/` for project-specific scripts
- `derivatives/` for outputs
- `logs/` for SLURM log files
- `env/` for local-only secrets (ignored by Git)

## Inputs and data sources
- `inputs/abide1` (ABIDE I RawDataBIDS)
- `inputs/abide2` (ABIDE II RawData)
- `inputs/abide-both` (merged BIDS view via relative symlinks)
- `inputs/templateflow` (TemplateFlow subdatasets)

## Prerequisites (portable)
- Git and git-annex
- DataLad
- Python 3.9+
- Container runtime: Docker and Apptainer/Singularity
- Optional: micromamba environment for local installs

## Clone and bootstrap
1. Clone with DataLad.

```bash
datalad clone <REPO_URL> abide_fmriprep
cd abide_fmriprep
```

2. Install subdatasets without downloading data.

```bash
datalad get -n inputs/abide1 inputs/abide2 inputs/abide-both inputs/templateflow
datalad get -n derivatives/fmriprep-25.2
```

## Build the merged input (`inputs/abide-both`)
`inputs/abide-both` provides a unified BIDS view across ABIDE I and II using
relative symlinks. Build or refresh it with:

```bash
micromamba run -n datalad python code/build_abide_both.py --project-root .
```

This creates:
- `inputs/abide-both/participants.tsv`
- `inputs/abide-both/dataset_description.json`
- `inputs/abide-both/sub-...` (symlinked subject trees)

## Container setup (DataLad containers)
This dataset uses `datalad containers-run` so that all runs are captured with
provenance. Add two container entries, one for Docker (local) and one for
Apptainer/Singularity (HPC).

```bash
datalad containers-add -n fmriprep-docker \
  --url dhub://nipreps/fmriprep:25.2.4 \
  --call-fmt 'docker run --rm -t -v "$BIDS_DIR_HOST":/bids:ro -v "$OUT_DIR_HOST":/out -v "$FMRIPREP_WORKDIR":/work -v "$TEMPLATEFLOW_HOME_HOST":/templateflow -v "$FS_LICENSE_FILE":/fs/license.txt -e TEMPLATEFLOW_HOME=/templateflow -e TEMPLATEFLOW_USE_DATALAD=on {img} {cmd}'
```

```bash
datalad containers-add -n fmriprep-apptainer \
  --url docker://nipreps/fmriprep:25.2.4 \
  --call-fmt 'apptainer run --cleanenv -B "$BIDS_DIR_HOST":/bids:ro -B "$OUT_DIR_HOST":/out -B "$FMRIPREP_WORKDIR":/work -B "$TEMPLATEFLOW_HOME_HOST":/templateflow -B "$FS_LICENSE_FILE":/fs/license.txt --env TEMPLATEFLOW_HOME=/templateflow --env TEMPLATEFLOW_USE_DATALAD=on {img} {cmd}'
```

The SLURM script defaults to `fmriprep-docker`. Override with
`--container-name fmriprep-apptainer` on HPC.

## Environment variables and secrets
Required variables:
- `TEMPLATEFLOW_HOME=<repo_root>/inputs/templateflow`
- `TEMPLATEFLOW_USE_DATALAD=on`
- `FS_LICENSE=<repo_root>/env/secrets/fs_license.txt`

Create the secrets directory and license file (not tracked by Git):

```bash
mkdir -p env/secrets
cp /path/to/license.txt env/secrets/fs_license.txt
```

## Local one-subject test (macOS, Docker)
Examples for ABIDE I (CMU_a) and ABIDE II (BNI_1). These subject IDs exist in
the current inputs. Note that `inputs/abide-both` is a symlink view, so the
source subject must be fetched in the original dataset.

```bash
# ABIDE I example
datalad get -r inputs/abide1/CMU_a/sub-0050642

export BIDS_DIR_HOST="$PWD/inputs/abide-both"
export OUT_DIR_HOST="$PWD/derivatives/fmriprep-25.2"
export TEMPLATEFLOW_HOME_HOST="$PWD/inputs/templateflow"
export FMRIPREP_WORKDIR="$PWD/.tmp/fmriprep-work"
export FS_LICENSE_FILE="$PWD/env/secrets/fs_license.txt"

mkdir -p "$FMRIPREP_WORKDIR"

datalad containers-run -n fmriprep-docker \
  --explicit \
  -m "fMRIPrep abide-both ABIDE1 CMU_a sub-v1+s1+0050642 (local test)" \
  --input inputs/abide-both/sub-v1+s1+0050642 \
  --input inputs/abide1/CMU_a/sub-0050642 \
  --output derivatives/fmriprep-25.2 \
  -- \
  /bids /out participant \
    --participant-label v1+s1+0050642 \
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
datalad get -r inputs/abide2/BNI_1/sub-29006
export BIDS_DIR_HOST="$PWD/inputs/abide-both"
export OUT_DIR_HOST="$PWD/derivatives/fmriprep-25.2"
export TEMPLATEFLOW_HOME_HOST="$PWD/inputs/templateflow"
export FMRIPREP_WORKDIR="$PWD/.tmp/fmriprep-work"
export FS_LICENSE_FILE="$PWD/env/secrets/fs_license.txt"
mkdir -p "$FMRIPREP_WORKDIR"
```

Then reuse the same `datalad containers-run` command, swapping the subject
label to `v2+s0+29006` and the input paths to
`inputs/abide-both/sub-v2+s0+29006` and `inputs/abide2/BNI_1/sub-29006`.

## HPC / SLURM usage (HES-SO)
Module load commands and partitions are TBD. A micromamba environment is the
recommended default on HES-SO until a shared module is provided.

If Apptainer is available on HES-SO, run the container registration there to
avoid local Singularity build issues:

```bash
micromamba run -n datalad datalad containers-add fmriprep-docker --update \
  --url dhub://nipreps/fmriprep:25.2.4 \
  --call-fmt 'docker run --rm -t -v "$BIDS_DIR_HOST":/bids:ro -v "$OUT_DIR_HOST":/out -v "$FMRIPREP_WORKDIR":/work -v "$TEMPLATEFLOW_HOME_HOST":/templateflow -v "$FS_LICENSE_FILE":/fs/license.txt -e TEMPLATEFLOW_HOME=/templateflow -e TEMPLATEFLOW_USE_DATALAD=on {img} {cmd}'

micromamba run -n datalad datalad containers-add fmriprep-apptainer --update \
  --url docker://nipreps/fmriprep:25.2.4 \
  --call-fmt 'apptainer run --cleanenv -B "$BIDS_DIR_HOST":/bids:ro -B "$OUT_DIR_HOST":/out -B "$FMRIPREP_WORKDIR":/work -B "$TEMPLATEFLOW_HOME_HOST":/templateflow -B "$FS_LICENSE_FILE":/fs/license.txt --env TEMPLATEFLOW_HOME=/templateflow --env TEMPLATEFLOW_USE_DATALAD=on {img} {cmd}'
```

Auto-discover subjects from `inputs/abide-both` (filters are optional):

```bash
sbatch --array=1-N code/bootstrap_fmriprep_ARRAY.sbatch.sh \
  --project-root /path/to/abide_fmriprep \
  --dataset abide1 \
  --site CMU_a \
  --container-name fmriprep-apptainer
```

Unfiltered run across all subjects (no dataset/site filters):

```bash
sbatch --array=1-N code/bootstrap_fmriprep_ARRAY.sbatch.sh \
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
The SLURM script resolves symlinks in `inputs/abide-both` and fetches data
from the original `inputs/abide1`/`inputs/abide2` datasets.

## Output naming scheme
Subjects are normalized in `inputs/abide-both` as:
- ABIDE I: `sub-v1+s<siteindex>+<orig>`
- ABIDE II: `sub-v2+s<siteindex>+<orig>`

The `siteindex` is the zero-based alphabetical index of the site within each
dataset (see `inputs/abide-both/participants.tsv`). Outputs keep these IDs
directly (no post-run rename).

## Derivatives and provenance
- Canonical output dataset: `derivatives/fmriprep-25.2`
- All executions use `datalad containers-run` for provenance
- The SLURM script pushes to the `gin` remote by default
