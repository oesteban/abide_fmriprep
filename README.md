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
datalad get -n inputs/abide1 inputs/abide2 inputs/templateflow
datalad get -n derivatives/fmriprep-25.2
```

## Container setup (DataLad containers)
This dataset uses `datalad containers-run` so that all runs are captured with
provenance. Add two container entries, one for Docker (local) and one for
Apptainer/Singularity (HPC).

```bash
datalad containers-add -n fmriprep-docker \
  --url docker://nipreps/fmriprep:25.2.0 \
  --call-fmt 'docker run --rm -t -v "$BIDS_DIR_HOST":/bids:ro -v "$OUT_DIR_HOST":/out -v "$FMRIPREP_WORKDIR":/work -v "$TEMPLATEFLOW_HOME_HOST":/templateflow -v "$FS_LICENSE_FILE":/fs/license.txt -e TEMPLATEFLOW_HOME=/templateflow -e TEMPLATEFLOW_USE_DATALAD=on {img} {cmd}'
```

```bash
datalad containers-add -n fmriprep-apptainer \
  --url docker://nipreps/fmriprep:25.2.0 \
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
Example for ABIDE I (NYU) and ABIDE II (NYU_1). These subject IDs exist in the
current inputs.

```bash
# ABIDE I example
datalad get -r inputs/abide1/NYU/sub-0050958

export BIDS_DIR_HOST="$PWD/inputs/abide1/NYU"
export OUT_DIR_HOST="$PWD/derivatives/fmriprep-25.2"
export TEMPLATEFLOW_HOME_HOST="$PWD/inputs/templateflow"
export FMRIPREP_WORKDIR="$PWD/.tmp/fmriprep-work"
export FS_LICENSE_FILE="$PWD/env/secrets/fs_license.txt"

mkdir -p "$FMRIPREP_WORKDIR"

datalad containers-run -n fmriprep-docker \
  --explicit \
  -m "fMRIPrep ABIDE1 NYU sub-0050958 (local test)" \
  --input inputs/abide1/NYU/sub-0050958 \
  --output derivatives/fmriprep-25.2 \
  -- \
  /bids /out participant \
    --participant-label 0050958 \
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
datalad get -r inputs/abide2/NYU_1/sub-29196
export BIDS_DIR_HOST="$PWD/inputs/abide2/NYU_1"
export OUT_DIR_HOST="$PWD/derivatives/fmriprep-25.2"
export TEMPLATEFLOW_HOME_HOST="$PWD/inputs/templateflow"
export FMRIPREP_WORKDIR="$PWD/.tmp/fmriprep-work"
export FS_LICENSE_FILE="$PWD/env/secrets/fs_license.txt"
mkdir -p "$FMRIPREP_WORKDIR"
```

Then reuse the same `datalad containers-run` command, swapping the subject
label to `29196` and input path to `inputs/abide2/NYU_1/sub-29196`.

## HPC / SLURM usage (HES-SO)
Module load commands and partitions are TBD. A micromamba environment is the
recommended default on HES-SO until a shared module is provided.

Auto-discover subjects per site (no subjects file required):

```bash
sbatch --array=1-N code/bootstrap_fmriprep_ARRAY.sbatch.sh \
  --project-root /path/to/abide_fmriprep \
  --dataset abide1 \
  --site NYU \
  --container-name fmriprep-apptainer
```

Explicit subjects list:

```bash
sbatch --array=1-N code/bootstrap_fmriprep_ARRAY.sbatch.sh \
  --project-root /path/to/abide_fmriprep \
  --dataset abide2 \
  --site NYU_1 \
  --subjects-file /path/to/subjects.txt \
  --container-name fmriprep-apptainer
```

Scratch behavior uses `$SLURM_TMPDIR` when available, otherwise `/tmp`.

## Output naming scheme
Each subject is renamed in derivatives to unify ABIDE I/II IDs:
- ABIDE I: `sub-v1+SITE+orig`
- ABIDE II: `sub-v2+SITE+orig`

`SITE` is normalized by removing non-alphanumerics and uppercasing
(`site_to_code` in `code/bootstrap_fmriprep_ARRAY.sbatch.sh`).

## Derivatives and provenance
- Canonical output dataset: `derivatives/fmriprep-25.2`
- All executions use `datalad containers-run` for provenance
- The SLURM script pushes to the `gin` remote by default
