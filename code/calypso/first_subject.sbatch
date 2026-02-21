#!/usr/bin/env bash
#
# Example: first single-subject run on Calypso (Apptainer), recorded by DataLad.
#
# Usage:
#   sbatch --partition <PART> --account <ACCT> code/calypso/first_subject.sbatch
#
# Assumptions:
# - You submit from the superdataset root (so $SLURM_SUBMIT_DIR is the repo)
# - You have micromamba env "datalad" (see code/calypso/README.md)
# - You have env/secrets/fs_license.txt on the filesystem (gitignored)
#

#SBATCH --job-name=fmriprep-first
#SBATCH --output=logs/%x_%j.out
#SBATCH --error=logs/%x_%j.err
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=09:59:00

set -euo pipefail
NAS_HOME="$HOME/nas_home"
export MAMBA_EXE="$NAS_HOME/bin/micromamba"
export MAMBA_ROOT_PREFIX="$NAS_HOME/micromamba"
export PATH="$NAS_HOME/bin:$PATH"
eval "$($MAMBA_EXE shell hook --shell bash --root-prefix $MAMBA_ROOT_PREFIX)"
micromamba activate datalad-nas

DL=(datalad)

PROJECT_ROOT="${PROJECT_ROOT:-${SLURM_SUBMIT_DIR:-$PWD}}"
SUBJECT="${SUBJECT:-v1s0x0050642}"
CONTAINER_NAME="${CONTAINER_NAME:-fmriprep-apptainer}"

echo "[INFO] PROJECT_ROOT=$PROJECT_ROOT"
echo "[INFO] SUBJECT=$SUBJECT"
echo "[INFO] CONTAINER_NAME=$CONTAINER_NAME"

cd "$PROJECT_ROOT"

# If Calypso requires a module to expose Apptainer/Singularity, load it here.
# (Exact module names vary.)
# module load apptainer || module load singularity || true

# Ensure we are not committing into master by accident
BRANCH="$(git branch --show-current)"
if [[ "$BRANCH" == "master" ]]; then
  echo "[FATAL] Refusing to run on branch 'master'. Create/switch to 'run/first-local' first." >&2
  exit 2
fi

DERIV_BRANCH="$(git -C derivatives/fmriprep-25.2 branch --show-current)"
if [[ "$DERIV_BRANCH" == "master" ]]; then
  echo "[FATAL] Refusing to run with derivatives on branch 'master'. Switch derivatives/fmriprep-25.2 to 'run/first-local' first." >&2
  exit 2
fi

# Ensure subdatasets are installed (metadata-only)
"${DL[@]}" get -n inputs/abide-both inputs/templateflow derivatives/fmriprep-25.2

# Fetch the merged subject (data is retrievable via registered 'web' URLs)
"${DL[@]}" get -r "inputs/abide-both/sub-${SUBJECT}"

# Container mount variables (MUST be absolute paths)
export INPUTS_DIR_HOST="$PROJECT_ROOT/inputs"
export OUT_DIR_HOST="$PROJECT_ROOT/derivatives/fmriprep-25.2"
export TEMPLATEFLOW_HOME_HOST="$PROJECT_ROOT/inputs/templateflow"
export FS_LICENSE_FILE="$PROJECT_ROOT/env/secrets/fs_license.txt"
export FMRIPREP_WORKDIR="${SLURM_TMPDIR:-/tmp}/${USER}/fmriprep-work-${SLURM_JOB_ID}"
mkdir -p "$FMRIPREP_WORKDIR"

if [[ ! -f "$FS_LICENSE_FILE" ]]; then
  echo "[FATAL] Missing FreeSurfer license: $FS_LICENSE_FILE" >&2
  exit 2
fi

# Keep Apptainer cache/tmp off $HOME if possible
export APPTAINER_CACHEDIR="${SCRATCH:-$PROJECT_ROOT/.tmp}/apptainer-cache"
export APPTAINER_TMPDIR="${SLURM_TMPDIR:-/tmp}/${USER}/apptainer-tmp"
mkdir -p "$APPTAINER_CACHEDIR" "$APPTAINER_TMPDIR"

"${DL[@]}" containers-run -n "$CONTAINER_NAME" \
  --explicit \
  -m "fMRIPrep abide-both sub-${SUBJECT} (Calypso first run)" \
  --input "inputs/abide-both/sub-${SUBJECT}" \
  --output "derivatives/fmriprep-25.2" \
  -- \
  /bids/abide-both /out participant \
    --participant-label "${SUBJECT}" \
    --skip-bids-validation \
    --output-layout bids \
    --fs-license-file /fs/license.txt \
    --nthreads "${SLURM_CPUS_PER_TASK:-16}" \
    --omp-nthreads "${SLURM_CPUS_PER_TASK:-16}" \
    --mem-mb "${SLURM_MEM_PER_NODE:-64000}" \
    -w /work

DERIV_STATUS="$(git -C derivatives/fmriprep-25.2 status --porcelain=v1 || true)"
if [[ -n "$DERIV_STATUS" ]]; then
  echo "[WARN] derivatives/fmriprep-25.2 is dirty after containers-run; saving explicitly:"
  echo "$DERIV_STATUS" | head -n 50
  "${DL[@]}" -C derivatives/fmriprep-25.2 save -m "fmriprep: save outputs for sub-${SUBJECT} (Calypso)"
  # Record updated gitlink in the superdataset (only if needed)
  "${DL[@]}" save -m "chore: update derivatives pointer for sub-${SUBJECT}" derivatives/fmriprep-25.2 || true
fi

echo "[INFO] Run complete. Pushing results..."

# Derivatives: push data+git to GIN (batch-friendly: relies on SSH keys)
"${DL[@]}" -d derivatives/fmriprep-25.2 push --to gin --data anything

# Derivatives: push git only to GitHub
"${DL[@]}" -d derivatives/fmriprep-25.2 push --to origin --data nothing

# Superdataset: push run record + updated gitlink (git only) to GitHub
"${DL[@]}" push --to origin --data nothing

echo "[INFO] DONE"
