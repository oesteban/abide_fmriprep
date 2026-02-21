#!/usr/bin/env bash
#SBATCH --job-name=bootstrap-fmriprep
#SBATCH --output=logs/%x_%A_%a.out
#SBATCH --error=logs/%x_%A_%a.err
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=10:00:00

set -euo pipefail

# -------------------------
# Helpers
# -------------------------
die() { echo "[FATAL] $*" >&2; exit 2; }

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"
}

list_subjects_from_participants() {
  local participants_tsv="$1"
  local dataset_filter="$2"
  local site_filter="$3"
  awk -F'\t' -v ds="$dataset_filter" -v site="$site_filter" '
    NR==1 { next }
    (ds == "" || $2 == ds) && (site == "" || $3 == site) { print $1 }
  ' "$participants_tsv"
}

lookup_participant() {
  local participants_tsv="$1"
  local participant_id="$2"
  awk -F'\t' -v pid="$participant_id" '
    NR==1 { next }
    $1 == pid { print $2 "\t" $3 "\t" $4 "\t" $5; exit }
  ' "$participants_tsv"
}

# -------------------------
# Args
# -------------------------
PROJECT_ROOT=""
DATASET=""
SITE=""
SUBJECT=""
SUBJECTS_FILE=""

FS_LICENSE_FILE=""
TEMPLATEFLOW_HOME_HOST=""
CONTAINER_NAME="fmriprep-docker"
CONTAINER_IMAGE=""
GIN_REMOTE="gin"

CIFTI_DENSITY="91k"
OUTPUT_LAYOUT="bids"
SKIP_BIDS_VALIDATION=1

usage() {
  cat <<EOF
Usage (array, auto-discover subjects from inputs/abide-both):
  sbatch --array=1-N code/bootstrap_fmriprep_ARRAY.sbatch.sh \\
    --project-root /path/to/abide-fmriprep-yoda \\
    [--dataset abide1|abide2] \\
    [--site <SITE>]

Usage (array, explicit subjects list):
  sbatch --array=1-N code/bootstrap_fmriprep_ARRAY.sbatch.sh \\
    --project-root /path/to/abide-fmriprep-yoda \\
    [--dataset abide1|abide2] \\
    [--site <SITE>] \\
    --subjects-file lists/<file>

Usage (single subject):
  sbatch code/bootstrap_fmriprep_ARRAY.sbatch.sh \\
    --project-root /path/to/abide-fmriprep-yoda \\
    [--dataset abide1|abide2] \\
    [--site <SITE>] \\
    --subject <sub-v1sXxXXXX|v1sXxXXXX>

Optional:
  --fs-license-file /path/to/license.txt (defaults to FS_LICENSE or <project-root>/env/secrets/fs_license.txt)
  --templateflow-home-host /path/to/templateflow (defaults to <project-root>/inputs/templateflow)
  --container-name <name>  (default: fmriprep-docker; e.g., fmriprep-apptainer)
  --container-image /path/to/image (defaults to FMRIPREP_SIF env var; overrides .datalad/config image)
  --gin-remote <name>      (default: gin)
  --cifti-density 91k|170k (default: 91k)
  --skip-bids-validation 0|1 (default: 1)
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project-root) PROJECT_ROOT="$2"; shift 2 ;;
    --dataset) DATASET="$2"; shift 2 ;;
    --site) SITE="$2"; shift 2 ;;
    --subject) SUBJECT="$2"; shift 2 ;;
    --subjects-file) SUBJECTS_FILE="$2"; shift 2 ;;
    --fs-license-file) FS_LICENSE_FILE="$2"; shift 2 ;;
    --templateflow-home-host) TEMPLATEFLOW_HOME_HOST="$2"; shift 2 ;;
    --container-name) CONTAINER_NAME="$2"; shift 2 ;;
    --container-image) CONTAINER_IMAGE="$2"; shift 2 ;;
    --gin-remote) GIN_REMOTE="$2"; shift 2 ;;
    --cifti-density) CIFTI_DENSITY="$2"; shift 2 ;;
    --skip-bids-validation) SKIP_BIDS_VALIDATION="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) die "Unknown arg: $1" ;;
  esac
done

[[ -n "$PROJECT_ROOT" ]] || die "--project-root is required"
if [[ -n "$DATASET" ]]; then
  DATASET="$(echo "$DATASET" | tr '[:upper:]' '[:lower:]')"
  case "$DATASET" in
    abide1|abide2) ;;
    *) die "--dataset must be abide1 or abide2 when provided" ;;
  esac
fi

# -------------------------
# YODA-relative paths inside project-root
# -------------------------
BOTH_REL="inputs/abide-both"
TF_REL="inputs/templateflow"
PROC_REL="derivatives/fmriprep-25.2"

# -------------------------
# Sanity checks (fail early if project isn't set up)
# -------------------------
need_cmd datalad
need_cmd git

[[ -d "$PROJECT_ROOT" ]] || die "Project root not found: $PROJECT_ROOT"
git -C "$PROJECT_ROOT" rev-parse --is-inside-work-tree >/dev/null 2>&1 \
  || die "--project-root is not a Git repository: $PROJECT_ROOT (expected YODA superdataset)"

# Defaults that depend on project-root
: "${TEMPLATEFLOW_HOME_HOST:=$PROJECT_ROOT/$TF_REL}"

[[ -d "$TEMPLATEFLOW_HOME_HOST" ]] || die "TemplateFlow path not found: $TEMPLATEFLOW_HOME_HOST"
[[ -d "$PROJECT_ROOT/$PROC_REL" ]] || die "Processed subdataset path not found: $PROJECT_ROOT/$PROC_REL"
[[ -d "$PROJECT_ROOT/$BOTH_REL" ]] || die "Merged input subdataset path missing: $PROJECT_ROOT/$BOTH_REL"

PARTICIPANTS_TSV="$PROJECT_ROOT/$BOTH_REL/participants.tsv"
[[ -f "$PARTICIPANTS_TSV" ]] || die "participants.tsv not found: $PARTICIPANTS_TSV (run build_abide_both.py)"

# FreeSurfer license: default to FS_LICENSE or env/secrets
if [[ -z "$FS_LICENSE_FILE" ]]; then
  if [[ -n "${FS_LICENSE:-}" ]]; then
    FS_LICENSE_FILE="$FS_LICENSE"
  else
    FS_LICENSE_FILE="$PROJECT_ROOT/env/secrets/fs_license.txt"
  fi
fi
[[ -f "$FS_LICENSE_FILE" ]] || die "FreeSurfer license file not found: $FS_LICENSE_FILE (set --fs-license-file or FS_LICENSE)"


# Subject from array list if not provided
if [[ -z "$SUBJECT" ]]; then
  if [[ -n "$SUBJECTS_FILE" ]]; then
    [[ -n "${SLURM_ARRAY_TASK_ID:-}" ]] || die "SLURM_ARRAY_TASK_ID not set (submit as an array?)"
    [[ -f "$SUBJECTS_FILE" ]] || die "Subjects file not found: $SUBJECTS_FILE"
    SUBJECT="$(sed -n "${SLURM_ARRAY_TASK_ID}p" "$SUBJECTS_FILE" | tr -d '\r' | xargs)"
    [[ -n "$SUBJECT" ]] || die "No subject found at line ${SLURM_ARRAY_TASK_ID} in $SUBJECTS_FILE"
  else
    [[ -n "${SLURM_ARRAY_TASK_ID:-}" ]] || die "SLURM_ARRAY_TASK_ID not set (submit as an array?)"
    mapfile -t SUBJECTS < <(
      list_subjects_from_participants "$PARTICIPANTS_TSV" "$DATASET" "$SITE" | sort
    )
    NUM_SUBJECTS="${#SUBJECTS[@]}"
    [[ "$NUM_SUBJECTS" -gt 0 ]] || die "No subjects found in participants.tsv for dataset/site filters"
    IDX=$((SLURM_ARRAY_TASK_ID - 1))
    if [[ "$IDX" -lt 0 || "$IDX" -ge "$NUM_SUBJECTS" ]]; then
      die "SLURM_ARRAY_TASK_ID (${SLURM_ARRAY_TASK_ID}) out of range 1..${NUM_SUBJECTS}"
    fi
    SUBJECT="${SUBJECTS[$IDX]}"
  fi
fi

SUBJECT="${SUBJECT#sub-}"

PARTICIPANT_ID="sub-${SUBJECT}"
PARTICIPANT_ROW="$(lookup_participant "$PARTICIPANTS_TSV" "$PARTICIPANT_ID")"
[[ -n "$PARTICIPANT_ROW" ]] || die "Subject not found in participants.tsv: $PARTICIPANT_ID"

IFS=$'\t' read -r SOURCE_DATASET SOURCE_SITE SITE_INDEX SOURCE_SUBJECT <<<"$PARTICIPANT_ROW"

echo "[INFO] PROJECT_ROOT=$PROJECT_ROOT"
echo "[INFO] FILTER_DATASET=${DATASET:-<none>}  FILTER_SITE=${SITE:-<none>}"
echo "[INFO] SUBJECT=sub-${SUBJECT}  SOURCE_DATASET=$SOURCE_DATASET  SOURCE_SITE=$SOURCE_SITE  SOURCE_SUBJECT=$SOURCE_SUBJECT  SITE_INDEX=$SITE_INDEX"
echo "[INFO] TEMPLATEFLOW_HOME_HOST=$TEMPLATEFLOW_HOME_HOST"
echo "[INFO] CONTAINER_NAME=$CONTAINER_NAME  GIN_REMOTE=$GIN_REMOTE"

# -------------------------
# Job-local clone (concurrency-safe)
# -------------------------
JOB_SCRATCH="${SLURM_TMPDIR:-/tmp}/${USER}/bootstrap-fmriprep/${SLURM_JOB_ID:-$$}_${SLURM_ARRAY_TASK_ID:-0}"
mkdir -p "$JOB_SCRATCH"

JOB_CLONE="${JOB_SCRATCH}/project"
WORKDIR="${JOB_SCRATCH}/work"
mkdir -p "$WORKDIR"

echo "[INFO] Cloning project into scratch: $JOB_CLONE"
datalad clone "$PROJECT_ROOT" "$JOB_CLONE"

cd "$JOB_CLONE"

# Install (but don't download) the processed subdataset so we can commit into it
datalad get -n "$PROC_REL"

# Ensure merged input subdataset is installed in the clone (metadata only)
datalad get -n "$BOTH_REL"

# Get the one subject (recursively) in the merged dataset.
# inputs/abide-both is a self-contained git-annex dataset (no cross-dataset symlinks),
# so we can retrieve content directly from its registered web URLs.
BOTH_SUBDIR_REL="${BOTH_REL}/sub-${SUBJECT}"
BOTH_SUBDIR_ABS="$JOB_CLONE/$BOTH_SUBDIR_REL"
echo "[INFO] datalad get merged subject: $BOTH_SUBDIR_REL"
datalad get -r "$BOTH_SUBDIR_REL"

# BIDS root is the merged dataset
BIDS_ROOT_HOST="$JOB_CLONE/$BOTH_REL"
if [[ ! -f "$BIDS_ROOT_HOST/dataset_description.json" ]]; then
  die "No dataset_description.json found in merged BIDS root: $BIDS_ROOT_HOST"
fi
echo "[INFO] BIDS_ROOT_HOST=$BIDS_ROOT_HOST"

# Prepare processed dataset branch (branch-per-job)
OUT_DIR_HOST="$JOB_CLONE/$PROC_REL"
JOB_BRANCH="job/abide-both/${SOURCE_DATASET}/${SOURCE_SITE}/sub-${SUBJECT}/${SLURM_JOB_ID:-$$}_${SLURM_ARRAY_TASK_ID:-0}"
echo "[INFO] Checking out processed job branch: $JOB_BRANCH"
git -C "$OUT_DIR_HOST" checkout -b "$JOB_BRANCH"

# Export vars consumed by the container call-format (configured via datalad containers-add)
# NOTE: The shipped container definition mounts INPUTS_DIR_HOST to /bids, and
# we pass /bids/abide-both as the BIDS root to fMRIPrep.
export INPUTS_DIR_HOST="$JOB_CLONE/inputs"
export OUT_DIR_HOST="$OUT_DIR_HOST"
export TEMPLATEFLOW_HOME_HOST="$TEMPLATEFLOW_HOME_HOST"
export FS_LICENSE_FILE="$FS_LICENSE_FILE"
export FMRIPREP_WORKDIR="$WORKDIR"

# fMRIPrep resources
NTHREADS="${SLURM_CPUS_PER_TASK:-1}"
OMP_NTHREADS="${SLURM_CPUS_PER_TASK:-1}"
MEM_MB="${SLURM_MEM_PER_NODE:-64000}"

# Build fMRIPrep flags
BIDSVAL_FLAG=""
if [[ "$SKIP_BIDS_VALIDATION" == "1" ]]; then
  BIDSVAL_FLAG="--skip-bids-validation"
fi

# Resolve container image: CLI flag > FMRIPREP_SIF env var
: "${CONTAINER_IMAGE:=${FMRIPREP_SIF:-}}"
if [[ -n "$CONTAINER_IMAGE" ]]; then
  [[ -f "$CONTAINER_IMAGE" ]] || die "Container image not found: $CONTAINER_IMAGE"
  export FMRIPREP_SIF="$CONTAINER_IMAGE"
fi
echo "[INFO] FMRIPREP_SIF=${FMRIPREP_SIF:-<not set, using container config>}"

# CIFTI default resolution is 91k; 170k also supported.
echo "[INFO] Running fMRIPrep via datalad containers-run"
datalad containers-run -n "$CONTAINER_NAME" \
  --explicit \
  -m "fMRIPrep abide-both ${SOURCE_DATASET} ${SOURCE_SITE} sub-${SUBJECT}" \
  --input "$BOTH_SUBDIR_REL" \
  --output "$PROC_REL" \
  -- \
  /bids/abide-both /out participant \
    --participant-label "$SUBJECT" \
    $BIDSVAL_FLAG \
    --output-layout "$OUTPUT_LAYOUT" \
    --fs-license-file /fs/license.txt \
    --cifti-output "$CIFTI_DENSITY" \
    --output-spaces MNI152NLin2009cAsym fsLR \
    --nthreads "$NTHREADS" \
    --omp-nthreads "$OMP_NTHREADS" \
    --mem-mb "$MEM_MB" \
    -w /work

# Push processed dataset branch to GIN (data + git)
echo "[INFO] Pushing processed dataset to '$GIN_REMOTE' (branch: $JOB_BRANCH)"
datalad -d "$OUT_DIR_HOST" push --to "$GIN_REMOTE" --data anything

# Drop derivatives content in the processed dataset clone (step 5)
echo "[INFO] Dropping all annexed content from processed clone (post-push)"
datalad -d "$OUT_DIR_HOST" drop -r .

# Drop raw subject in the job clone (step 4)
echo "[INFO] Dropping raw subject content from job clone"
datalad -d "$JOB_CLONE" drop -r "$BOTH_SUBDIR_REL" || true

echo "[INFO] DONE. Job scratch: $JOB_SCRATCH"
