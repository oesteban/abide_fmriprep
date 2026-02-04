#!/usr/bin/env bash
#SBATCH --job-name=bootstrap-fmriprep
#SBATCH --output=logs/%x_%A_%a.out
#SBATCH --error=logs/%x_%A_%a.err
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=24:00:00

set -euo pipefail

# -------------------------
# Helpers
# -------------------------
die() { echo "[FATAL] $*" >&2; exit 2; }

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"
}

site_to_code() {
  # Example: BNI_1 -> BNI1 ; CMU_a -> CMUA ; NYU -> NYU
  local site="$1"
  echo "$site" | tr -cd '[:alnum:]' | tr '[:lower:]' '[:upper:]'
}

make_new_label() {
  # label WITHOUT "sub-"
  local ver="$1"      # v1 or v2
  local sitecode="$2" # e.g., BNI1
  local orig="$3"     # original subject label WITHOUT "sub-"
  echo "${ver}+${sitecode}+${orig}"
}

with_lock() {
  local lockfile="$1"; shift
  mkdir -p "$(dirname "$lockfile")"
  exec 9>"$lockfile"
  flock -x 9
  "$@"
  flock -u 9
}

prefetch_templateflow_all() {
  local tf_ds="$1"     # absolute path to templateflow dataset
  local lockfile="$2"  # absolute path to lock file on shared FS
  with_lock "$lockfile" bash -lc "
    cd '$tf_ds'
    datalad get -r .
  "
}

rename_subject_tree() {
  # Rename all files/dirs that contain 'sub-<orig>' into 'sub-<new>'
  # Avoid touching .git/.datalad internals.
  local root="$1"
  local orig="$2"        # without "sub-"
  local newlabel="$3"    # without "sub-"

  local from="sub-${orig}"
  local to="sub-${newlabel}"

  find "$root" -depth \
    \( -path "$root/.git" -o -path "$root/.git/*" -o -path "$root/.datalad" -o -path "$root/.datalad/*" \) -prune -o \
    -name "*${from}*" -print0 \
  | while IFS= read -r -d '' p; do
      local np="${p//${from}/${to}}"
      [[ "$p" == "$np" ]] && continue
      mkdir -p "$(dirname "$np")"
      mv "$p" "$np"
    done
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
CONTAINER_NAME="fmriprep"
GIN_REMOTE="gin"

CIFTI_DENSITY="91k"
OUTPUT_LAYOUT="bids"
SKIP_BIDS_VALIDATION=1

usage() {
  cat <<EOF
Usage (array):
  sbatch --array=1-N code/bootstrap_fmriprep_ARRAY.sbatch.sh \\
    --project-root /path/to/abide-fmriprep-yoda \\
    --dataset abide1|abide2 \\
    --site <SITE> \\
    --subjects-file lists/<file>

Usage (single subject):
  sbatch code/bootstrap_fmriprep_ARRAY.sbatch.sh \\
    --project-root /path/to/abide-fmriprep-yoda \\
    --dataset abide1|abide2 \\
    --site <SITE> \\
    --subject <sub-XXXX|XXXX>

Optional:
  --fs-license-file /path/to/license.txt
  --templateflow-home-host /path/to/templateflow (defaults to <project-root>/inputs/templateflow)
  --container-name <name>  (default: fmriprep)
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
    --gin-remote) GIN_REMOTE="$2"; shift 2 ;;
    --cifti-density) CIFTI_DENSITY="$2"; shift 2 ;;
    --skip-bids-validation) SKIP_BIDS_VALIDATION="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) die "Unknown arg: $1" ;;
  esac
done

[[ -n "$PROJECT_ROOT" ]] || die "--project-root is required"
[[ -n "$DATASET" ]] || die "--dataset is required"
[[ -n "$SITE" ]] || die "--site is required"

# Subject from array list if not provided
if [[ -z "$SUBJECT" ]]; then
  [[ -n "$SUBJECTS_FILE" ]] || die "Either --subject or --subjects-file is required"
  [[ -n "${SLURM_ARRAY_TASK_ID:-}" ]] || die "SLURM_ARRAY_TASK_ID not set (submit as an array?)"
  [[ -f "$SUBJECTS_FILE" ]] || die "Subjects file not found: $SUBJECTS_FILE"
  SUBJECT="$(sed -n "${SLURM_ARRAY_TASK_ID}p" "$SUBJECTS_FILE" | tr -d '\r' | xargs)"
  [[ -n "$SUBJECT" ]] || die "No subject found at line ${SLURM_ARRAY_TASK_ID} in $SUBJECTS_FILE"
fi

SUBJECT="${SUBJECT#sub-}"

# -------------------------
# YODA-relative paths inside project-root
# -------------------------
ABIDE1_REL="inputs/abide1_RawDataBIDS"
ABIDE2_REL="inputs/abide2_RawData"
TF_REL="inputs/templateflow"
PROC_REL="derivatives/abide_fmriprep"

# -------------------------
# Sanity checks (fail early if project isn't set up)
# -------------------------
need_cmd datalad
need_cmd git
need_cmd flock
need_cmd docker

[[ -d "$PROJECT_ROOT" ]] || die "Project root not found: $PROJECT_ROOT"
git -C "$PROJECT_ROOT" rev-parse --is-inside-work-tree >/dev/null 2>&1 \
  || die "--project-root is not a Git repository: $PROJECT_ROOT (expected YODA superdataset)"

# Defaults that depend on project-root
: "${TEMPLATEFLOW_HOME_HOST:=$PROJECT_ROOT/$TF_REL}"

[[ -d "$TEMPLATEFLOW_HOME_HOST" ]] || die "TemplateFlow path not found: $TEMPLATEFLOW_HOME_HOST"
[[ -d "$PROJECT_ROOT/$PROC_REL" ]] || die "Processed subdataset path not found: $PROJECT_ROOT/$PROC_REL"

# FreeSurfer license: require explicit if not provided via env
if [[ -z "$FS_LICENSE_FILE" ]]; then
  die "Provide --fs-license-file (FreeSurfer license must be readable on compute nodes)."
fi
[[ -f "$FS_LICENSE_FILE" ]] || die "FreeSurfer license file not found: $FS_LICENSE_FILE"

case "$DATASET" in
  abide1|ABIDE1)
    RAW_REL="$ABIDE1_REL"
    VER="v1"
    ;;
  abide2|ABIDE2)
    RAW_REL="$ABIDE2_REL"
    VER="v2"
    ;;
  *) die "Unknown dataset: $DATASET (expected abide1|abide2)" ;;
esac

[[ -d "$PROJECT_ROOT/$RAW_REL" ]] || die "Raw input subdataset path missing: $PROJECT_ROOT/$RAW_REL"

SITE_CODE="$(site_to_code "$SITE")"
NEW_LABEL="$(make_new_label "$VER" "$SITE_CODE" "$SUBJECT")"
NEW_SUBDIR="sub-${NEW_LABEL}"

echo "[INFO] PROJECT_ROOT=$PROJECT_ROOT"
echo "[INFO] DATASET=$DATASET  SITE=$SITE (code=$SITE_CODE)  SUBJECT=$SUBJECT  NEW=$NEW_SUBDIR"
echo "[INFO] TEMPLATEFLOW_HOME_HOST=$TEMPLATEFLOW_HOME_HOST"
echo "[INFO] CONTAINER_NAME=$CONTAINER_NAME  GIN_REMOTE=$GIN_REMOTE"

# -------------------------
# Prefetch TemplateFlow once (locked, shared FS)
# -------------------------
TF_LOCK="$PROJECT_ROOT/.git/bootstrap_locks/templateflow_all.lock"
echo "[INFO] Prefetching TemplateFlow in canonical project (locked): $TEMPLATEFLOW_HOME_HOST"
prefetch_templateflow_all "$TEMPLATEFLOW_HOME_HOST" "$TF_LOCK"

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

# Ensure raw input subdataset is installed in the clone (metadata only)
datalad get -n "$RAW_REL"

# Get the one subject (recursively, so func/anat are present)
SUBPATH_REL="${RAW_REL}/${SITE}/sub-${SUBJECT}"
echo "[INFO] datalad get subject: $SUBPATH_REL"
datalad get -r "$SUBPATH_REL"

# Decide BIDS root (site folder preferred if it has dataset_description.json)
BIDS_SITE_DIR="$JOB_CLONE/${RAW_REL}/${SITE}"
BIDS_ROOT_HOST=""
if [[ -f "${BIDS_SITE_DIR}/dataset_description.json" ]]; then
  BIDS_ROOT_HOST="$BIDS_SITE_DIR"
elif [[ -f "$JOB_CLONE/${RAW_REL}/dataset_description.json" ]]; then
  BIDS_ROOT_HOST="$JOB_CLONE/${RAW_REL}"
else
  # fMRIPrep expects the BIDS root to contain dataset_description.json :contentReference[oaicite:2]{index=2}
  die "No dataset_description.json found at site-level or dataset-level. Cannot determine BIDS root for fMRIPrep."
fi
echo "[INFO] BIDS_ROOT_HOST=$BIDS_ROOT_HOST"

# Prepare processed dataset branch (branch-per-job)
OUT_DIR_HOST="$JOB_CLONE/$PROC_REL"
JOB_BRANCH="job/${DATASET}/${SITE_CODE}/${SUBJECT}/${SLURM_JOB_ID:-$$}_${SLURM_ARRAY_TASK_ID:-0}"
echo "[INFO] Checking out processed job branch: $JOB_BRANCH"
git -C "$OUT_DIR_HOST" checkout -b "$JOB_BRANCH"

# Export vars consumed by the container call-format (configured via datalad containers-add)
export BIDS_DIR_HOST="$BIDS_ROOT_HOST"
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

# CIFTI default resolution is 91k; 170k also supported :contentReference[oaicite:3]{index=3}
echo "[INFO] Running fMRIPrep via datalad containers-run"
datalad containers-run -n "$CONTAINER_NAME" \
  --explicit \
  -m "fMRIPrep ${DATASET} ${SITE} sub-${SUBJECT}" \
  --input "$SUBPATH_REL" \
  --output "$PROC_REL" \
  -- \
  /bids /out participant \
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

# Rename outputs to unified subject ID (sub-v1+... or sub-v2+...)
echo "[INFO] Renaming outputs inside processed dataset: sub-${SUBJECT} -> ${NEW_SUBDIR}"
rename_subject_tree "$OUT_DIR_HOST" "$SUBJECT" "$NEW_LABEL"

# Save rename as commit in processed dataset
datalad -d "$OUT_DIR_HOST" save -m "Rename sub-${SUBJECT} -> ${NEW_SUBDIR} (${DATASET}/${SITE})"

# Push processed dataset branch to GIN (data + git)
echo "[INFO] Pushing processed dataset to '$GIN_REMOTE' (branch: $JOB_BRANCH)"
datalad -d "$OUT_DIR_HOST" push --to "$GIN_REMOTE" --data anything

# Drop derivatives content in the processed dataset clone (step 5)
echo "[INFO] Dropping all annexed content from processed clone (post-push)"
datalad -d "$OUT_DIR_HOST" drop -r .

# Drop raw subject in the job clone (step 4)
echo "[INFO] Dropping raw subject content from job clone"
datalad -d "$JOB_CLONE" drop -r "$SUBPATH_REL" || true

echo "[INFO] DONE. Job scratch: $JOB_SCRATCH"
