#!/usr/bin/env bash
#SBATCH --job-name=fmriprep
#SBATCH --output=logs/%x_%A_%a.out
#SBATCH --error=logs/%x_%A_%a.err
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=10:00:00

set -euo pipefail

# -------------------------
# Scratch cleanup (trap on EXIT — runs on success, failure, and signals)
# -------------------------
# git-annex locks object directories (dr-xr-xr-x), so chmod before rm.
JOB_SCRATCH=""
cleanup() {
  local rc=$?
  if [[ -n "$JOB_SCRATCH" && -d "$JOB_SCRATCH" ]]; then
    echo "[INFO] Cleaning up scratch: $JOB_SCRATCH"
    chmod -R u+w "$JOB_SCRATCH" 2>/dev/null || true
    rm -rf "$JOB_SCRATCH"
  fi
  exit $rc
}
trap cleanup EXIT

# -------------------------
# Early environment setup (must run before any datalad/git-annex/singularity commands)
# -------------------------
_early_cluster="${SLURM_CLUSTER_NAME:-unknown}"
case "$_early_cluster" in
  curnagl)
    export PATH="/work/FAC/FBM/DNF/oesteban/hcph/opt/mamba/envs/fmriprep/bin:$PATH"
    module load gcc/12.3.0 singularityce/4.1.0 2>/dev/null || true
    ;;
  calypso)
    module load singularity 2>/dev/null || true
    ;;
esac

# -------------------------
# Helpers
# -------------------------
die() { echo "[FATAL] $*" >&2; exit 2; }

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"
}

extract_site_prefix() {
  # sub-v1s0x0050642 → v1s0, or v1s0x0050642 → v1s0
  local id="${1#sub-}"
  echo "${id%%x*}"
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

list_done_subjects() {
  # Check all site-level datasets and the legacy monolithic dataset.
  local deriv_dir="$1"
  # New site-level datasets (derivatives/v1s0, derivatives/v2s3, etc.)
  for site_dir in "$deriv_dir"/v[12]s*/; do
    [[ -d "$site_dir" ]] || continue
    [[ -d "$site_dir/.git" || -f "$site_dir/.git" ]] || continue
    git -C "$site_dir" ls-tree --name-only master 2>/dev/null \
      | grep '^sub-' | grep -v '\.html$' || true
  done
  # Legacy: old monolithic derivatives/fmriprep-25.2
  if [[ -d "$deriv_dir/fmriprep-25.2" ]] \
     && [[ -d "$deriv_dir/fmriprep-25.2/.git" || -f "$deriv_dir/fmriprep-25.2/.git" ]]; then
    git -C "$deriv_dir/fmriprep-25.2" ls-tree --name-only master 2>/dev/null \
      | grep '^sub-' | grep -v '\.html$' || true
  fi
}

detect_cluster() {
  if [[ -n "${SLURM_CLUSTER_NAME:-}" ]]; then
    echo "$SLURM_CLUSTER_NAME"
    return
  fi
  local fqdn
  fqdn="$(hostname -f 2>/dev/null || hostname)"
  case "$fqdn" in
    *.dcsr.unil.ch|curnagl|dna*) echo "curnagl" ;;
    calypso*)                     echo "calypso" ;;
    *)                            echo "unknown" ;;
  esac
}

push_with_backoff() {
  local ds_path="$1"
  local remote="$2"
  local data_mode="$3"
  local max_attempts="${4:-5}"
  local delay=15

  for (( attempt=1; attempt<=max_attempts; attempt++ )); do
    echo "[INFO] Push attempt $attempt/$max_attempts (remote: $remote, data: $data_mode)"
    if datalad push -d "$ds_path" --to "$remote" --data "$data_mode"; then
      echo "[INFO] Push succeeded on attempt $attempt"
      return 0
    fi

    if (( attempt < max_attempts )); then
      echo "[WARN] Push failed — syncing git-annex and retrying in ${delay}s"
      git -C "$ds_path" fetch "$remote" git-annex 2>/dev/null || true
      git -C "$ds_path" annex merge 2>/dev/null || true
      sleep "$delay"
      delay=$(( delay * 2 ))
    fi
  done

  # All attempts exhausted — rescue working repo to persistent storage
  local rescue_dir="${SCRATCH:-/tmp}/abide-rescue/${SLURM_ARRAY_JOB_ID:-${SLURM_JOB_ID:-0}}_${SLURM_ARRAY_TASK_ID:-0}"
  echo "[FATAL] All $max_attempts push attempts failed — rescuing to $rescue_dir"
  mkdir -p "$rescue_dir"
  rsync -a "$ds_path/" "$rescue_dir/"
  die "Push failed after $max_attempts attempts. Rescued repo: $rescue_dir"
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
RIA_STORE=""
GIN_ORG="abide-fmriprep"
GIN_PUSH_DATA="anything"

CIFTI_DENSITY="91k"
OUTPUT_LAYOUT="bids"
SKIP_BIDS_VALIDATION=1
ANAT_ONLY=0

usage() {
  cat <<EOF
Usage (array, auto-discover subjects from inputs/abide-both):
  sbatch --array=1-N code/fmriprep-jobarray.sbatch \\
    --project-root /path/to/abide-fmriprep-yoda \\
    [--dataset abide1|abide2] \\
    [--site <SITE>]

Usage (array, explicit subjects list):
  sbatch --array=1-N code/fmriprep-jobarray.sbatch \\
    --project-root /path/to/abide-fmriprep-yoda \\
    --subjects-file lists/<file>

Usage (single subject):
  sbatch code/fmriprep-jobarray.sbatch \\
    --project-root /path/to/abide-fmriprep-yoda \\
    --subject <sub-v1sXxXXXX|v1sXxXXXX>

Optional:
  --fs-license-file /path/to/license.txt (defaults to FS_LICENSE or <project-root>/env/secrets/fs_license.txt)
  --templateflow-home-host /path/to/templateflow (defaults to <project-root>/inputs/templateflow)
  --container-name <name>  (default: fmriprep-docker; e.g., fmriprep-apptainer)
  --ria-store <URL>        (RIA store URL for cloning, e.g. ria+file:///path/to/store)
  --gin-org <org>          (GIN organization for site repos; default: abide-fmriprep)
  --gin-push-data nothing|anything (default: anything)
  --cifti-density 91k|170k (default: 91k)
  --skip-bids-validation 0|1 (default: 1)
  --anat-only              (run only anatomical processing)
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
    --ria-store) RIA_STORE="$2"; shift 2 ;;
    --gin-org) GIN_ORG="$2"; shift 2 ;;
    --gin-push-data) GIN_PUSH_DATA="$2"; shift 2 ;;
    --cifti-density) CIFTI_DENSITY="$2"; shift 2 ;;
    --skip-bids-validation) SKIP_BIDS_VALIDATION="$2"; shift 2 ;;
    --anat-only) ANAT_ONLY=1; shift ;;
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
DERIV_REL="derivatives"

# -------------------------
# Sanity checks (fail early if project isn't set up)
# -------------------------
need_cmd datalad
need_cmd git

JOB_LOG_DIR="${PROJECT_ROOT}/logs"
mkdir -p "$JOB_LOG_DIR"

CLUSTER="$(detect_cluster)"
echo "[INFO] Cluster: $CLUSTER"

case "$CLUSTER" in
  curnagl)  ;; # /users/ is GPFS-shared — ~/.gitconfig visible everywhere
  calypso)  export GIT_CONFIG_GLOBAL="$HOME/nas_home/.gitconfig" ;;
  *)        echo "[WARN] Unknown cluster — no cluster-specific setup applied" ;;
esac

# Warn if --ria-store was passed on Curnagl (no RIA store available)
if [[ "$CLUSTER" == "curnagl" && -n "$RIA_STORE" ]]; then
  echo "[WARN] --ria-store ignored on Curnagl (GIN-only push)"
  RIA_STORE=""
fi

[[ -d "$PROJECT_ROOT" ]] || die "Project root not found: $PROJECT_ROOT"
git -C "$PROJECT_ROOT" rev-parse --is-inside-work-tree >/dev/null 2>&1 \
  || die "--project-root is not a Git repository: $PROJECT_ROOT (expected YODA superdataset)"

# Defaults that depend on project-root
: "${TEMPLATEFLOW_HOME_HOST:=$PROJECT_ROOT/$TF_REL}"

[[ -d "$TEMPLATEFLOW_HOME_HOST" ]] || die "TemplateFlow path not found: $TEMPLATEFLOW_HOME_HOST"
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


# -------------------------
# Subject selection
# -------------------------
if [[ -z "$SUBJECT" ]]; then
  if [[ -n "$SUBJECTS_FILE" ]]; then
    [[ -n "${SLURM_ARRAY_TASK_ID:-}" ]] || die "SLURM_ARRAY_TASK_ID not set (submit as an array?)"
    [[ -f "$SUBJECTS_FILE" ]] || die "Subjects file not found: $SUBJECTS_FILE"
    SUBJECT="$(sed -n "${SLURM_ARRAY_TASK_ID}p" "$SUBJECTS_FILE" | tr -d '\r' | xargs)"
    [[ -n "$SUBJECT" ]] || die "No subject found at line ${SLURM_ARRAY_TASK_ID} in $SUBJECTS_FILE"
    # Skip if already done
    _check="sub-${SUBJECT#sub-}"
    if list_done_subjects "$PROJECT_ROOT/$DERIV_REL" | grep -qxF "$_check"; then
      echo "[INFO] Subject $_check already done — skipping."
      exit 0
    fi
  else
    [[ -n "${SLURM_ARRAY_TASK_ID:-}" ]] || die "SLURM_ARRAY_TASK_ID not set (submit as an array?)"
    mapfile -t SUBJECTS < <(
      comm -23 \
        <(list_subjects_from_participants "$PARTICIPANTS_TSV" "$DATASET" "$SITE" | sort) \
        <(list_done_subjects "$PROJECT_ROOT/$DERIV_REL" | sort)
    )
    NUM_SUBJECTS="${#SUBJECTS[@]}"
    echo "[INFO] ${NUM_SUBJECTS} subjects to process (already-done excluded)"
    [[ "$NUM_SUBJECTS" -gt 0 ]] || die "No subjects found (or all already done)"
    IDX=$((SLURM_ARRAY_TASK_ID - 1))
    if [[ "$IDX" -lt 0 || "$IDX" -ge "$NUM_SUBJECTS" ]]; then
      die "SLURM_ARRAY_TASK_ID (${SLURM_ARRAY_TASK_ID}) out of range 1..${NUM_SUBJECTS}"
    fi
    SUBJECT="${SUBJECTS[$IDX]}"
  fi
fi

SUBJECT="${SUBJECT#sub-}"
PARTICIPANT_ID="sub-${SUBJECT}"

# Skip if already done (final check for --subject mode)
if list_done_subjects "$PROJECT_ROOT/$DERIV_REL" | grep -qxF "$PARTICIPANT_ID"; then
  echo "[INFO] Subject $PARTICIPANT_ID already done — skipping."
  exit 0
fi

PARTICIPANT_ROW="$(lookup_participant "$PARTICIPANTS_TSV" "$PARTICIPANT_ID")"
[[ -n "$PARTICIPANT_ROW" ]] || die "Subject not found in participants.tsv: $PARTICIPANT_ID"

IFS=$'\t' read -r SOURCE_DATASET SOURCE_SITE SITE_INDEX SOURCE_SUBJECT <<<"$PARTICIPANT_ROW"

# -------------------------
# Derive site prefix and paths
# -------------------------
SITE_PREFIX="$(extract_site_prefix "$SUBJECT")"
SITE_REL="${DERIV_REL}/${SITE_PREFIX}"

[[ -d "$PROJECT_ROOT/$SITE_REL" ]] \
  || die "Site dataset not found: $PROJECT_ROOT/$SITE_REL (run create_site_datasets.sh)"

echo "[INFO] PROJECT_ROOT=$PROJECT_ROOT"
echo "[INFO] FILTER_DATASET=${DATASET:-<none>}  FILTER_SITE=${SITE:-<none>}"
echo "[INFO] SUBJECT=$PARTICIPANT_ID  SOURCE=$SOURCE_DATASET/$SOURCE_SITE  SITE_PREFIX=$SITE_PREFIX"
echo "[INFO] TEMPLATEFLOW_HOME_HOST=$TEMPLATEFLOW_HOME_HOST"
echo "[INFO] CONTAINER_NAME=$CONTAINER_NAME"

# -------------------------
# Job-local clone (concurrency-safe)
# -------------------------
JOB_SCRATCH="${SLURM_TMPDIR:-${TMPDIR:-/tmp}}/${USER}/bootstrap-fmriprep/${SLURM_JOB_ID:-$$}_${SLURM_ARRAY_TASK_ID:-0}"
mkdir -p "$JOB_SCRATCH"

JOB_CLONE="${JOB_SCRATCH}/project"
WORKDIR="${JOB_SCRATCH}/work"
mkdir -p "$WORKDIR"

# Clone from RIA store when available; fall back to project root.
DATASET_ID="$(git -C "$PROJECT_ROOT" config --file .datalad/config --get datalad.dataset.id)"
if [[ -n "$RIA_STORE" ]]; then
  CLONE_URL="${RIA_STORE}#${DATASET_ID}"
  echo "[INFO] Cloning from RIA store: $CLONE_URL"
  datalad clone "$CLONE_URL" "$JOB_CLONE"
else
  echo "[INFO] Cloning project into scratch: $JOB_CLONE"
  datalad clone "$PROJECT_ROOT" "$JOB_CLONE"
fi

cd "$JOB_CLONE"

# -------------------------
# Install site subdataset and set up job branch
# -------------------------
echo "[INFO] Installing site subdataset: $SITE_REL"
datalad get -n "$SITE_REL"

# Check if subject already on master in the site dataset
if git -C "$SITE_REL" ls-tree --name-only master -- "$PARTICIPANT_ID" 2>/dev/null | grep -q .; then
  echo "[INFO] Subject $PARTICIPANT_ID already on master in $SITE_PREFIX — skipping."
  exit 0
fi

# Set up GIN remote in the cloned site subdataset
GIN_URL="git@gin.g-node.org:/${GIN_ORG}/${SITE_PREFIX}.git"
git -C "$SITE_REL" remote add gin "$GIN_URL" 2>/dev/null || true

# Check if job branch already exists on GIN (prior run awaiting reconciliation)
JOB_BRANCH="job/${PARTICIPANT_ID}"
if git -C "$SITE_REL" ls-remote --heads gin "$JOB_BRANCH" 2>/dev/null | grep -q .; then
  echo "[INFO] Job branch $JOB_BRANCH already on GIN — skipping."
  exit 0
fi

# Create job branch in site subdataset
echo "[INFO] Creating job branch: $JOB_BRANCH"
git -C "$SITE_REL" checkout -b "$JOB_BRANCH"

# Disable stale Calypso RIA siblings inherited from dataset config.
for _ds in . "$SITE_REL" "$BOTH_REL"; do
  git -C "$_ds" annex dead calypso-ria-storage 2>/dev/null || true
done

# -------------------------
# Fetch input data
# -------------------------
# Ensure merged input subdataset is installed in the clone (metadata only)
datalad get -n "$BOTH_REL"

# Fetch pre-computed PyBIDS database index
datalad get "$BOTH_REL/.bids-layout-db/layout_index.sqlite"
echo "[INFO] PyBIDS database index available at $BOTH_REL/.bids-layout-db"

# Get the one subject (recursively) in the merged dataset
BOTH_SUBDIR_REL="${BOTH_REL}/sub-${SUBJECT}"
echo "[INFO] Fetching merged subject: $BOTH_SUBDIR_REL"
datalad get -r "$BOTH_SUBDIR_REL"

# BIDS root is the merged dataset
BIDS_ROOT_HOST="$JOB_CLONE/$BOTH_REL"
if [[ ! -f "$BIDS_ROOT_HOST/dataset_description.json" ]]; then
  die "No dataset_description.json found in merged BIDS root: $BIDS_ROOT_HOST"
fi
echo "[INFO] BIDS_ROOT_HOST=$BIDS_ROOT_HOST"

# -------------------------
# Set up container environment
# -------------------------
# OUT_DIR_HOST points to the site dataset (fMRIPrep writes directly into it)
OUT_DIR_HOST="$JOB_CLONE/$SITE_REL"

export INPUTS_DIR_HOST="$JOB_CLONE/inputs"
export OUT_DIR_HOST
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

ANAT_ONLY_FLAG=""
if [[ "$ANAT_ONLY" == "1" ]]; then
  ANAT_ONLY_FLAG="--anat-only"
  echo "[INFO] Running in anat-only mode"
fi

# Remove stale fsaverage from cloned site dataset.
# When the site dataset is cloned, label files (git-tracked) are real but surf files
# (annex) are broken symlinks. BIDSFreeSurferDir's marker check passes on the label
# file, so it skips the copytree — leaving FreeSurfer unable to read
# fsaverage/surf/rh.sphere.reg. Removing the directory forces a fresh copytree from
# the container's $FREESURFER_HOME/subjects/fsaverage.
FSAVG_DIR="$OUT_DIR_HOST/sourcedata/freesurfer/fsaverage"
if [[ -d "$FSAVG_DIR" ]]; then
  echo "[INFO] Removing stale fsaverage from cloned site dataset (forces fresh copytree)"
  chmod -R u+w "$FSAVG_DIR" 2>/dev/null || true
  rm -rf "$FSAVG_DIR"
fi

# -------------------------
# Discover sessions and build --output declarations
# -------------------------
# With --explicit, only declared outputs are saved. This prevents shared files
# (dataset_description.json, CITATION.*, fsaverage/) from being committed per-job,
# which would cause merge conflicts during reconciliation.
OUTPUT_ARGS=(
  --output "${SITE_REL}/${PARTICIPANT_ID}"
  --output "${SITE_REL}/${PARTICIPANT_ID}.html"
)

# Discover FreeSurfer session output paths from input data
for ses_dir in "$BIDS_ROOT_HOST/${PARTICIPANT_ID}"/ses-*/; do
  if [[ -d "$ses_dir" ]]; then
    ses="$(basename "$ses_dir")"
    OUTPUT_ARGS+=(--output "${SITE_REL}/sourcedata/freesurfer/${PARTICIPANT_ID}_${ses}")
  fi
done

echo "[INFO] Output declarations: ${OUTPUT_ARGS[*]}"

# -------------------------
# Run fMRIPrep
# -------------------------
FMRIPREP_START_ISO="$(date '+%Y-%m-%dT%H:%M:%S')"
echo "[INFO] fmriprep_start=$FMRIPREP_START_ISO"
echo "[INFO] Running fMRIPrep via datalad containers-run"
datalad containers-run -n "$CONTAINER_NAME" \
  --explicit \
  -m "fMRIPrep ${SOURCE_DATASET} ${SOURCE_SITE} ${PARTICIPANT_ID}" \
  --input "$BOTH_SUBDIR_REL" \
  "${OUTPUT_ARGS[@]}" \
  -- \
  /bids/abide-both /out participant \
    --participant-label "$SUBJECT" \
    $BIDSVAL_FLAG \
    $ANAT_ONLY_FLAG \
    --output-layout "$OUTPUT_LAYOUT" \
    --fs-license-file /fs/license.txt \
    --cifti-output "$CIFTI_DENSITY" \
    --output-spaces MNI152NLin2009cAsym \
    --bids-database-dir /bids/abide-both/.bids-layout-db \
    --nthreads "$NTHREADS" \
    --omp-nthreads "$OMP_NTHREADS" \
    --mem-mb "$MEM_MB" \
    -w /work

FMRIPREP_STOP_ISO="$(date '+%Y-%m-%dT%H:%M:%S')"
echo "[INFO] fmriprep_stop=$FMRIPREP_STOP_ISO"

# -------------------------
# Push site subdataset job branch to GIN
# -------------------------
# The containers-run commit lands on the job branch in the site subdataset.
# Push both the branch and annex content to the site's GIN repo.
# A separate reconciliation step (code/reconcile_subdatasets.sh) octopus-merges
# all job branches into master after a batch completes.
echo "[INFO] Pushing site subdataset $SITE_PREFIX to GIN (branch: $JOB_BRANCH, data: $GIN_PUSH_DATA)"
push_with_backoff "$JOB_CLONE/$SITE_REL" gin "$GIN_PUSH_DATA"

# -------------------------
# Cleanup
# -------------------------
# Drop raw subject content from the job clone (free annexed inputs before scratch cleanup)
echo "[INFO] Dropping raw subject content from job clone"
datalad drop -d "$JOB_CLONE" -r "$BOTH_SUBDIR_REL" || true

echo "[INFO] fMRIPrep finished successfully for ${PARTICIPANT_ID}."
# EXIT trap will clean up $JOB_SCRATCH automatically.
