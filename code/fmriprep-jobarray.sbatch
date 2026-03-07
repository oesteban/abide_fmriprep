#!/usr/bin/env bash
#SBATCH --job-name=fmriprep
#SBATCH --output=${JOB_LOG_DIR}/%x_%A_%a.out
#SBATCH --error=${JOB_LOG_DIR}/%x_%A_%a.err
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

list_done_subjects() {
  local proc_dir="$1"
  git -C "$proc_dir" ls-tree --name-only master 2>/dev/null \
    | grep '^sub-' | grep -v '\.html$' || true
}

detect_cluster() {
  # SLURM_CLUSTER_NAME is the most reliable source inside a SLURM job.
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
GIN_REMOTE="gin"
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
    [--dataset abide1|abide2] \\
    [--site <SITE>] \\
    --subjects-file lists/<file>

Usage (single subject):
  sbatch code/fmriprep-jobarray.sbatch \\
    --project-root /path/to/abide-fmriprep-yoda \\
    [--dataset abide1|abide2] \\
    [--site <SITE>] \\
    --subject <sub-v1sXxXXXX|v1sXxXXXX>

Optional:
  --fs-license-file /path/to/license.txt (defaults to FS_LICENSE or <project-root>/env/secrets/fs_license.txt)
  --templateflow-home-host /path/to/templateflow (defaults to <project-root>/inputs/templateflow)
  --container-name <name>  (default: fmriprep-docker; e.g., fmriprep-apptainer)
  --ria-store <URL>        (RIA store URL for cloning, e.g. ria+file:///path/to/store)
  --gin-remote <name>      (default: gin)
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
    --gin-remote) GIN_REMOTE="$2"; shift 2 ;;
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
PROC_REL="derivatives/fmriprep-25.2"

# -------------------------
# Sanity checks (fail early if project isn't set up)
# -------------------------
need_cmd datalad
need_cmd git

[[ -n "${JOB_LOG_DIR:-}" ]] || die "\$JOB_LOG_DIR must be set before sbatch submission.
  Curnagl:  export JOB_LOG_DIR=\$SCRATCH/abide_fmriprep/logs
  Calypso:  export JOB_LOG_DIR=\$HOME/nas_home/abide_fmriprep/logs"

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
    # Skip if already on master
    _check="sub-${SUBJECT#sub-}"
    if list_done_subjects "$PROJECT_ROOT/$PROC_REL" | grep -qxF "$_check"; then
      echo "[INFO] Subject $_check already on master — skipping."
      exit 0
    fi
  else
    [[ -n "${SLURM_ARRAY_TASK_ID:-}" ]] || die "SLURM_ARRAY_TASK_ID not set (submit as an array?)"
    mapfile -t SUBJECTS < <(
      comm -23 \
        <(list_subjects_from_participants "$PARTICIPANTS_TSV" "$DATASET" "$SITE" | sort) \
        <(list_done_subjects "$PROJECT_ROOT/$PROC_REL" | sort)
    )
    NUM_SUBJECTS="${#SUBJECTS[@]}"
    echo "[INFO] ${NUM_SUBJECTS} subjects to process (already-done on master excluded)"
    [[ "$NUM_SUBJECTS" -gt 0 ]] || die "No subjects found in participants.tsv for dataset/site filters (or all already done)"
    IDX=$((SLURM_ARRAY_TASK_ID - 1))
    if [[ "$IDX" -lt 0 || "$IDX" -ge "$NUM_SUBJECTS" ]]; then
      die "SLURM_ARRAY_TASK_ID (${SLURM_ARRAY_TASK_ID}) out of range 1..${NUM_SUBJECTS}"
    fi
    SUBJECT="${SUBJECTS[$IDX]}"
  fi
fi

SUBJECT="${SUBJECT#sub-}"

PARTICIPANT_ID="sub-${SUBJECT}"

# Skip if already on master
if list_done_subjects "$PROJECT_ROOT/$PROC_REL" | grep -qxF "$PARTICIPANT_ID"; then
  echo "[INFO] Subject $PARTICIPANT_ID already on master — skipping."
  exit 0
fi

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
# JOB_SCRATCH is initialized empty at the top so the EXIT trap is safe.
# Assign the real path here; the trap will clean it up when the job ends.
JOB_SCRATCH="${SLURM_TMPDIR:-${TMPDIR:-/tmp}}/${USER}/bootstrap-fmriprep/${SLURM_JOB_ID:-$$}_${SLURM_ARRAY_TASK_ID:-0}"
mkdir -p "$JOB_SCRATCH"

JOB_CLONE="${JOB_SCRATCH}/project"
WORKDIR="${JOB_SCRATCH}/work"
mkdir -p "$WORKDIR"

# Clone from RIA store (annex-capable) when available; fall back to project root.
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

# Install (but don't download) the processed subdataset so we can commit into it
datalad get -n "$PROC_REL"

# Set up NAS-resident RIA sibling in the clone (safety net for results).
# Only needed on Calypso — Curnagl uses GIN-only push.
if [[ "$CLUSTER" != "curnagl" ]]; then
  RIA_NAS_URL="$(git -C "$PROJECT_ROOT/$PROC_REL" remote get-url ria-nas 2>/dev/null || true)"
  if [[ -n "$RIA_NAS_URL" ]]; then
    echo "[INFO] Adding RIA sibling 'ria-nas' ($RIA_NAS_URL) to clone's derivatives subdataset"
    git -C "$PROC_REL" remote add ria-nas "$RIA_NAS_URL"
    git -C "$PROC_REL" config remote.ria-nas.annex-ignore true
    git -C "$PROC_REL" fetch "$PROJECT_ROOT/$PROC_REL" +git-annex:git-annex 2>/dev/null || true
    git -C "$PROC_REL" annex enableremote ria-nas-storage 2>/dev/null || true
    git -C "$PROC_REL" config remote.ria-nas.datalad-publish-depends ria-nas-storage
  else
    echo "[WARN] RIA sibling not configured in project root (results will only go to GIN)"
  fi
else
  echo "[INFO] Curnagl: skipping RIA sibling (GIN-only push)"
fi

# Ensure merged input subdataset is installed in the clone (metadata only)
datalad get -n "$BOTH_REL"

# Fetch pre-computed PyBIDS database index (local copy from PROJECT_ROOT).
# Must be pre-fetched once on the persistent project root:
#   datalad get -d inputs/abide-both inputs/abide-both/.bids-layout-db/layout_index.sqlite
datalad get "$BOTH_REL/.bids-layout-db/layout_index.sqlite"
echo "[INFO] PyBIDS database index available at $BOTH_REL/.bids-layout-db"

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

ANAT_ONLY_FLAG=""
if [[ "$ANAT_ONLY" == "1" ]]; then
  ANAT_ONLY_FLAG="--anat-only"
  echo "[INFO] Running in anat-only mode"
fi

# Remove stale fsaverage from cloned derivatives.
# When a previous job commits fsaverage, .gitattributes puts label files (text)
# into git and surf files (binary) into annex.  Subsequent clones get real label
# files but broken annex symlinks for surfaces.  BIDSFreeSurferDir's FS7 marker
# check (a label file) passes, so it skips the copytree — leaving FreeSurfer
# unable to read fsaverage/surf/rh.sphere.reg.  Removing the directory forces a
# fresh copytree from the container's $FREESURFER_HOME/subjects/fsaverage.
FSAVG_DIR="$OUT_DIR_HOST/sourcedata/freesurfer/fsaverage"
if [[ -d "$FSAVG_DIR" ]]; then
  echo "[INFO] Removing stale fsaverage from cloned derivatives (forces fresh copytree)"
  chmod -R u+w "$FSAVG_DIR" 2>/dev/null || true
  rm -rf "$FSAVG_DIR"
fi

# CIFTI default resolution is 91k; 170k also supported.
FMRIPREP_START_ISO="$(date '+%Y-%m-%dT%H:%M:%S')"
echo "[INFO] fmriprep_start=$FMRIPREP_START_ISO"
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
# Set up GIN remote
# -------------------------
# GIN (gin.g-node.org) runs Gogs and supports git-annex content transfer
# over SSH. When --gin-push-data=anything (default), both branches and annex
# content are pushed; when nothing, only branches are pushed (annex content
# stays on the RIA store and can be synced to GIN later).
GIN_PUSH_URL="$(git -C "$PROJECT_ROOT/$PROC_REL" config remote."${GIN_REMOTE}".pushurl 2>/dev/null || \
  git -C "$PROJECT_ROOT/$PROC_REL" config remote."${GIN_REMOTE}".url 2>/dev/null || true)"
if [[ -z "$GIN_PUSH_URL" ]]; then
  die "GIN remote '$GIN_REMOTE' not configured in $PROJECT_ROOT/$PROC_REL"
fi
echo "[INFO] Adding '$GIN_REMOTE' remote ($GIN_PUSH_URL) to clone's derivatives subdataset"
git -C "$OUT_DIR_HOST" remote add "$GIN_REMOTE" "$GIN_PUSH_URL"
if [[ "$GIN_PUSH_DATA" == "nothing" ]]; then
  git -C "$OUT_DIR_HOST" config remote."${GIN_REMOTE}".annex-ignore true
fi

# -------------------------
# Push processed results (cluster-aware)
# -------------------------
push_to_gin_with_backoff() {
  local max_attempts=7
  local delay=15
  local attempt

  for (( attempt=1; attempt<=max_attempts; attempt++ )); do
    echo "[INFO] GIN push attempt $attempt/$max_attempts (branch: $JOB_BRANCH, data: $GIN_PUSH_DATA)"
    if datalad push -d "$OUT_DIR_HOST" --to "$GIN_REMOTE" --data "$GIN_PUSH_DATA"; then
      echo "[INFO] GIN push succeeded on attempt $attempt"
      return 0
    fi

    if (( attempt < max_attempts )); then
      echo "[WARN] GIN push failed — fetching git-annex branch and retrying in ${delay}s"
      git -C "$OUT_DIR_HOST" fetch "$GIN_REMOTE" git-annex 2>/dev/null || true
      git -C "$OUT_DIR_HOST" annex merge 2>/dev/null || true
      sleep "$delay"
      delay=$(( delay * 2 ))
    fi
  done

  # All attempts exhausted — rescue working repo to persistent storage
  local rescue_dir="${SCRATCH:-/tmp}/abide-rescue/${SLURM_ARRAY_JOB_ID:-${SLURM_JOB_ID:-0}}_${SLURM_ARRAY_TASK_ID:-0}"
  echo "[FATAL] All $max_attempts GIN push attempts failed — rescuing to $rescue_dir"
  mkdir -p "$rescue_dir"
  rsync -a "$OUT_DIR_HOST/" "$rescue_dir/"
  die "GIN push failed after $max_attempts attempts. Rescued repo: $rescue_dir"
}

# Convert fmriprep.toml dir name (YYYYMMDD-HHMMSS) to ISO 8601
toml_dir_to_iso() {
  echo "$1" | sed 's/\([0-9]\{4\}\)\([0-9]\{2\}\)\([0-9]\{2\}\)-\([0-9]\{2\}\)\([0-9]\{2\}\)\([0-9]\{2\}\)/\1-\2-\3T\4:\5:\6/'
}

# Convert nipype timestamp (YYMMDD-HH:MM:SS) to ISO 8601
nipype_ts_to_iso() {
  echo "$1" | sed 's/^\([0-9]\{2\}\)\([0-9]\{2\}\)\([0-9]\{2\}\)-/20\1-\2-\3T/'
}

merge_into_master() {
  # Inline merge of job branch into master in the central repo (Curnagl only).
  # All steps non-fatal — merge_job_branches.sh handles cleanup later.
  (
    flock -w 120 9 || { echo "[WARN] Could not acquire merge lock within 120s — skipping inline merge"; exit 0; }

    cd "$PROJECT_ROOT/$PROC_REL" || { echo "[WARN] Cannot cd to derivatives — skipping merge"; exit 0; }

    # Fetch the job branch from GIN
    git fetch "$GIN_REMOTE" "$JOB_BRANCH" 2>/dev/null \
      || { echo "[WARN] Could not fetch $JOB_BRANCH from $GIN_REMOTE — skipping merge"; exit 0; }

    # Ensure we are on master
    git checkout master 2>/dev/null \
      || { echo "[WARN] Cannot checkout master — skipping merge"; exit 0; }

    # Attempt merge
    if ! git merge --no-edit FETCH_HEAD 2>/dev/null; then
      # Check if only CITATION files conflict
      conflicted="$(git diff --name-only --diff-filter=U)"
      only_citation=1
      while IFS= read -r cfile; do
        case "$cfile" in
          logs/CITATION.md|logs/CITATION.html|logs/CITATION.tex) ;;
          *) only_citation=0; break ;;
        esac
      done <<< "$conflicted"

      if [[ $only_citation -eq 1 && -n "$conflicted" ]]; then
        git checkout master -- logs/CITATION.md logs/CITATION.html logs/CITATION.tex 2>/dev/null || true
        git add logs/CITATION.md logs/CITATION.html logs/CITATION.tex
        git commit --no-edit
        echo "[INFO] Merged $JOB_BRANCH (CITATION conflict resolved — kept master)"
      else
        echo "[WARN] Merge conflict in non-CITATION files for $JOB_BRANCH — aborting"
        git merge --abort
        exit 0
      fi
    else
      echo "[INFO] Merged $JOB_BRANCH into master"
    fi

    # --- Extract metadata for participants.tsv ---

    # stc_ref_time from logs/CITATION.md
    stc_val="$(sed -n 's/.*slice-time corrected to \([0-9.]*\)s.*/\1/p' logs/CITATION.md 2>/dev/null | head -1)" || true

    # fmriprep_start: prefer fmriprep.toml dir name on the branch
    fmriprep_start=""
    start_raw="$(git ls-tree -r --name-only FETCH_HEAD -- "${PARTICIPANT_ID}/" 2>/dev/null \
      | sed -n 's|.*/log/\([0-9]\{8\}-[0-9]\{6\}\)_.*/fmriprep\.toml|\1|p' \
      | head -1)" || true
    if [[ -n "$start_raw" ]]; then
      fmriprep_start="$(toml_dir_to_iso "$start_raw")"
    fi
    : "${fmriprep_start:=${FMRIPREP_START_ISO:-n/a}}"

    # fmriprep_stop: prefer nipype timestamp from SLURM log
    fmriprep_stop=""
    _logfile="${JOB_LOG_DIR}/fmriprep_${SLURM_ARRAY_JOB_ID:-${SLURM_JOB_ID:-0}}_${SLURM_ARRAY_TASK_ID:-0}.out"
    if [[ -f "$_logfile" ]]; then
      stop_raw="$(grep -B1 'fMRIPrep finished successfully' "$_logfile" \
        | sed -n 's/^\([0-9]\{6\}-[0-9:]*\).*/\1/p' | head -1)" || true
      if [[ -n "$stop_raw" ]]; then
        fmriprep_stop="$(nipype_ts_to_iso "$stop_raw")"
      fi
    fi
    : "${fmriprep_stop:=${FMRIPREP_STOP_ISO:-n/a}}"

    # --- Upsert subject row in participants.tsv ---
    tsv="participants.tsv"
    stc_display="${stc_val:-n/a}"

    if [[ ! -f "$tsv" ]]; then
      printf 'participant_id\tstc_ref_time\tfmriprep_start\tfmriprep_stop\n' > "$tsv"
    fi

    {
      awk -F'\t' -v pid="$PARTICIPANT_ID" 'NR==1{next} $1!=pid{print}' "$tsv"
      printf '%s\t%s\t%s\t%s\n' "$PARTICIPANT_ID" "$stc_display" "$fmriprep_start" "$fmriprep_stop"
    } | sort -t$'\t' -k1,1 > "${tsv}.body"

    {
      printf 'participant_id\tstc_ref_time\tfmriprep_start\tfmriprep_stop\n'
      cat "${tsv}.body"
    } > "$tsv"
    rm -f "${tsv}.body"

    git add participants.tsv
    if ! git diff --cached --quiet -- participants.tsv; then
      git commit -m "enh: update participants.tsv for $PARTICIPANT_ID"
      echo "[INFO] Updated participants.tsv for $PARTICIPANT_ID"
    fi

    # Push master to GIN (single attempt)
    if git push "$GIN_REMOTE" master 2>/dev/null; then
      echo "[INFO] Pushed master to $GIN_REMOTE"
    else
      echo "[WARN] Could not push master to $GIN_REMOTE — next merge will pick it up"
    fi

  ) 9>"$PROJECT_ROOT/$PROC_REL/.merge.lock"
}

case "$CLUSTER" in
  curnagl)
    push_to_gin_with_backoff
    merge_into_master || echo "[WARN] Inline merge failed (non-fatal)"
    ;;
  *)
    # Calypso: RIA push (safety net) then GIN push (single retry)
    PUSH_OK=0

    if git -C "$OUT_DIR_HOST" remote get-url ria-nas &>/dev/null; then
      echo "[INFO] Pushing to RIA store (NAS safety net)"
      if datalad push -d "$OUT_DIR_HOST" --to ria-nas --data anything; then
        echo "[INFO] RIA push succeeded — results are safe on NAS"
        PUSH_OK=1

        echo "[INFO] Syncing git-annex location tracking to central repo"
        git -C "$PROJECT_ROOT/$PROC_REL" fetch ria-nas git-annex 2>/dev/null \
          && git -C "$PROJECT_ROOT/$PROC_REL" annex merge 2>/dev/null \
          || echo "[WARN] Could not sync git-annex branch to central repo (non-fatal)"
      else
        echo "[WARN] RIA push failed"
      fi
    fi

    echo "[INFO] Pushing to '$GIN_REMOTE' (branch: $JOB_BRANCH, data: $GIN_PUSH_DATA)"
    if datalad push -d "$OUT_DIR_HOST" --to "$GIN_REMOTE" --data "$GIN_PUSH_DATA"; then
      echo "[INFO] GIN push succeeded (data=$GIN_PUSH_DATA)"
      PUSH_OK=1
    else
      echo "[WARN] GIN push failed — retrying after git-annex branch sync"
      git -C "$OUT_DIR_HOST" fetch "$GIN_REMOTE" git-annex 2>/dev/null || true
      git -C "$OUT_DIR_HOST" annex merge 2>/dev/null || true
      if datalad push -d "$OUT_DIR_HOST" --to "$GIN_REMOTE" --data "$GIN_PUSH_DATA"; then
        echo "[INFO] GIN push succeeded on retry"
        PUSH_OK=1
      else
        echo "[WARN] GIN push failed on retry"
      fi
    fi

    if [[ "$PUSH_OK" -eq 0 ]]; then
      die "Both RIA and GIN pushes failed. Results exist ONLY in scratch: $JOB_SCRATCH"
    fi
    ;;
esac

# Drop raw subject content from the job clone (free annexed inputs before scratch cleanup)
echo "[INFO] Dropping raw subject content from job clone"
datalad drop -d "$JOB_CLONE" -r "$BOTH_SUBDIR_REL" || true

echo "[INFO] fMRIPrep finished successfully for sub-${SUBJECT}."
# EXIT trap will clean up $JOB_SCRATCH automatically.
