#!/usr/bin/env bash
#SBATCH --job-name=test-annex-push
#SBATCH --output=logs/%x_%A_%a.out
#SBATCH --error=logs/%x_%A_%a.err
#SBATCH --cpus-per-task=2
#SBATCH --mem=4G
#SBATCH --time=00:15:00

set -euo pipefail

# -------------------------
# Helpers
# -------------------------
die() { echo "[FATAL] $*" >&2; exit 2; }
need_cmd() { command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"; }

lookup_participant() {
  local participants_tsv="$1"
  local participant_id="$2"
  awk -F'\t' -v pid="$participant_id" '
    NR==1 { next }
    $1 == pid { print $2 "\t" $3 "\t" $4 "\t" $5; exit }
  ' "$participants_tsv"
}

# -------------------------
# Args (same as ARRAY script, subset)
# -------------------------
PROJECT_ROOT=""
SUBJECT=""
GIN_REMOTE="gin"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project-root) PROJECT_ROOT="$2"; shift 2 ;;
    --subject) SUBJECT="$2"; shift 2 ;;
    --gin-remote) GIN_REMOTE="$2"; shift 2 ;;
    *) die "Unknown arg: $1" ;;
  esac
done

[[ -n "$PROJECT_ROOT" ]] || die "--project-root is required"
[[ -n "$SUBJECT" ]] || die "--subject is required"

BOTH_REL="inputs/abide-both"
PROC_REL="derivatives/fmriprep-25.2"

need_cmd datalad
need_cmd git

[[ -d "$PROJECT_ROOT" ]] || die "Project root not found: $PROJECT_ROOT"

PARTICIPANTS_TSV="$PROJECT_ROOT/$BOTH_REL/participants.tsv"
[[ -f "$PARTICIPANTS_TSV" ]] || die "participants.tsv not found"

SUBJECT="${SUBJECT#sub-}"
PARTICIPANT_ID="sub-${SUBJECT}"
PARTICIPANT_ROW="$(lookup_participant "$PARTICIPANTS_TSV" "$PARTICIPANT_ID")"
[[ -n "$PARTICIPANT_ROW" ]] || die "Subject not found: $PARTICIPANT_ID"

IFS=$'\t' read -r SOURCE_DATASET SOURCE_SITE SITE_INDEX SOURCE_SUBJECT <<<"$PARTICIPANT_ROW"

echo "[INFO] TEST: annex content push verification"
echo "[INFO] SUBJECT=$PARTICIPANT_ID  DATASET=$SOURCE_DATASET  SITE=$SOURCE_SITE"

# -------------------------
# Job-local clone
# -------------------------
JOB_SCRATCH="${SLURM_TMPDIR:-/tmp}/${USER}/test-annex-push/${SLURM_JOB_ID:-$$}_${SLURM_ARRAY_TASK_ID:-0}"
mkdir -p "$JOB_SCRATCH"
JOB_CLONE="${JOB_SCRATCH}/project"

echo "[INFO] Cloning project into scratch: $JOB_CLONE"
datalad clone "$PROJECT_ROOT" "$JOB_CLONE"
cd "$JOB_CLONE"

# Install derivatives subdataset (metadata only)
datalad get -n "$PROC_REL"

# ---- RIA sibling setup (identical to ARRAY script) ----
RIA_NAS_URL="$(git -C "$PROJECT_ROOT/$PROC_REL" remote get-url ria-nas 2>/dev/null || true)"
if [[ -n "$RIA_NAS_URL" ]]; then
  echo "[INFO] Adding RIA sibling 'ria-nas' ($RIA_NAS_URL) to clone's derivatives subdataset"
  git -C "$PROC_REL" remote add ria-nas "$RIA_NAS_URL"
  git -C "$PROC_REL" config remote.ria-nas.annex-ignore true
  git -C "$PROC_REL" fetch "$PROJECT_ROOT/$PROC_REL" +git-annex:git-annex 2>/dev/null || true
  git -C "$PROC_REL" annex enableremote ria-nas-storage 2>/dev/null || true
  git -C "$PROC_REL" config remote.ria-nas.datalad-publish-depends ria-nas-storage
else
  echo "[WARN] RIA sibling not configured"
fi

# Prepare job branch
OUT_DIR_HOST="$JOB_CLONE/$PROC_REL"
JOB_BRANCH="test/annex-push/${SLURM_JOB_ID:-$$}_${SLURM_ARRAY_TASK_ID:-0}"
echo "[INFO] Checking out branch: $JOB_BRANCH"
git -C "$OUT_DIR_HOST" checkout -b "$JOB_BRANCH"

# ---- Mock: create a dummy binary file (instead of running fMRIPrep) ----
MOCK_DIR="$OUT_DIR_HOST/sub-${SUBJECT}/ses-1/anat"
mkdir -p "$MOCK_DIR"
# Create a ~100KB binary file (random data, definitely not a text pointer)
dd if=/dev/urandom of="$MOCK_DIR/sub-${SUBJECT}_ses-1_TEST-annex-push.nii.gz" bs=1024 count=100 2>/dev/null
echo "[INFO] Created mock binary: $(ls -la "$MOCK_DIR/sub-${SUBJECT}_ses-1_TEST-annex-push.nii.gz")"

# Commit the mock file via datalad
cd "$JOB_CLONE"
datalad save -d "$PROC_REL" -m "TEST: mock annex content for push verification"

# Verify the file is annexed (not in git)
echo "[INFO] Checking annex status of mock file:"
git -C "$OUT_DIR_HOST" annex whereis "sub-${SUBJECT}/ses-1/anat/sub-${SUBJECT}_ses-1_TEST-annex-push.nii.gz" 2>&1 || true

# ---- GIN remote setup (identical to ARRAY script) ----
# GIN only receives branches (SSH blocked → no annex content transfer)
GIN_PUSH_URL="$(git -C "$PROJECT_ROOT/$PROC_REL" config remote."${GIN_REMOTE}".pushurl 2>/dev/null || \
  git -C "$PROJECT_ROOT/$PROC_REL" config remote."${GIN_REMOTE}".url 2>/dev/null || true)"
if [[ -z "$GIN_PUSH_URL" ]]; then
  die "GIN remote '$GIN_REMOTE' not configured"
fi
echo "[INFO] Adding '$GIN_REMOTE' remote ($GIN_PUSH_URL)"
git -C "$OUT_DIR_HOST" remote add "$GIN_REMOTE" "$GIN_PUSH_URL"
git -C "$OUT_DIR_HOST" config remote."${GIN_REMOTE}".annex-ignore true

# ---- Two-stage push (identical to ARRAY script) ----
PUSH_OK=0

# Stage 1: RIA (annex content + branches)
if git -C "$OUT_DIR_HOST" remote get-url ria-nas &>/dev/null; then
  echo "[INFO] Pushing to RIA store (NAS safety net)"
  if datalad push -d "$OUT_DIR_HOST" --to ria-nas --data anything; then
    echo "[INFO] RIA push succeeded"
    PUSH_OK=1

    # Update the central repo's git-annex branch so `datalad get` can
    # find content on the RIA store without manual intervention.
    echo "[INFO] Syncing git-annex location tracking to central repo"
    git -C "$PROJECT_ROOT/$PROC_REL" fetch ria-nas git-annex 2>/dev/null \
      && git -C "$PROJECT_ROOT/$PROC_REL" annex merge 2>/dev/null \
      || echo "[WARN] Could not sync git-annex branch to central repo (non-fatal)"
  else
    echo "[WARN] RIA push failed"
  fi
fi

# Stage 2: GIN (branches only — no annex content over HTTPS)
echo "[INFO] Pushing branches to '$GIN_REMOTE'"
if datalad push -d "$OUT_DIR_HOST" --to "$GIN_REMOTE" --data nothing; then
  echo "[INFO] GIN push succeeded (branches only)"
  PUSH_OK=1
else
  echo "[WARN] GIN push failed"
fi

if [[ "$PUSH_OK" -eq 0 ]]; then
  die "Both RIA and GIN pushes failed"
fi

# ---- Verify annex content actually arrived ----
echo ""
echo "========================================"
echo "  POST-PUSH VERIFICATION"
echo "========================================"

# Check RIA store
RIA_BARE="$(dirname "$RIA_NAS_URL")/$(basename "$RIA_NAS_URL")"
echo "[CHECK] RIA store annex objects:"
find "$RIA_BARE/annex/objects" -type f 2>/dev/null | wc -l
echo " files in annex/objects"

# Re-check whereis after push
echo "[CHECK] git annex whereis (post-push):"
git -C "$OUT_DIR_HOST" annex whereis "sub-${SUBJECT}/ses-1/anat/sub-${SUBJECT}_ses-1_TEST-annex-push.nii.gz" 2>&1 || true

echo ""
echo "[INFO] TEST COMPLETE. Job scratch: $JOB_SCRATCH"
