#!/usr/bin/env bash
#SBATCH --job-name=test-ria-push
#SBATCH --output=logs/%x_%j.out
#SBATCH --error=logs/%x_%j.err
#SBATCH --cpus-per-task=2
#SBATCH --mem=4G
#SBATCH --time=00:15:00

set -euo pipefail

# -------------------------
# Mock fMRIPrep job: tests the RIA + GIN two-stage push pathway
# Creates a dummy file in derivatives, commits, and pushes.
# -------------------------

die() { echo "[FATAL] $*" >&2; exit 2; }

PROJECT_ROOT=""
GIN_REMOTE="gin"
SUBJECT="v1s0x0050642"  # default test subject

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project-root) PROJECT_ROOT="$2"; shift 2 ;;
    --gin-remote) GIN_REMOTE="$2"; shift 2 ;;
    --subject) SUBJECT="$2"; shift 2 ;;
    *) die "Unknown arg: $1" ;;
  esac
done

[[ -n "$PROJECT_ROOT" ]] || die "--project-root is required"
[[ -d "$PROJECT_ROOT" ]] || die "Project root not found: $PROJECT_ROOT"

SUBJECT="${SUBJECT#sub-}"
PROC_REL="derivatives/fmriprep-25.2"

echo "[INFO] PROJECT_ROOT=$PROJECT_ROOT"
echo "[INFO] SUBJECT=sub-${SUBJECT}"
echo "[INFO] GIN_REMOTE=$GIN_REMOTE"

# -------------------------
# Job-local clone
# -------------------------
JOB_SCRATCH="${SLURM_TMPDIR:-/tmp}/${USER}/test-ria-push/${SLURM_JOB_ID:-$$}"
mkdir -p "$JOB_SCRATCH"

JOB_CLONE="${JOB_SCRATCH}/project"
echo "[INFO] Cloning project into scratch: $JOB_CLONE"
datalad clone "$PROJECT_ROOT" "$JOB_CLONE"
cd "$JOB_CLONE"

# Install derivatives subdataset (metadata only)
datalad get -n "$PROC_REL"

# Set up NAS-resident RIA sibling in the clone (safety net for results).
# The clone's subdataset comes from GitHub which lacks the ria-nas git remote.
# We read the URL from the project root and add it directly, then fetch
# the git-annex branch to get the ria-nas-storage special remote config.
RIA_NAS_URL="$(git -C "$PROJECT_ROOT/$PROC_REL" remote get-url ria-nas 2>/dev/null || true)"
if [[ -n "$RIA_NAS_URL" ]]; then
  echo "[INFO] Adding RIA sibling 'ria-nas' ($RIA_NAS_URL) to clone's derivatives subdataset"
  git -C "$PROC_REL" remote add ria-nas "$RIA_NAS_URL"
  git -C "$PROC_REL" fetch "$PROJECT_ROOT/$PROC_REL" +git-annex:git-annex 2>/dev/null || true
  git -C "$PROC_REL" annex enableremote ria-nas-storage 2>/dev/null || true
else
  echo "[WARN] RIA sibling not configured in project root (results will only go to GIN)"
fi

# Show siblings for debugging
echo "[INFO] Derivatives siblings:"
datalad siblings -d "$PROC_REL"

# Create job branch
OUT_DIR_HOST="$JOB_CLONE/$PROC_REL"
JOB_BRANCH="test/ria-push/sub-${SUBJECT}/${SLURM_JOB_ID:-$$}"
echo "[INFO] Checking out test branch: $JOB_BRANCH"
git -C "$OUT_DIR_HOST" checkout -b "$JOB_BRANCH"

# -------------------------
# Mock fMRIPrep: create a dummy output file
# -------------------------
MOCK_DIR="$OUT_DIR_HOST/sub-${SUBJECT}/ses-1/func"
mkdir -p "$MOCK_DIR"
MOCK_FILE="$MOCK_DIR/sub-${SUBJECT}_ses-1_task-rest_space-MNI152NLin2009cAsym_desc-mocktest_bold.nii.gz"

echo "[INFO] Creating mock output: $MOCK_FILE"
# Create a small gzip file (not a real NIfTI, just for testing push)
echo "mock-fmriprep-output $(date -Iseconds) job=${SLURM_JOB_ID:-$$}" | gzip > "$MOCK_FILE"

# Commit the mock output via datalad save
echo "[INFO] Saving mock output with datalad"
datalad save -d "$OUT_DIR_HOST" -m "TEST: mock fMRIPrep sub-${SUBJECT} (job ${SLURM_JOB_ID:-$$})"

# -------------------------
# Push: RIA (safety net) then GIN (permanent)
# -------------------------
# Add GIN remote to clone's derivatives subdataset
GIN_PUSH_URL="$(git -C "$PROJECT_ROOT/$PROC_REL" config remote."${GIN_REMOTE}".pushurl 2>/dev/null || \
  git -C "$PROJECT_ROOT/$PROC_REL" config remote."${GIN_REMOTE}".url 2>/dev/null || true)"
if [[ -n "$GIN_PUSH_URL" ]]; then
  echo "[INFO] Adding '$GIN_REMOTE' remote ($GIN_PUSH_URL) to clone's derivatives subdataset"
  git -C "$OUT_DIR_HOST" remote add "$GIN_REMOTE" "$GIN_PUSH_URL"
fi

PUSH_OK=0

# Stage 1: Push to NAS-resident RIA store (local NFS — fast, no auth)
if git -C "$OUT_DIR_HOST" remote get-url ria-nas &>/dev/null; then
  echo "[INFO] Pushing to RIA store (NAS safety net)"
  if datalad push -d "$OUT_DIR_HOST" --to ria-nas --data anything; then
    echo "[INFO] RIA push succeeded — results are safe on NAS"
    PUSH_OK=1
  else
    echo "[WARN] RIA push failed"
  fi
else
  echo "[WARN] ria-nas remote not available in clone"
fi

# Stage 2: Push to GIN (permanent remote storage)
if [[ -n "$GIN_PUSH_URL" ]]; then
  echo "[INFO] Pushing to '$GIN_REMOTE' (branch: $JOB_BRANCH)"
  if datalad push -d "$OUT_DIR_HOST" --to "$GIN_REMOTE" --data anything; then
    echo "[INFO] GIN push succeeded"
    PUSH_OK=1
  else
    echo "[WARN] GIN push failed"
  fi
else
  echo "[WARN] GIN remote not configured — skipping"
fi

# Fail only if BOTH pushes failed
if [[ "$PUSH_OK" -eq 0 ]]; then
  die "Both RIA and GIN pushes failed. Results exist ONLY in scratch: $JOB_SCRATCH"
fi

# Cleanup
echo "[INFO] Dropping annexed content from clone"
datalad drop -d "$OUT_DIR_HOST" -r . || true

echo "[INFO] DONE. Test passed. Job scratch: $JOB_SCRATCH"
