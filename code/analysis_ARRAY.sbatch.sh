#!/usr/bin/env bash
#SBATCH --job-name=abide-timeseries
#SBATCH --output=logs/%x_%A_%a.out
#SBATCH --error=logs/%x_%A_%a.err
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=01:00:00

set -euo pipefail

die() { echo "[FATAL] $*" >&2; exit 2; }

# -------------------------
# Args
# -------------------------
PROJECT_ROOT=""
DATASET=""
SITE=""
SUBJECT=""
SUBJECTS_FILE=""
CONDA_ENV="abide-analysis"
OVERWRITE=""

usage() {
  cat <<EOF
Usage (array, all subjects from inputs/abide-both):
  sbatch --array=1-N code/analysis_ARRAY.sbatch.sh \\
    --project-root /path/to/abide_preproc

Usage (array, filtered by dataset/site):
  sbatch --array=1-N code/analysis_ARRAY.sbatch.sh \\
    --project-root /path/to/abide_preproc \\
    --dataset abide1 --site NYU

Usage (single subject):
  sbatch code/analysis_ARRAY.sbatch.sh \\
    --project-root /path/to/abide_preproc \\
    --subject sub-v1s0x0050642

Optional:
  --subjects-file <file>  File with one participant ID per line
  --conda-env <name>      Conda/micromamba environment (default: abide-analysis)
  --overwrite             Overwrite existing outputs
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project-root) PROJECT_ROOT="$2"; shift 2 ;;
    --dataset) DATASET="$2"; shift 2 ;;
    --site) SITE="$2"; shift 2 ;;
    --subject) SUBJECT="$2"; shift 2 ;;
    --subjects-file) SUBJECTS_FILE="$2"; shift 2 ;;
    --conda-env) CONDA_ENV="$2"; shift 2 ;;
    --overwrite) OVERWRITE="--overwrite"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "Unknown arg: $1" ;;
  esac
done

[[ -n "$PROJECT_ROOT" ]] || die "--project-root is required"
[[ -d "$PROJECT_ROOT" ]] || die "Project root not found: $PROJECT_ROOT"

PARTICIPANTS_TSV="$PROJECT_ROOT/inputs/abide-both/participants.tsv"
[[ -f "$PARTICIPANTS_TSV" ]] || die "participants.tsv not found: $PARTICIPANTS_TSV"

list_subjects_from_participants() {
  local participants_tsv="$1"
  local dataset_filter="$2"
  local site_filter="$3"
  awk -F'\t' -v ds="$dataset_filter" -v site="$site_filter" '
    NR==1 { next }
    (ds == "" || $2 == ds) && (site == "" || $3 == site) { print $1 }
  ' "$participants_tsv"
}

# -------------------------
# Resolve subject
# -------------------------
if [[ -z "$SUBJECT" ]]; then
  [[ -n "${SLURM_ARRAY_TASK_ID:-}" ]] || die "SLURM_ARRAY_TASK_ID not set (submit as an array?)"

  if [[ -n "$SUBJECTS_FILE" ]]; then
    [[ -f "$SUBJECTS_FILE" ]] || die "Subjects file not found: $SUBJECTS_FILE"
    SUBJECT="$(sed -n "${SLURM_ARRAY_TASK_ID}p" "$SUBJECTS_FILE" | tr -d '\r' | xargs)"
    [[ -n "$SUBJECT" ]] || die "No subject at line ${SLURM_ARRAY_TASK_ID} in $SUBJECTS_FILE"
  else
    mapfile -t SUBJECTS < <(
      list_subjects_from_participants "$PARTICIPANTS_TSV" "$DATASET" "$SITE" | sort
    )
    NUM_SUBJECTS="${#SUBJECTS[@]}"
    [[ "$NUM_SUBJECTS" -gt 0 ]] || die "No subjects found for filters"
    IDX=$((SLURM_ARRAY_TASK_ID - 1))
    if [[ "$IDX" -lt 0 || "$IDX" -ge "$NUM_SUBJECTS" ]]; then
      die "SLURM_ARRAY_TASK_ID (${SLURM_ARRAY_TASK_ID}) out of range 1..${NUM_SUBJECTS}"
    fi
    SUBJECT="${SUBJECTS[$IDX]}"
  fi
fi

echo "[INFO] PROJECT_ROOT=$PROJECT_ROOT"
echo "[INFO] SUBJECT=$SUBJECT"
echo "[INFO] FILTER_DATASET=${DATASET:-<none>}  FILTER_SITE=${SITE:-<none>}"

# -------------------------
# Run extraction
# -------------------------
micromamba run -n "$CONDA_ENV" python3 \
  "$PROJECT_ROOT/code/analysis/01_extract_timeseries.py" \
  --participant-id "$SUBJECT" \
  --project-root "$PROJECT_ROOT" \
  $OVERWRITE

echo "[INFO] DONE: $SUBJECT"
