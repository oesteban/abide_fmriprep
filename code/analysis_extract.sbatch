#!/bin/bash
#SBATCH --job-name=abide-extract
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=00:30:00
#SBATCH --output=logs/extract_%A_%a.out
#SBATCH --error=logs/extract_%A_%a.err

set -euo pipefail

# -------------------------------------------------------------------
# Per-subject extraction array job.
# Stages fMRIPrep BOLD, extracts all variants, drops BOLD.
#
# Usage:
#   sbatch --array=1-N code/analysis_extract.sbatch \
#       --project-root /path \
#       --subjects-file /path/to/subjects.txt \
#       [--variants "v1 v2 v3"]
# -------------------------------------------------------------------

PROJECT_ROOT=""
SUBJECTS_FILE=""
VARIANTS="v1 v2 v3"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --project-root) PROJECT_ROOT="$2"; shift 2 ;;
        --subjects-file) SUBJECTS_FILE="$2"; shift 2 ;;
        --variants) VARIANTS="$2"; shift 2 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

if [[ -z "$PROJECT_ROOT" ]] || [[ -z "$SUBJECTS_FILE" ]]; then
    echo "ERROR: --project-root and --subjects-file required"
    exit 1
fi

# Source env after PROJECT_ROOT is known
source "$PROJECT_ROOT/code/analysis_env.sh"
export PATH="$ANALYSIS_ENV:$DATALAD_ENV:$PATH"

SUBJECT=$(sed -n "${SLURM_ARRAY_TASK_ID}p" "$SUBJECTS_FILE")
if [[ -z "$SUBJECT" ]]; then
    echo "ERROR: No subject at line $SLURM_ARRAY_TASK_ID"
    exit 1
fi

echo "=== Extract: $SUBJECT (variants: $VARIANTS) ==="
echo "Date: $(date -Iseconds)"

# Stage BOLD + mask from GIN
cd "$PROJECT_ROOT/derivatives/fmriprep-25.2"
datalad get "${SUBJECT}/ses-1/func/${SUBJECT}_ses-1_task-rest_*_space-MNI152NLin2009cAsym_desc-preproc_bold.nii.gz" 2>/dev/null || true
datalad get "${SUBJECT}/ses-1/func/${SUBJECT}_ses-1_task-rest_*_space-MNI152NLin2009cAsym_desc-brain_mask.nii.gz" 2>/dev/null || true
cd "$PROJECT_ROOT"

# Extract all variants
python3 code/analysis/extract_subject.py \
    --project-root "$PROJECT_ROOT" \
    --participant-id "$SUBJECT" \
    --variants $VARIANTS

# Drop BOLD to reclaim scratch
cd "$PROJECT_ROOT/derivatives/fmriprep-25.2"
datalad drop "${SUBJECT}/ses-1/func/${SUBJECT}_ses-1_task-rest_*_space-MNI152NLin2009cAsym_desc-preproc_bold.nii.gz" 2>/dev/null || true

echo "=== Done ==="
