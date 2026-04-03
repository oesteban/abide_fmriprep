#!/bin/bash
#SBATCH --job-name=abide-timeseries
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=01:00:00
#SBATCH --output=logs/timeseries_%A_%a.out
#SBATCH --error=logs/timeseries_%A_%a.err

set -euo pipefail

# -------------------------------------------------------------------
# Per-subject MSDL time series extraction (SLURM array job).
#
# Usage:
#   sbatch --array=1-N code/analysis_timeseries.sbatch \
#       --project-root /path/to/abide_fmriprep \
#       --subjects-file /path/to/subjects.txt \
#       --variant v1
# -------------------------------------------------------------------


PROJECT_ROOT=""
SUBJECTS_FILE=""
VARIANT="v1"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --project-root) PROJECT_ROOT="$2"; shift 2 ;;
        --subjects-file) SUBJECTS_FILE="$2"; shift 2 ;;
        --variant) VARIANT="$2"; shift 2 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

if [[ -z "$PROJECT_ROOT" ]] || [[ -z "$SUBJECTS_FILE" ]]; then
    echo "ERROR: --project-root and --subjects-file are required"
    exit 1
fi

source "$PROJECT_ROOT/code/analysis_env.sh"

SUBJECT=$(sed -n "${SLURM_ARRAY_TASK_ID}p" "$SUBJECTS_FILE")
if [[ -z "$SUBJECT" ]]; then
    echo "ERROR: No subject at line $SLURM_ARRAY_TASK_ID in $SUBJECTS_FILE"
    exit 1
fi

echo "=== Time series extraction ==="
echo "Subject: $SUBJECT"
echo "Variant: $VARIANT"
echo "Array task: $SLURM_ARRAY_TASK_ID"
echo "Host: $(hostname)"
echo "Date: $(date -Iseconds)"

python3 "$PROJECT_ROOT/code/analysis/02_extract_timeseries.py" \
    --project-root "$PROJECT_ROOT" \
    --participant-id "$SUBJECT" \
    --variant "$VARIANT"

echo "=== Done ==="
