#!/bin/bash
#SBATCH --job-name=abide-prescreen
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH --time=01:00:00
#SBATCH --output=logs/prescreen_%j.out
#SBATCH --error=logs/prescreen_%j.err

set -euo pipefail

source "$(dirname "$0")/analysis_env.sh"
export PATH="$ANALYSIS_ENV:$PATH"

PROJECT_ROOT=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --project-root) PROJECT_ROOT="$2"; shift 2 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

if [[ -z "$PROJECT_ROOT" ]]; then
    echo "ERROR: --project-root is required"
    exit 1
fi

echo "=== Pre-screen QC ==="
echo "Project root: $PROJECT_ROOT"
echo "Host: $(hostname)"
echo "Date: $(date -Iseconds)"

python3 "$PROJECT_ROOT/code/analysis/01_prescreen_qc.py" \
    --project-root "$PROJECT_ROOT"

echo "=== Done ==="
