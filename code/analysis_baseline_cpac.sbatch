#!/bin/bash
#SBATCH --job-name=abide-cpac-baseline
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=08:00:00
#SBATCH --output=logs/cpac_baseline_%j.out
#SBATCH --error=logs/cpac_baseline_%j.err

set -euo pipefail


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

source "$PROJECT_ROOT/code/analysis_env.sh"

mkdir -p "$NILEARN_DATA"

echo "=== C-PAC Baseline ==="
echo "Project root: $PROJECT_ROOT"
echo "Data dir: $NILEARN_DATA"
echo "Host: $(hostname)"
echo "Date: $(date -Iseconds)"

python3 "$PROJECT_ROOT/code/analysis/06_baseline_cpac.py" \
    --project-root "$PROJECT_ROOT" \
    --data-dir "$NILEARN_DATA"

echo "=== Done ==="
echo "Date: $(date -Iseconds)"
