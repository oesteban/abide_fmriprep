#!/bin/bash
#SBATCH --job-name=abide-classify
#SBATCH --cpus-per-task=16
#SBATCH --mem=32G
#SBATCH --time=04:00:00
#SBATCH --output=logs/classify_%j.out
#SBATCH --error=logs/classify_%j.err

set -euo pipefail


PROJECT_ROOT=""
VARIANT="v1"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --project-root) PROJECT_ROOT="$2"; shift 2 ;;
        --variant) VARIANT="$2"; shift 2 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

if [[ -z "$PROJECT_ROOT" ]]; then
    echo "ERROR: --project-root is required"
    exit 1
fi

source "$PROJECT_ROOT/code/analysis_env.sh"

echo "=== Build connectomes (variant $VARIANT) ==="
echo "Project root: $PROJECT_ROOT"
echo "Host: $(hostname)"
echo "Date: $(date -Iseconds)"

python3 "$PROJECT_ROOT/code/analysis/03_build_connectomes.py" \
    --project-root "$PROJECT_ROOT" --variant "$VARIANT"

echo ""
echo "=== Classification ==="
echo "Date: $(date -Iseconds)"

python3 "$PROJECT_ROOT/code/analysis/04_classify.py" \
    --project-root "$PROJECT_ROOT" --variant "$VARIANT"

echo ""
echo "=== Visualization ==="
echo "Date: $(date -Iseconds)"

python3 "$PROJECT_ROOT/code/analysis/05_visualize.py" \
    --project-root "$PROJECT_ROOT" --variant "$VARIANT"

echo "=== All done ==="
echo "Date: $(date -Iseconds)"
