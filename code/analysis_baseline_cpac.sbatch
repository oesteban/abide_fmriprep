#!/bin/bash
#SBATCH --job-name=abide-cpac-baseline
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=08:00:00
#SBATCH --output=logs/cpac_baseline_%j.out
#SBATCH --error=logs/cpac_baseline_%j.err

set -euo pipefail

# -------------------------------------------------------------------
# Baseline: download C-PAC preprocessed ABIDE I from PCP, extract
# MSDL time series, run tangent + Ridge/SVC classification.
# -------------------------------------------------------------------

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

# Activate analysis environment (Curnagl: PATH-based)
export PATH=/work/FAC/FBM/DNF/oesteban/hcph/opt/mamba/envs/abide-analysis/bin:$PATH

# Use scratch for PCP download cache (large files, ~50 GB)
DATA_DIR="/scratch/oesteban/nilearn_data"
mkdir -p "$DATA_DIR"

echo "=== C-PAC Baseline ==="
echo "Project root: $PROJECT_ROOT"
echo "Data dir: $DATA_DIR"
echo "Host: $(hostname)"
echo "Date: $(date -Iseconds)"

python3 "$PROJECT_ROOT/code/analysis/06_baseline_cpac.py" \
    --project-root "$PROJECT_ROOT" \
    --data-dir "$DATA_DIR"

echo "=== Done ==="
echo "Date: $(date -Iseconds)"
