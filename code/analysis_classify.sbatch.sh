#!/usr/bin/env bash
#SBATCH --job-name=abide-classify
#SBATCH --output=logs/%x_%A.out
#SBATCH --error=logs/%x_%A.err
#SBATCH --cpus-per-task=16
#SBATCH --mem=32G
#SBATCH --time=04:00:00

set -euo pipefail

die() { echo "[FATAL] $*" >&2; exit 2; }

PROJECT_ROOT=""
CONDA_ENV="abide-analysis"
EXPERIMENTS="abide1 both"
CLASSIFIERS="ridge svc_linear"

usage() {
  cat <<EOF
Usage:
  sbatch code/analysis_classify.sbatch.sh \\
    --project-root /path/to/abide_preproc

Optional:
  --conda-env <name>          Conda/micromamba environment (default: abide-analysis)
  --experiments <list>        Space-separated: abide1 abide2 both (default: abide1 both)
  --classifiers <list>        Space-separated: ridge svc_linear (default: ridge svc_linear)
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project-root) PROJECT_ROOT="$2"; shift 2 ;;
    --conda-env) CONDA_ENV="$2"; shift 2 ;;
    --experiments) EXPERIMENTS="$2"; shift 2 ;;
    --classifiers) CLASSIFIERS="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) die "Unknown arg: $1" ;;
  esac
done

[[ -n "$PROJECT_ROOT" ]] || die "--project-root is required"
[[ -d "$PROJECT_ROOT" ]] || die "Project root not found: $PROJECT_ROOT"

echo "[INFO] PROJECT_ROOT=$PROJECT_ROOT"
echo "[INFO] Step 1/2: Building connectomes"
micromamba run -n "$CONDA_ENV" python3 \
  "$PROJECT_ROOT/code/analysis/02_build_connectomes.py" \
  --project-root "$PROJECT_ROOT"

echo "[INFO] Step 2/2: Running classification"
micromamba run -n "$CONDA_ENV" python3 \
  "$PROJECT_ROOT/code/analysis/03_classify.py" \
  --project-root "$PROJECT_ROOT" \
  --experiments $EXPERIMENTS \
  --classifiers $CLASSIFIERS

echo "[INFO] DONE"
