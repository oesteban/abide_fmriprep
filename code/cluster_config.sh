# cluster_config.sh — central cluster detection and environment configuration
#
# Sourced by SLURM scripts to set cluster-specific paths. Detects the
# current cluster from SLURM_CLUSTER_NAME or hostname FQDN and exports
# environment variables accordingly.
#
# To adapt for a new cluster:
#   1. Add a case entry below with the cluster name
#   2. Set DATALAD_ENV, ANALYSIS_ENV, NILEARN_DATA, and MODULES
#
# To override without editing this file, export the variables before
# sourcing (they are only set if not already defined).

detect_cluster() {
  if [[ -n "${SLURM_CLUSTER_NAME:-}" ]]; then
    echo "$SLURM_CLUSTER_NAME"
    return
  fi
  local fqdn
  fqdn="$(hostname -f 2>/dev/null || hostname)"
  case "$fqdn" in
    *.dcsr.unil.ch|curnagl|dna*) echo "curnagl" ;;
    calypso*)                     echo "calypso" ;;
    *)                            echo "unknown" ;;
  esac
}

ABIDE_CLUSTER="${ABIDE_CLUSTER:-$(detect_cluster)}"

case "$ABIDE_CLUSTER" in
  curnagl)
    : "${DATALAD_ENV:=/work/FAC/FBM/DNF/oesteban/hcph/opt/mamba/envs/fmriprep/bin}"
    : "${ANALYSIS_ENV:=/work/FAC/FBM/DNF/oesteban/hcph/opt/mamba/envs/abide-analysis/bin}"
    : "${NILEARN_DATA:=/scratch/oesteban/nilearn_data}"
    MODULES="gcc/12.3.0 singularityce/4.1.0"
    ;;
  calypso)
    : "${DATALAD_ENV:=}"
    : "${ANALYSIS_ENV:=}"
    : "${NILEARN_DATA:=${HOME}/.cache/nilearn}"
    MODULES="singularity"
    ;;
  *)
    : "${DATALAD_ENV:=}"
    : "${ANALYSIS_ENV:=}"
    : "${NILEARN_DATA:=${HOME}/.cache/nilearn}"
    MODULES=""
    ;;
esac

# Prepend DataLad environment to PATH (if set)
if [[ -n "$DATALAD_ENV" ]]; then
  export PATH="${DATALAD_ENV}:$PATH"
fi

# Load HPC modules (if any)
if [[ -n "$MODULES" ]]; then
  module load $MODULES 2>/dev/null || true
fi

export NILEARN_DATA
