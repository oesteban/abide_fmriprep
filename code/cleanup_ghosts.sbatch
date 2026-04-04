#!/usr/bin/env bash
#SBATCH --job-name=cleanup-ghosts
#SBATCH --output=logs/%x_%j.out
#SBATCH --error=logs/%x_%j.err
#SBATCH --cpus-per-task=2
#SBATCH --mem=4G
#SBATCH --time=00:30:00

#
# cleanup_ghosts.sbatch — remove ghost subject entries from site datasets
#
# Ghost subjects have git metadata on master (directories, HTML reports,
# FreeSurfer dirs) but lost annex content (GIN push failed, tmpdir purged).
# This script removes those entries so subjects can be reprocessed.
#
# Usage:
#   sbatch code/cleanup_ghosts.sbatch \
#     --project-root /scratch/oesteban/abide_preproc \
#     --subjects-file lists/reprocess_4.txt
#
# IMPORTANT: This script operates on the PERSISTENT superdataset (not a
# SLURM_TMPDIR clone). It mutates master in site subdatasets and pushes
# to GIN + GitHub.

set -euo pipefail

# -------------------------
# Early environment setup
# -------------------------
source "$(dirname "$0")/cluster_config.sh"

# -------------------------
# Helpers
# -------------------------
die()     { echo "[FATAL] $*" >&2; exit 2; }
info()    { echo "[INFO] $*"; }
success() { echo "[OK] $*"; }
warn()    { echo "[WARN] $*" >&2; }

extract_site_prefix() {
  # sub-v1s0x0050642 → v1s0, or v1s0x0050642 → v1s0
  local id="${1#sub-}"
  echo "${id%%x*}"
}

# -------------------------
# Args
# -------------------------
PROJECT_ROOT=""
SUBJECTS_FILE=""
GIN_ORG="abide-fmriprep"

usage() {
  cat <<EOF
Usage:
  sbatch code/cleanup_ghosts.sbatch --project-root <path> --subjects-file <path>
                                    [--gin-org <org>]

Options:
  --project-root <path>     Superdataset root (required)
  --subjects-file <path>    File with one subject ID per line (required)
  --gin-org <org>           GIN organization (default: abide-fmriprep)
  -h, --help                Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project-root) PROJECT_ROOT="$2"; shift 2 ;;
    --subjects-file) SUBJECTS_FILE="$2"; shift 2 ;;
    --gin-org) GIN_ORG="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) die "Unknown arg: $1" ;;
  esac
done

[[ -n "$PROJECT_ROOT" ]] || die "Missing --project-root"
[[ -n "$SUBJECTS_FILE" ]] || die "Missing --subjects-file"
[[ -d "$PROJECT_ROOT" ]] || die "Project root not found: $PROJECT_ROOT"
[[ -f "$SUBJECTS_FILE" ]] || die "Subjects file not found: $SUBJECTS_FILE"

cd "$PROJECT_ROOT"

# -------------------------
# Group subjects by site
# -------------------------
declare -A SITE_SUBJECTS

while IFS= read -r line; do
  line="$(echo "$line" | tr -d '\r' | xargs)"
  [[ -z "$line" || "$line" == \#* ]] && continue
  sub="${line#sub-}"
  site="$(extract_site_prefix "$sub")"
  SITE_SUBJECTS["$site"]+="sub-${sub} "
done < "$SUBJECTS_FILE"

info "Subjects grouped into ${#SITE_SUBJECTS[@]} site(s):"
for site in "${!SITE_SUBJECTS[@]}"; do
  info "  $site: ${SITE_SUBJECTS[$site]}"
done

# -------------------------
# Clean ghost entries per site
# -------------------------
for site in "${!SITE_SUBJECTS[@]}"; do
  info "--- Processing site: $site ---"
  site_dir="derivatives/$site"

  # Install subdataset metadata (lightweight, no content)
  if [[ ! -d "$site_dir/.git" && ! -f "$site_dir/.git" ]]; then
    info "Installing subdataset: $site_dir"
    datalad get -n "$site_dir"
  fi

  # Add GIN remote (idempotent)
  gin_url="git@gin.g-node.org:/${GIN_ORG}/${site}.git"
  git -C "$site_dir" remote add gin "$gin_url" 2>/dev/null || true
  info "GIN remote: $gin_url"

  # Fetch + fast-forward merge from GIN to ensure we have the latest master
  info "Fetching from GIN..."
  git -C "$site_dir" fetch gin master
  git -C "$site_dir" fetch gin git-annex 2>/dev/null || true
  git -C "$site_dir" annex merge 2>/dev/null || true
  git -C "$site_dir" merge --ff-only gin/master || {
    warn "Fast-forward merge failed for $site — master may already be up to date"
  }

  # Remove ghost entries
  removed=0
  read -ra subs <<< "${SITE_SUBJECTS[$site]}"
  for sub in "${subs[@]}"; do
    [[ -z "$sub" ]] && continue
    info "Removing ghost: $sub"

    # Subject directory
    git -C "$site_dir" rm -r "$sub" 2>/dev/null || warn "  $sub/ not found in git tree"
    # HTML report
    git -C "$site_dir" rm -r "${sub}.html" 2>/dev/null || warn "  ${sub}.html not found"
    # FreeSurfer outputs (ses-1 is standard for ABIDE)
    git -C "$site_dir" rm -r "sourcedata/freesurfer/${sub}_ses-1" 2>/dev/null \
      || warn "  sourcedata/freesurfer/${sub}_ses-1 not found"
    # Also try ses-2 (46 ABIDE II subjects have it)
    git -C "$site_dir" rm -r "sourcedata/freesurfer/${sub}_ses-2" 2>/dev/null || true

    removed=$((removed + 1))
  done

  if [[ "$removed" -gt 0 ]]; then
    # Check if there are staged changes before committing
    if git -C "$site_dir" diff --cached --quiet 2>/dev/null; then
      warn "No changes staged for $site — ghosts may already be cleaned"
    else
      git -C "$site_dir" commit -m "fix: remove $removed ghost subject(s) (annex content lost, needs reprocessing)"
      success "Committed ghost removal for $site ($removed subjects)"

      # Push to GIN (git metadata only — annex content is already lost)
      info "Pushing $site to GIN..."
      datalad push -d "$site_dir" --to gin --data nothing
      success "Pushed $site to GIN"

      # Push to GitHub (best-effort — HTTPS origin may lack credentials on compute nodes)
      info "Pushing $site to origin (GitHub)..."
      if datalad push -d "$site_dir" --to origin 2>&1; then
        success "Pushed $site to origin"
      else
        warn "Push to origin failed for $site (HTTPS credentials?). GIN push succeeded — GitHub can be synced later."
      fi
    fi
  fi
done

# -------------------------
# Clean overlay (if initialized)
# -------------------------
OVERLAY="derivatives/fmriprep-25.2"

info "--- Processing overlay: $OVERLAY ---"
if [[ ! -d "$OVERLAY/.git" && ! -f "$OVERLAY/.git" ]]; then
  info "Installing overlay subdataset..."
  datalad get -n "$OVERLAY"
fi

overlay_changed=0
for site in "${!SITE_SUBJECTS[@]}"; do
  read -ra subs <<< "${SITE_SUBJECTS[$site]}"
  for sub in "${subs[@]}"; do
    [[ -z "$sub" ]] && continue
    git -C "$OVERLAY" rm -r "$sub" 2>/dev/null && overlay_changed=1 || true
    git -C "$OVERLAY" rm -r "${sub}.html" 2>/dev/null || true
  done
done

if [[ "$overlay_changed" -gt 0 ]] && ! git -C "$OVERLAY" diff --cached --quiet 2>/dev/null; then
  git -C "$OVERLAY" commit -m "fix: remove ghost subjects (will be re-registered after reprocessing)"
  info "Pushing overlay to origin..."
  if datalad push -d "$OVERLAY" --to origin 2>&1; then
    success "Overlay cleaned and pushed"
  else
    warn "Push overlay to origin failed (HTTPS credentials?). Overlay commit saved locally."
  fi
else
  info "No ghost entries found in overlay — skipping"
fi

# -------------------------
# Save superdataset state
# -------------------------
info "Saving superdataset state..."
datalad save -d . -m "fix: remove ghost subjects from site datasets and overlay"
success "Superdataset saved"

info "=== Ghost cleanup complete ==="
info "Next: submit fMRIPrep reprocessing for these subjects"
