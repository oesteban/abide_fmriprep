#!/usr/bin/env bash
#
# create_site_datasets.sh — initialize per-site DataLad derivative datasets
#
# Creates one DataLad subdataset per site prefix under derivatives/.
# Each site dataset is a valid fMRIPrep BIDS derivatives root (layout 6b).
#
# Usage:
#   code/create_site_datasets.sh --project-root .
#   code/create_site_datasets.sh --project-root . --dry-run
#   code/create_site_datasets.sh --project-root . --create-siblings --gin-org abide-fmriprep

set -euo pipefail

# -------------------------
# Helpers
# -------------------------
die()     { echo -e "\033[31m[FATAL]\033[0m $*" >&2; exit 2; }
info()    { echo -e "\033[36m[INFO]\033[0m $*"; }
success() { echo -e "\033[32m[OK]\033[0m $*"; }
warn()    { echo -e "\033[33m[SKIP]\033[0m $*"; }

# -------------------------
# Args
# -------------------------
PROJECT_ROOT=""
DRY_RUN=0
GIN_ORG="abide-fmriprep"
GITHUB_ORG="abide-fmriprep"
CREATE_SIBLINGS=0
SIBLING_DELAY=2

usage() {
  cat <<EOF
Usage:
  code/create_site_datasets.sh --project-root <path> [options]

Options:
  --project-root <path>   Path to the YODA superdataset root (required)
  --dry-run               Print actions without executing
  --create-siblings       Create GIN + GitHub siblings for each site dataset
  --gin-org <org>         GIN organization name (default: abide-fmriprep)
  --github-org <org>      GitHub organization name (default: abide-fmriprep)
  --sibling-delay <sec>   Delay between API calls (default: 2)
  -h, --help              Show this help

Sibling setup (--create-siblings):
  - gin:    git-annex data + git history (HTTPS fetch, SSH push)
  - github: git history only (HTTPS fetch, SSH push; publish-depends gin)
  - .gitmodules URL updated to GitHub HTTPS for portability
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project-root) PROJECT_ROOT="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    --create-siblings) CREATE_SIBLINGS=1; shift ;;
    --gin-org) GIN_ORG="$2"; shift 2 ;;
    --github-org) GITHUB_ORG="$2"; shift 2 ;;
    --sibling-delay) SIBLING_DELAY="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) die "Unknown arg: $1" ;;
  esac
done

[[ -n "$PROJECT_ROOT" ]] || die "--project-root is required"
PROJECT_ROOT="$(cd "$PROJECT_ROOT" && pwd)"

PARTICIPANTS_TSV="$PROJECT_ROOT/inputs/abide-both/participants.tsv"
[[ -f "$PARTICIPANTS_TSV" ]] || die "participants.tsv not found: $PARTICIPANTS_TSV (run build_abide_both.py)"

DERIV_DIR="$PROJECT_ROOT/derivatives"
[[ -d "$DERIV_DIR" ]] || mkdir -p "$DERIV_DIR"

# -------------------------
# Discover site prefixes from participants.tsv
# -------------------------
# participants.tsv columns: participant_id, source_dataset, source_site, site_index, ...
# participant_id format: sub-v1s0x0050642 → site prefix v1s0
# Build a lookup file: site_prefix \t source_dataset \t source_site (one row per unique prefix)
SITE_LOOKUP="$(mktemp)"
trap 'rm -f "$SITE_LOOKUP"' EXIT

awk -F'\t' 'NR>1 {
  pid=$1; ds=$2; site=$3
  sub(/^sub-/, "", pid)
  sub(/x.*/, "", pid)
  if (!(pid in seen)) { seen[pid]=1; print pid "\t" ds "\t" site }
}' "$PARTICIPANTS_TSV" | sort > "$SITE_LOOKUP"

NUM_SITES="$(wc -l < "$SITE_LOOKUP" | tr -d ' ')"
info "Found $NUM_SITES site prefixes"

if [[ $DRY_RUN -eq 1 ]]; then
  info "DRY RUN — no changes will be made"
  while IFS=$'\t' read -r site ds name; do
    info "  $site ($ds / $name)"
  done < "$SITE_LOOKUP"
  exit 0
fi

# -------------------------
# fMRIPrep dataset_description.json
# -------------------------
DATASET_DESC='{
    "Name": "fMRIPrep - fMRI PREProcessing workflow",
    "BIDSVersion": "1.10.0",
    "DatasetType": "derivative",
    "GeneratedBy": [
        {
            "Name": "fMRIPrep",
            "Version": "25.2.4",
            "CodeURL": "https://github.com/nipreps/fmriprep/archive/25.2.4.tar.gz"
        }
    ],
    "HowToAcknowledge": "Please cite https://doi.org/10.1038/s41592-018-0235-4"
}'

# .bidsignore (same as the old derivatives dataset)
BIDSIGNORE='*.html
logs/
figures/
*_xfm.*
*from-*_to-*
*space-fsLR*
*space-fsnative*
*space-fsaverage*
sourcedata/'

# -------------------------
# Create site datasets
# -------------------------
created=0
skipped=0

while IFS=$'\t' read -r site ds name; do
  site_dir="$DERIV_DIR/$site"

  if [[ -f "$site_dir/.datalad/config" ]]; then
    warn "$site ($ds / $name) — already exists"
    (( skipped++ )) || true
  else
    info "Creating site dataset: $site ($ds / $name)"

    # Create as subdataset of the superdataset
    datalad create -d "$PROJECT_ROOT" "$site_dir"

    # Apply cfg_fmriprep procedure (.gitattributes for metadata in git, imaging in annex)
    datalad run-procedure -d "$site_dir" cfg_fmriprep

    # Write dataset_description.json
    echo "$DATASET_DESC" > "$site_dir/dataset_description.json"

    # Write .bidsignore
    echo "$BIDSIGNORE" > "$site_dir/.bidsignore"

    # Create sourcedata/freesurfer placeholder
    mkdir -p "$site_dir/sourcedata/freesurfer"
    touch "$site_dir/sourcedata/freesurfer/.gitkeep"

    # Save site dataset
    datalad save -d "$site_dir" \
      -m "Initialize site dataset for $site ($ds / $name)"

    success "$site"
    (( created++ )) || true
  fi

  # Create siblings if requested (runs for both new and existing datasets)
  if [[ $CREATE_SIBLINGS -eq 1 ]]; then
    # GIN sibling: git + annex data (HTTPS fetch, SSH push)
    info "  Creating GIN sibling: ${GIN_ORG}/$site"
    datalad create-sibling-gin -d "$site_dir" \
      --name gin --access-protocol https-ssh \
      --existing skip --credential gin \
      "${GIN_ORG}/${site}"
    sleep "$SIBLING_DELAY"

    # GitHub sibling: git only (HTTPS fetch, SSH push; publish-depends gin)
    info "  Creating GitHub sibling: ${GITHUB_ORG}/$site"
    datalad create-sibling-github -d "$site_dir" \
      --name github --access-protocol https-ssh \
      --existing skip --credential github \
      --publish-depends gin \
      "${GITHUB_ORG}/${site}"
    sleep "$SIBLING_DELAY"

    # Initial push to GIN (git + annex), then GitHub (git only)
    info "  Pushing initial content to GIN"
    datalad push -d "$site_dir" --to gin --data anything
    info "  Pushing initial content to GitHub"
    datalad push -d "$site_dir" --to github

    # Update .gitmodules URL to GitHub HTTPS for portability
    # (local clones resolve subdatasets from local filesystem first anyway)
    _submod_key="submodule.derivatives/${site}.url"
    _github_url="https://github.com/${GITHUB_ORG}/${site}.git"
    git -C "$PROJECT_ROOT" config -f .gitmodules "$_submod_key" "$_github_url"
    info "  .gitmodules URL → $_github_url"
  fi
done < "$SITE_LOOKUP"

# -------------------------
# Save superdataset (registers all new subdatasets in .gitmodules)
# -------------------------
if [[ $created -gt 0 ]]; then
  info "Saving superdataset (registering $created new site subdatasets)..."
  datalad save -d "$PROJECT_ROOT" \
    -m "Register $created site-level derivative datasets"
  success "Superdataset saved"
fi

# -------------------------
# Summary
# -------------------------
echo ""
info "===== Summary ====="
success "Created: $created"
warn "Skipped (already exist): $skipped"
info "Total site prefixes: $NUM_SITES"

if [[ $CREATE_SIBLINGS -eq 0 && $created -gt 0 ]]; then
  info ""
  info "Siblings were NOT created. To create them later:"
  info "  code/create_site_datasets.sh --project-root . --create-siblings"
fi
