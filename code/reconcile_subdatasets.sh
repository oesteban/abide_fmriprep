#!/usr/bin/env bash
#
# reconcile_subdatasets.sh — register new per-subject GIN repos as subdatasets
#
# After a batch of SLURM jobs completes, this script discovers new GIN repos
# in the abide-fmriprep organization, installs them as DataLad subdatasets in
# the derivatives parent, updates participants.tsv, and pushes the parent.
#
# Replaces the old merge_job_branches.sh workflow (branch-per-job merging).
#
# Usage:
#   code/reconcile_subdatasets.sh [--dry-run] [-C <derivatives-path>]
#                                 [--logs-dir <path>] [--gin-org <org>]
#                                 [--push]

set -euo pipefail

# -------------------------
# Helpers
# -------------------------
die() { echo -e "\033[31m[FATAL]\033[0m $*" >&2; exit 2; }

info()    { echo -e "\033[36m[INFO]\033[0m $*"; }
success() { echo -e "\033[32m[OK]\033[0m $*"; }
warn()    { echo -e "\033[33m[SKIP]\033[0m $*"; }
fail()    { echo -e "\033[31m[FAIL]\033[0m $*"; }

# -------------------------
# Args
# -------------------------
DRY_RUN=0
DERIV_PATH=""
LOGS_DIR=""
GIN_ORG="abide-fmriprep"
DO_PUSH=0

usage() {
  cat <<EOF
Usage:
  reconcile_subdatasets.sh [--dry-run] [-C <derivatives-path>]
                           [--logs-dir <path>] [--gin-org <org>]
                           [--push]

Options:
  --dry-run         List repos and actions without making changes
  -C <path>         Path to the derivatives subdataset (default: current directory)
  --logs-dir <path> Path to the SLURM logs directory (default: auto-detect)
  --gin-org <org>   GIN organization name (default: abide-fmriprep)
  --push            Push parent datasets to GIN and GitHub after reconciliation
  -h, --help        Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    -C) DERIV_PATH="$2"; shift 2 ;;
    --logs-dir) LOGS_DIR="$2"; shift 2 ;;
    --gin-org) GIN_ORG="$2"; shift 2 ;;
    --push) DO_PUSH=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "Unknown arg: $1" ;;
  esac
done

if [[ -n "$DERIV_PATH" ]]; then
  cd "$DERIV_PATH"
fi

# Verify we are inside a git repo and on master
[[ -d .git || -f .git ]] || die "Not a git repository: $(pwd)"
CURRENT_BRANCH="$(git symbolic-ref --short HEAD 2>/dev/null)" \
  || die "Cannot determine current branch (detached HEAD?)"
[[ "$CURRENT_BRANCH" == "master" ]] || die "Must be on master (currently on $CURRENT_BRANCH)"

DERIV_ABS="$(pwd)"
FS_REL="sourcedata/freesurfer"
FS_ABS="$DERIV_ABS/$FS_REL"

# Auto-detect the SLURM logs directory from the YODA superdataset
if [[ -z "$LOGS_DIR" ]]; then
  _candidate="$(cd .. && git rev-parse --show-toplevel 2>/dev/null)" || true
  if [[ -n "$_candidate" && -d "${_candidate}/logs" ]]; then
    LOGS_DIR="${_candidate}/logs"
  fi
fi

if [[ -n "$LOGS_DIR" && -d "$LOGS_DIR" ]]; then
  info "SLURM logs directory: $LOGS_DIR"
else
  warn "SLURM logs directory not found"
  LOGS_DIR=""
fi

# -------------------------
# Phase 1 — Discover GIN repos in the organization
# -------------------------
info "Discovering repos in GIN organization '$GIN_ORG'..."

# List all repos via SSH (git ls-remote on the org, or use the GIN API).
# GIN runs Gogs, so we can use its API: GET /api/v1/orgs/:org/repos
GIN_API="https://gin.g-node.org/api/v1"

# Fetch all repo names (paginated)
ALL_GIN_REPOS=()
page=1
while true; do
  response="$(curl -s "${GIN_API}/orgs/${GIN_ORG}/repos?page=${page}&limit=50" 2>/dev/null)" || break
  # Extract repo names (simple JSON parsing with grep/sed, no jq dependency)
  page_repos=()
  while IFS= read -r name; do
    page_repos+=("$name")
  done < <(echo "$response" | grep -o '"name":"[^"]*"' | sed 's/"name":"//;s/"//')

  if [[ ${#page_repos[@]} -eq 0 ]]; then
    break
  fi
  ALL_GIN_REPOS+=("${page_repos[@]}")
  (( page++ ))
done

info "Found ${#ALL_GIN_REPOS[@]} repos in '$GIN_ORG'."

# Separate fmriprep and freesurfer repos
FMRIPREP_REPOS=()
FREESURFER_REPOS=()
for repo in "${ALL_GIN_REPOS[@]}"; do
  case "$repo" in
    fmriprep-*) FMRIPREP_REPOS+=("$repo") ;;
    freesurfer-*) FREESURFER_REPOS+=("$repo") ;;
  esac
done

info "fMRIPrep repos: ${#FMRIPREP_REPOS[@]}, FreeSurfer repos: ${#FREESURFER_REPOS[@]}"

# -------------------------
# Phase 2 — Identify repos not yet registered as subdatasets
# -------------------------

# Get existing fMRIPrep subdatasets from .gitmodules
existing_fmriprep=()
while IFS= read -r submod; do
  [[ -n "$submod" ]] && existing_fmriprep+=("$submod")
done < <(git config -f .gitmodules --get-regexp 'submodule\.sub-.*\.path' 2>/dev/null \
  | awk '{print $2}' | grep -v '/' || true)

# Get existing FreeSurfer subdatasets
existing_freesurfer=()
if [[ -d "$FS_ABS" && -f "$FS_ABS/.gitmodules" ]]; then
  while IFS= read -r submod; do
    [[ -n "$submod" ]] && existing_freesurfer+=("$(basename "$submod")")
  done < <(git -C "$FS_ABS" config -f .gitmodules --get-regexp 'submodule\..*\.path' 2>/dev/null \
    | awk '{print $2}' || true)
fi

info "Existing subdatasets — fMRIPrep: ${#existing_fmriprep[@]}, FreeSurfer: ${#existing_freesurfer[@]}"

# Find new fMRIPrep repos
new_fmriprep=()
for repo in "${FMRIPREP_REPOS[@]}"; do
  subject_short="${repo#fmriprep-}"
  subject_id="sub-${subject_short}"
  # Check if already registered
  found=0
  for existing in "${existing_fmriprep[@]}"; do
    if [[ "$existing" == "$subject_id" ]]; then
      found=1
      break
    fi
  done
  if [[ $found -eq 0 ]]; then
    new_fmriprep+=("$repo")
  fi
done

# Find new FreeSurfer repos
new_freesurfer=()
for repo in "${FREESURFER_REPOS[@]}"; do
  subject_ses="${repo#freesurfer-}"
  fs_dir_name="sub-${subject_ses}"
  found=0
  for existing in "${existing_freesurfer[@]}"; do
    if [[ "$existing" == "$fs_dir_name" ]]; then
      found=1
      break
    fi
  done
  if [[ $found -eq 0 ]]; then
    new_freesurfer+=("$repo")
  fi
done

info "New repos to register — fMRIPrep: ${#new_fmriprep[@]}, FreeSurfer: ${#new_freesurfer[@]}"

if [[ ${#new_fmriprep[@]} -eq 0 && ${#new_freesurfer[@]} -eq 0 ]]; then
  info "Nothing to reconcile — all GIN repos are already registered."
  exit 0
fi

if [[ $DRY_RUN -eq 1 ]]; then
  info "DRY RUN — would register:"
  for repo in "${new_fmriprep[@]}"; do
    subject_short="${repo#fmriprep-}"
    info "  fMRIPrep: sub-${subject_short} <- ${GIN_ORG}/${repo}"
  done
  for repo in "${new_freesurfer[@]}"; do
    subject_ses="${repo#freesurfer-}"
    info "  FreeSurfer: sub-${subject_ses} <- ${GIN_ORG}/${repo}"
  done
  exit 0
fi

# -------------------------
# Phase 3 — Install new fMRIPrep subdatasets
# -------------------------
installed_fmriprep=0
for repo in "${new_fmriprep[@]}"; do
  subject_short="${repo#fmriprep-}"
  subject_id="sub-${subject_short}"
  clone_url="git@gin.g-node.org:/${GIN_ORG}/${repo}.git"

  info "Installing fMRIPrep subdataset: $subject_id"
  if datalad clone -d . "$clone_url" "$subject_id" 2>/dev/null; then
    success "Installed $subject_id"
    (( installed_fmriprep++ )) || true
  else
    fail "Failed to install $subject_id from $clone_url"
  fi
done

# -------------------------
# Phase 4 — Install new FreeSurfer subdatasets
# -------------------------
installed_freesurfer=0
for repo in "${new_freesurfer[@]}"; do
  subject_ses="${repo#freesurfer-}"
  fs_dir_name="sub-${subject_ses}"
  clone_url="git@gin.g-node.org:/${GIN_ORG}/${repo}.git"

  info "Installing FreeSurfer subdataset: $fs_dir_name"
  if datalad clone -d "$FS_REL" "$clone_url" "${FS_REL}/${fs_dir_name}" 2>/dev/null; then
    success "Installed $fs_dir_name"
    (( installed_freesurfer++ )) || true
  else
    fail "Failed to install $fs_dir_name from $clone_url"
  fi
done

# -------------------------
# Phase 5 — Update participants.tsv
# -------------------------
# Convert fmriprep.toml dir name (YYYYMMDD-HHMMSS) to ISO 8601
toml_dir_to_iso() {
  echo "$1" | sed 's/\([0-9]\{4\}\)\([0-9]\{2\}\)\([0-9]\{2\}\)-\([0-9]\{2\}\)\([0-9]\{2\}\)\([0-9]\{2\}\)/\1-\2-\3T\4:\5:\6/'
}

# Convert nipype timestamp (YYMMDD-HH:MM:SS) to ISO 8601
nipype_ts_to_iso() {
  echo "$1" | sed 's/^\([0-9]\{2\}\)\([0-9]\{2\}\)\([0-9]\{2\}\)-/20\1-\2-\3T/'
}

TSV="participants.tsv"
META_TMPFILE="$(mktemp)"
trap 'rm -f "$META_TMPFILE"' EXIT

# Pre-index SLURM logs
LOG_INDEX=""
if [[ -n "$LOGS_DIR" ]]; then
  LOG_INDEX="$(mktemp)"
  trap 'rm -f "$META_TMPFILE" "$LOG_INDEX"' EXIT

  info "Indexing successful SLURM logs..."
  while IFS= read -r logfile; do
    sub="$(sed -n 's/.*SUBJECT=\(sub-[^ ]*\).*/\1/p' "$logfile" | head -1)"
    if [[ -n "$sub" ]]; then
      printf '%s\t%s\n' "$sub" "$logfile" >> "$LOG_INDEX"
    fi
  done < <(grep -l 'fMRIPrep finished successfully' "$LOGS_DIR"/fmriprep_*.out 2>/dev/null)
fi

# Extract metadata for each newly installed fMRIPrep subject
for repo in "${new_fmriprep[@]}"; do
  subject_short="${repo#fmriprep-}"
  subject_id="sub-${subject_short}"

  if [[ ! -d "$subject_id" ]]; then
    continue
  fi

  # stc_ref_time from logs/CITATION.md (parent-level, generated by fMRIPrep)
  stc_val=""
  if [[ -f "logs/CITATION.md" ]]; then
    stc_val="$(sed -n 's/.*slice-time corrected to \([0-9.]*\)s.*/\1/p' logs/CITATION.md 2>/dev/null | head -1)" || true
  fi

  # Timing from SLURM logs
  start_iso=""
  stop_iso=""
  if [[ -n "$LOG_INDEX" ]]; then
    logfile="$(awk -F'\t' -v sid="$subject_id" '$1 == sid { print $2; exit }' "$LOG_INDEX")"
    if [[ -n "$logfile" && -f "$logfile" ]]; then
      start_raw="$(grep -B1 'Running fMRIPrep version' "$logfile" \
        | sed -n 's/^\([0-9]\{6\}-[0-9:]*\).*/\1/p' | head -1)" || true
      if [[ -n "$start_raw" ]]; then
        start_iso="$(nipype_ts_to_iso "$start_raw")"
      fi
      stop_raw="$(grep -B1 'fMRIPrep finished successfully' "$logfile" \
        | sed -n 's/^\([0-9]\{6\}-[0-9:]*\).*/\1/p' | head -1)" || true
      if [[ -n "$stop_raw" ]]; then
        stop_iso="$(nipype_ts_to_iso "$stop_raw")"
      fi
    fi
  fi

  # Fallback for start: fmriprep.toml log directory
  if [[ -z "$start_iso" ]]; then
    start_raw="$(find "$subject_id" -path '*/log/*/fmriprep.toml' 2>/dev/null \
      | sed -n 's|.*/log/\([0-9]\{8\}-[0-9]\{6\}\)_.*/fmriprep\.toml|\1|p' \
      | head -1)" || true
    if [[ -n "$start_raw" ]]; then
      start_iso="$(toml_dir_to_iso "$start_raw")"
    fi
  fi

  printf '%s\t%s\t%s\t%s\n' \
    "$subject_id" "${stc_val:-n/a}" "${start_iso:-n/a}" "${stop_iso:-n/a}" \
    >> "$META_TMPFILE"
done

# Build final participants.tsv
if [[ -s "$META_TMPFILE" ]]; then
  {
    if [[ -f "$TSV" ]]; then
      tail -n +2 "$TSV"
    fi
    cat "$META_TMPFILE"
  } | awk -F'\t' '{
    pid=$1; stc=$2; start=$3; stop=$4
    data_stc[pid] = stc
    data_start[pid] = start
    data_stop[pid] = stop
  } END {
    for (k in data_stc)
      print k "\t" data_stc[k] "\t" data_start[k] "\t" data_stop[k]
  }' | sort -t$'\t' -k1,1 > "${TSV}.tmp"

  {
    printf 'participant_id\tstc_ref_time\tfmriprep_start\tfmriprep_stop\n'
    cat "${TSV}.tmp"
  } > "$TSV"
  rm -f "${TSV}.tmp"
fi

# -------------------------
# Phase 6 — Write participants.json sidecar and save
# -------------------------
TSV_JSON="participants.json"
cat > "$TSV_JSON" <<'JSONEOF'
{
  "participant_id": {
    "Description": "Participant identifier"
  },
  "stc_ref_time": {
    "Description": "Reference time used for slice-timing correction",
    "Units": "s"
  },
  "fmriprep_start": {
    "Description": "Date and time when fMRIPrep began processing this participant (ISO 8601, local time on the HPC cluster)"
  },
  "fmriprep_stop": {
    "Description": "Date and time when fMRIPrep finished processing this participant (ISO 8601, local time on the HPC cluster)"
  }
}
JSONEOF

# Save freesurfer intermediate
if [[ $installed_freesurfer -gt 0 ]]; then
  datalad save -d "$FS_REL" -m "Register $installed_freesurfer new FreeSurfer subdataset(s)"
fi

# Save derivatives parent
datalad save -d . -m "Reconcile: register $installed_fmriprep fMRIPrep + $installed_freesurfer FreeSurfer subdataset(s)"

tsv_count=0
if [[ -f "$TSV" ]]; then
  tsv_count=$(( $(wc -l < "$TSV") - 1 ))
fi
info "participants.tsv: $tsv_count subject(s)."

# -------------------------
# Phase 7 — Push parent datasets (optional)
# -------------------------
if [[ $DO_PUSH -eq 1 ]]; then
  info "Pushing to GIN..."
  if datalad push --to gin; then
    success "GIN synced."
  else
    warn "datalad push --to gin failed."
  fi

  info "Pushing to origin (GitHub)..."
  if datalad push --to origin; then
    success "GitHub synced."
  else
    warn "datalad push --to origin failed — sync manually."
  fi
fi

# -------------------------
# Summary
# -------------------------
echo ""
info "===== Summary ====="
success "fMRIPrep subdatasets installed: $installed_fmriprep"
success "FreeSurfer subdatasets installed: $installed_freesurfer"
info "participants.tsv: $tsv_count subject(s)"
