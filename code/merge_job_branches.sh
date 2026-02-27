#!/usr/bin/env bash
#
# merge_job_branches.sh — merge per-subject fMRIPrep job branches into master
#
# Runs inside the derivatives/fmriprep-25.2 subdataset.
# Extracts per-subject metadata (stc_ref_time, fmriprep_start, fmriprep_stop)
# from each branch and the SLURM logs, records it in participants.tsv, and
# merges unmerged branches.
#
# Usage:
#   code/merge_job_branches.sh [--remote <name>] [--dry-run] [-C <derivatives-path>]
#                              [--logs-dir <path>]

set -euo pipefail

# -------------------------
# Helpers
# -------------------------
die() { echo -e "\033[31m[FATAL]\033[0m $*" >&2; exit 2; }

info()    { echo -e "\033[36m[INFO]\033[0m $*"; }
success() { echo -e "\033[32m[OK]\033[0m $*"; }
warn()    { echo -e "\033[33m[SKIP]\033[0m $*"; }
fail()    { echo -e "\033[31m[FAIL]\033[0m $*"; }

# Convert nipype timestamp (YYMMDD-HH:MM:SS) to ISO 8601 (YYYY-MM-DDTHH:MM:SS)
nipype_ts_to_iso() {
  echo "$1" | sed 's/^\([0-9]\{2\}\)\([0-9]\{2\}\)\([0-9]\{2\}\)-/20\1-\2-\3T/'
}

# Convert fmriprep.toml dir name (YYYYMMDD-HHMMSS) to ISO 8601
toml_dir_to_iso() {
  echo "$1" | sed 's/\([0-9]\{4\}\)\([0-9]\{2\}\)\([0-9]\{2\}\)-\([0-9]\{2\}\)\([0-9]\{2\}\)\([0-9]\{2\}\)/\1-\2-\3T\4:\5:\6/'
}

# -------------------------
# Args
# -------------------------
REMOTE="gin"
DRY_RUN=0
DERIV_PATH=""
LOGS_DIR=""

usage() {
  cat <<EOF
Usage:
  merge_job_branches.sh [--remote <name>] [--dry-run] [-C <derivatives-path>]
                        [--logs-dir <path>]

Options:
  --remote <name>   Git remote to scan for job branches (default: gin)
  --dry-run         List branches and their merge status without merging
  -C <path>         Path to the derivatives subdataset (default: current directory)
  --logs-dir <path> Path to the SLURM logs directory (default: auto-detect from
                    YODA superdataset root, i.e. ../../logs relative to derivatives)
  -h, --help        Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --remote) REMOTE="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    -C) DERIV_PATH="$2"; shift 2 ;;
    --logs-dir) LOGS_DIR="$2"; shift 2 ;;
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

# Auto-detect the SLURM logs directory from the YODA superdataset
if [[ -z "$LOGS_DIR" ]]; then
  # Walk up to find the superdataset root (contains .datalad/config at the top)
  _candidate="$(cd .. && git rev-parse --show-toplevel 2>/dev/null)" || true
  if [[ -n "$_candidate" && -d "${_candidate}/logs" ]]; then
    LOGS_DIR="${_candidate}/logs"
  fi
fi

if [[ -n "$LOGS_DIR" && -d "$LOGS_DIR" ]]; then
  info "SLURM logs directory: $LOGS_DIR"
else
  warn "SLURM logs directory not found — fmriprep_start/stop will use fallback sources"
  LOGS_DIR=""
fi

# -------------------------
# Phase 0 — Fetch remote refs
# -------------------------
info "Fetching from remote '$REMOTE'..."
git fetch "$REMOTE"

# Collect all job branches
BRANCHES=()
while IFS= read -r line; do
  BRANCHES+=("$line")
done < <(git branch -r | sed 's/^[[:space:]]*//' | grep "^${REMOTE}/job/")

if [[ ${#BRANCHES[@]} -eq 0 ]]; then
  info "No job branches found on remote '$REMOTE'."
  exit 0
fi

info "Found ${#BRANCHES[@]} job branch(es) on '$REMOTE'."

# -------------------------
# Pre-index SLURM logs: build a mapping of subject → log file
# -------------------------
# Only index logs that contain "fMRIPrep finished successfully" (successful runs).
# Format: subject<TAB>logfile (one per line)
LOG_INDEX=""
if [[ -n "$LOGS_DIR" ]]; then
  LOG_INDEX="$(mktemp)"
  trap 'rm -f "$LOG_INDEX" "$META_TMPFILE"' EXIT

  info "Indexing successful SLURM logs..."
  # First, find logs with a successful completion
  while IFS= read -r logfile; do
    # Extract subject from the first few lines: [INFO] SUBJECT=sub-XXXX
    sub="$(sed -n 's/.*SUBJECT=\(sub-[^ ]*\).*/\1/p' "$logfile" | head -1)"
    if [[ -n "$sub" ]]; then
      printf '%s\t%s\n' "$sub" "$logfile" >> "$LOG_INDEX"
    fi
  done < <(grep -l 'fMRIPrep finished successfully' "$LOGS_DIR"/bootstrap-fmriprep_*.out 2>/dev/null)

  log_index_count="$(wc -l < "$LOG_INDEX" | tr -d ' ')"
  info "Indexed $log_index_count successful log(s)."
fi

# -------------------------
# Phase 1 — Extract metadata from ALL branches
# -------------------------
# Temp file: subject<TAB>stc_ref_time<TAB>fmriprep_start<TAB>fmriprep_stop
META_TMPFILE="$(mktemp)"
if [[ -z "$LOG_INDEX" ]]; then
  trap 'rm -f "$META_TMPFILE"' EXIT
fi

MERGED=()
UNMERGED=()

for ref in "${BRANCHES[@]}"; do
  # Extract subject ID from branch path: .../sub-XXXX/...
  subject=""
  IFS='/' read -ra parts <<< "$ref"
  for part in "${parts[@]}"; do
    if [[ "$part" == sub-* ]]; then
      subject="$part"
      break
    fi
  done
  [[ -n "$subject" ]] || { warn "Cannot extract subject from $ref — skipping"; continue; }

  # Check merge status
  if git merge-base --is-ancestor "$ref" master 2>/dev/null; then
    MERGED+=("$ref")
    status_label="merged"
  else
    UNMERGED+=("$ref")
    status_label="pending"
  fi

  # --- stc_ref_time: from logs/CITATION.md ---
  stc_val=""
  stc_val="$(git show "${ref}:logs/CITATION.md" 2>/dev/null \
    | sed -n 's/.*slice-time corrected to \([0-9.]*\)s.*/\1/p')" || true

  # --- Timing: prefer SLURM logs, fall back to branch artifacts ---
  start_iso=""
  stop_iso=""

  # Try SLURM log first (source of truth)
  if [[ -n "$LOG_INDEX" ]]; then
    logfile="$(awk -F'\t' -v sid="$subject" '$1 == sid { print $2; exit }' "$LOG_INDEX")"
    if [[ -n "$logfile" && -f "$logfile" ]]; then
      # Start: nipype timestamp on the line before "Running fMRIPrep version"
      start_raw="$(grep -B1 'Running fMRIPrep version' "$logfile" \
        | sed -n 's/^\([0-9]\{6\}-[0-9:]*\).*/\1/p' | head -1)" || true
      if [[ -n "$start_raw" ]]; then
        start_iso="$(nipype_ts_to_iso "$start_raw")"
      fi
      # Stop: nipype timestamp on the line before "fMRIPrep finished successfully"
      stop_raw="$(grep -B1 'fMRIPrep finished successfully' "$logfile" \
        | sed -n 's/^\([0-9]\{6\}-[0-9:]*\).*/\1/p' | head -1)" || true
      if [[ -n "$stop_raw" ]]; then
        stop_iso="$(nipype_ts_to_iso "$stop_raw")"
      fi
    fi
  fi

  # Fallback for start: fmriprep.toml log directory name on the branch
  if [[ -z "$start_iso" ]]; then
    start_raw=""
    start_raw="$(git ls-tree -r --name-only "$ref" -- "${subject}/" 2>/dev/null \
      | sed -n 's|.*/log/\([0-9]\{8\}-[0-9]\{6\}\)_.*/fmriprep\.toml|\1|p' \
      | head -1)" || true
    if [[ -n "$start_raw" ]]; then
      start_iso="$(toml_dir_to_iso "$start_raw")"
    fi
  fi

  # Record all fields (n/a for missing values)
  printf '%s\t%s\t%s\t%s\n' \
    "$subject" "${stc_val:-n/a}" "${start_iso:-n/a}" "${stop_iso:-n/a}" \
    >> "$META_TMPFILE"

  if [[ $DRY_RUN -eq 1 ]]; then
    stc_display="${stc_val:-n/a}"
    start_display="${start_iso:-n/a}"
    stop_display="${stop_iso:-n/a}"
    if [[ "$status_label" == "merged" ]]; then
      success "$ref  [$status_label]  $subject  stc=$stc_display  start=$start_display  stop=$stop_display"
    else
      info "$ref  [$status_label]  $subject  stc=$stc_display  start=$start_display  stop=$stop_display"
    fi
  fi
done

info "Branches: ${#MERGED[@]} already merged, ${#UNMERGED[@]} pending."

# -------------------------
# Phase 2 — Initialize/update participants.tsv
# -------------------------
TSV="participants.tsv"

# Build the final TSV by merging existing file (if any) with extracted data.
# New data wins over existing entries for the same subject.
build_participants_tsv() {
  {
    # Existing entries (skip header)
    if [[ -f "$TSV" ]]; then
      tail -n +2 "$TSV"
    fi
    # Newly extracted entries
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

  # Write final file with header
  {
    printf 'participant_id\tstc_ref_time\tfmriprep_start\tfmriprep_stop\n'
    cat "${TSV}.tmp"
  } > "$TSV"
  rm -f "${TSV}.tmp"
}

tsv_count="$(wc -l < "$META_TMPFILE" | tr -d ' ')"
info "Extracted metadata for $tsv_count subject(s)."

if [[ $DRY_RUN -eq 1 ]]; then
  info "Dry run — no merges performed."
  exit 0
fi

build_participants_tsv
tsv_count=$(( $(wc -l < "$TSV") - 1 ))  # subtract header
info "participants.tsv: $tsv_count subject(s)."

# -------------------------
# Phase 3 — Merge unmerged branches
# -------------------------
merge_ok=0
merge_fail=0

for ref in ${UNMERGED[@]+"${UNMERGED[@]}"}; do
  info "Merging $ref ..."

  if git merge --no-edit "$ref" 2>/dev/null; then
    success "Merged $ref"
    (( merge_ok++ )) || true
    continue
  fi

  # Merge failed — check if only CITATION files conflict
  conflicted="$(git diff --name-only --diff-filter=U)"
  only_citation=1
  while IFS= read -r cfile; do
    case "$cfile" in
      logs/CITATION.md|logs/CITATION.html|logs/CITATION.tex) ;;
      *) only_citation=0; break ;;
    esac
  done <<< "$conflicted"

  if [[ $only_citation -eq 1 && -n "$conflicted" ]]; then
    # Resolve: keep master's CITATION files
    git checkout master -- logs/CITATION.md logs/CITATION.html logs/CITATION.tex 2>/dev/null || true
    git add logs/CITATION.md logs/CITATION.html logs/CITATION.tex
    git commit --no-edit
    success "Merged $ref (CITATION conflict resolved — kept master)"
    (( merge_ok++ )) || true
  else
    fail "Merge conflict in non-CITATION files for $ref — aborting merge"
    git merge --abort
    (( merge_fail++ )) || true
  fi
done

# -------------------------
# Phase 4 — Write participants.json sidecar, CHANGELOG.txt, and commit
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

# Update CHANGELOG.txt — append a dated entry for newly merged subjects
CHANGELOG="CHANGELOG.txt"
datestamp="$(date +%Y-%m-%d)"

if [[ ! -f "$CHANGELOG" ]]; then
  printf 'CHANGELOG — fMRIPrep 25.2 derivatives\n' > "$CHANGELOG"
  printf '======================================\n\n' >> "$CHANGELOG"
fi

{
  echo "## ${datestamp} — Merge job branches into master"
  echo ""
  if [[ ${#MERGED[@]} -gt 0 ]]; then
    echo "Previously merged (${#MERGED[@]} subjects):"
    for ref in "${MERGED[@]}"; do
      sub=""
      IFS='/' read -ra _parts <<< "$ref"
      for _p in "${_parts[@]}"; do [[ "$_p" == sub-* ]] && sub="$_p" && break; done
      echo "  - ${sub}"
    done
    echo ""
  fi
  if [[ $merge_ok -gt 0 || $merge_fail -gt 0 ]]; then
    echo "Newly merged ($merge_ok subjects):"
    for ref in ${UNMERGED[@]+"${UNMERGED[@]}"}; do
      sub=""
      IFS='/' read -ra _parts <<< "$ref"
      for _p in "${_parts[@]}"; do [[ "$_p" == sub-* ]] && sub="$_p" && break; done
      echo "  - ${sub}  (${ref})"
    done
    echo ""
  fi
  if [[ $merge_fail -gt 0 ]]; then
    echo "Failed merges: $merge_fail"
    echo ""
  fi
  echo "participants.tsv updated ($tsv_count subjects)."
  echo ""
} >> "$CHANGELOG"

git add "$TSV" "$TSV_JSON" "$CHANGELOG"
if ! git diff --cached --quiet -- "$TSV" "$TSV_JSON" "$CHANGELOG"; then
  git commit -m "enh: update participants.tsv/json and CHANGELOG after merging job branches"
  success "Committed participants.tsv, participants.json, and CHANGELOG.txt"
else
  info "participants.tsv already up to date — no commit needed."
fi

# -------------------------
# Summary
# -------------------------
echo ""
info "===== Summary ====="
success "Merged:  $merge_ok"
warn "Already merged: ${#MERGED[@]}"
if [[ $merge_fail -gt 0 ]]; then
  fail "Failed:  $merge_fail"
fi
info "participants.tsv: $tsv_count subject(s)"
