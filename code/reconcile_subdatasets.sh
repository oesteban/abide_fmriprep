#!/usr/bin/env bash
#
# reconcile_subdatasets.sh — octopus-merge job branches in site-level datasets
#
# After a batch of SLURM jobs completes, each site dataset has one or more
# job/<sub-XXX> branches on GIN. This script fetches those branches,
# octopus-merges them into master (with sequential fallback for CITATION
# conflicts), updates participants.tsv per site, and optionally pushes.
#
# Usage:
#   code/reconcile_subdatasets.sh [-C <superdataset-root>] [--site <prefix>]
#                                 [--dry-run] [--push] [--no-delete-branches]
#                                 [--remote <name>] [--logs-dir <path>]

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
PROJECT_ROOT="."
SITE_FILTERS=()
DRY_RUN=0
DO_PUSH=0
DELETE_BRANCHES=1
REMOTE="gin"
LOGS_DIR=""

usage() {
  cat <<EOF
Usage:
  reconcile_subdatasets.sh [-C <superdataset-root>] [--site <prefix>] ...
                           [--dry-run] [--push] [--no-delete-branches]
                           [--remote <name>] [--logs-dir <path>]

Options:
  -C <path>              Superdataset root (default: .)
  --site <prefix>        Filter to a site prefix (repeatable, e.g. --site v1s0 --site v2s3)
  --dry-run              Show what would happen without making changes
  --push                 Push merged master to github (triggers gin via publish-depends)
  --no-delete-branches   Keep merged branches on remote after merging
  --remote <name>        Remote to fetch job branches from (default: gin)
  --logs-dir <path>      SLURM logs directory (default: <root>/logs)
  -h, --help             Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -C) PROJECT_ROOT="$2"; shift 2 ;;
    --site) SITE_FILTERS+=("$2"); shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    --push) DO_PUSH=1; shift ;;
    --no-delete-branches) DELETE_BRANCHES=0; shift ;;
    --remote) REMOTE="$2"; shift 2 ;;
    --logs-dir) LOGS_DIR="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) die "Unknown arg: $1" ;;
  esac
done

# -------------------------
# Phase 0 — Setup
# -------------------------
PROJECT_ROOT="$(cd "$PROJECT_ROOT" && pwd)"
DERIV_DIR="$PROJECT_ROOT/derivatives"

[[ -d "$PROJECT_ROOT/.datalad" ]] || die "Not a DataLad superdataset: $PROJECT_ROOT"
[[ -d "$DERIV_DIR" ]] || die "Derivatives directory not found: $DERIV_DIR"

# SLURM logs directory
: "${LOGS_DIR:=$PROJECT_ROOT/logs}"
if [[ -d "$LOGS_DIR" ]]; then
  info "SLURM logs directory: $LOGS_DIR"
else
  warn "SLURM logs directory not found: $LOGS_DIR"
  LOGS_DIR=""
fi

# Build subject→logfile index from SLURM logs
LOG_INDEX=""
TMPFILES=()
cleanup() {
  rm -f "${TMPFILES[@]}" 2>/dev/null || true
}
trap cleanup EXIT

if [[ -n "$LOGS_DIR" ]]; then
  LOG_INDEX="$(mktemp)"
  TMPFILES+=("$LOG_INDEX")

  info "Indexing successful SLURM logs..."
  while IFS= read -r logfile; do
    sub="$(sed -n 's/.*SUBJECT=\(sub-[^ ]*\).*/\1/p' "$logfile" | head -1)"
    if [[ -n "$sub" ]]; then
      printf '%s\t%s\n' "$sub" "$logfile" >> "$LOG_INDEX"
    fi
  done < <(grep -l 'fMRIPrep finished successfully' "$LOGS_DIR"/fmriprep_*.out 2>/dev/null || true)

  log_count="$(wc -l < "$LOG_INDEX" | tr -d ' ')"
  info "Indexed $log_count successful log(s)."
fi

# -------------------------
# Phase 1 — Discover site datasets
# -------------------------
SITE_DIRS=()
for site_dir in "$DERIV_DIR"/v[12]s*/; do
  [[ -d "$site_dir" ]] || continue
  [[ -d "$site_dir/.git" || -f "$site_dir/.git" ]] || continue

  site_prefix="$(basename "$site_dir")"

  # Apply --site filter if given
  if [[ ${#SITE_FILTERS[@]} -gt 0 ]]; then
    match=0
    for f in "${SITE_FILTERS[@]}"; do
      [[ "$site_prefix" == "$f" ]] && match=1 && break
    done
    [[ $match -eq 1 ]] || continue
  fi

  SITE_DIRS+=("$site_dir")
done

info "Found ${#SITE_DIRS[@]} site dataset(s) to process."

if [[ ${#SITE_DIRS[@]} -eq 0 ]]; then
  info "Nothing to do."
  exit 0
fi

# -------------------------
# Phase 2 — Per-site loop
# -------------------------
total_merged=0
total_failed=0
total_skipped=0
sites_modified=0

for SITE_DIR in "${SITE_DIRS[@]}"; do
  SITE_PREFIX="$(basename "$SITE_DIR")"
  info "--- Processing site: $SITE_PREFIX ---"

  # Ensure we are on master
  current_branch="$(git -C "$SITE_DIR" symbolic-ref --short HEAD 2>/dev/null)" || true
  if [[ "$current_branch" != "master" ]]; then
    warn "$SITE_PREFIX: not on master (on $current_branch) — skipping"
    continue
  fi

  # A. Fetch remote
  info "$SITE_PREFIX: fetching from '$REMOTE'..."
  if ! git -C "$SITE_DIR" fetch "$REMOTE" 2>/dev/null; then
    warn "$SITE_PREFIX: cannot fetch remote '$REMOTE' — skipping"
    continue
  fi

  # B. Discover unmerged job branches
  ALL_BRANCHES=()
  while IFS= read -r line; do
    [[ -n "$line" ]] && ALL_BRANCHES+=("$line")
  done < <(git -C "$SITE_DIR" branch -r 2>/dev/null \
    | sed 's/^[[:space:]]*//' \
    | grep "^${REMOTE}/job/" || true)

  if [[ ${#ALL_BRANCHES[@]} -eq 0 ]]; then
    info "$SITE_PREFIX: no job branches found."
    continue
  fi

  UNMERGED=()
  ALREADY_MERGED=()
  for ref in "${ALL_BRANCHES[@]}"; do
    if git -C "$SITE_DIR" merge-base --is-ancestor "$ref" master 2>/dev/null; then
      ALREADY_MERGED+=("$ref")
    else
      UNMERGED+=("$ref")
    fi
  done

  info "$SITE_PREFIX: ${#UNMERGED[@]} unmerged, ${#ALREADY_MERGED[@]} already merged."
  (( total_skipped += ${#ALREADY_MERGED[@]} )) || true

  if [[ ${#UNMERGED[@]} -eq 0 ]]; then
    # Still handle branch cleanup for already-merged branches
    if [[ $DELETE_BRANCHES -eq 1 && ${#ALREADY_MERGED[@]} -gt 0 && $DRY_RUN -eq 0 ]]; then
      info "$SITE_PREFIX: cleaning up ${#ALREADY_MERGED[@]} already-merged branch(es)..."
      delete_refspecs=()
      for ref in "${ALREADY_MERGED[@]}"; do
        branch_name="${ref#${REMOTE}/}"
        delete_refspecs+=(":refs/heads/${branch_name}")
      done
      git -C "$SITE_DIR" push "$REMOTE" "${delete_refspecs[@]}" 2>/dev/null || true
      git -C "$SITE_DIR" remote prune "$REMOTE" 2>/dev/null || true
    fi
    continue
  fi

  # C. Extract metadata from unmerged branches
  META_TMPFILE="$(mktemp)"
  TMPFILES+=("$META_TMPFILE")

  for ref in "${UNMERGED[@]}"; do
    # Subject ID from branch name: gin/job/sub-v1s0x0050642 → sub-v1s0x0050642
    subject=""
    IFS='/' read -ra parts <<< "$ref"
    for part in "${parts[@]}"; do
      if [[ "$part" == sub-* ]]; then
        subject="$part"
        break
      fi
    done
    [[ -n "$subject" ]] || { warn "Cannot extract subject from $ref — skipping"; continue; }

    # stc_ref_time from logs/CITATION.md on the branch
    stc_val=""
    stc_val="$(git -C "$SITE_DIR" show "${ref}:logs/CITATION.md" 2>/dev/null \
      | sed -n 's/.*slice-time corrected to \([0-9.]*\)s.*/\1/p')" || true

    # Timing: prefer direct ISO lines from SLURM logs
    start_iso=""
    stop_iso=""

    if [[ -n "$LOG_INDEX" ]]; then
      logfile="$(awk -F'\t' -v sid="$subject" '$1 == sid { print $2; exit }' "$LOG_INDEX")"
      if [[ -n "$logfile" && -f "$logfile" ]]; then
        # Direct ISO timestamps from sbatch (fmriprep_start=, fmriprep_stop=)
        start_iso="$(sed -n 's/.*fmriprep_start=\([0-9T:-]*\).*/\1/p' "$logfile" | head -1)" || true
        stop_iso="$(sed -n 's/.*fmriprep_stop=\([0-9T:-]*\).*/\1/p' "$logfile" | head -1)" || true

        # Fallback: nipype timestamps
        if [[ -z "$start_iso" ]]; then
          start_raw="$(grep -B1 'Running fMRIPrep version' "$logfile" \
            | sed -n 's/^\([0-9]\{6\}-[0-9:]*\).*/\1/p' | head -1)" || true
          [[ -n "$start_raw" ]] && start_iso="$(nipype_ts_to_iso "$start_raw")"
        fi
        if [[ -z "$stop_iso" ]]; then
          stop_raw="$(grep -B1 'fMRIPrep finished successfully' "$logfile" \
            | sed -n 's/^\([0-9]\{6\}-[0-9:]*\).*/\1/p' | head -1)" || true
          [[ -n "$stop_raw" ]] && stop_iso="$(nipype_ts_to_iso "$stop_raw")"
        fi
      fi
    fi

    # Fallback for start: fmriprep.toml directory name on the branch
    if [[ -z "$start_iso" ]]; then
      start_raw=""
      start_raw="$(git -C "$SITE_DIR" ls-tree -r --name-only "$ref" -- "${subject}/" 2>/dev/null \
        | sed -n 's|.*/log/\([0-9]\{8\}-[0-9]\{6\}\)_.*/fmriprep\.toml|\1|p' \
        | head -1)" || true
      [[ -n "$start_raw" ]] && start_iso="$(toml_dir_to_iso "$start_raw")"
    fi

    printf '%s\t%s\t%s\t%s\n' \
      "$subject" "${stc_val:-n/a}" "${start_iso:-n/a}" "${stop_iso:-n/a}" \
      >> "$META_TMPFILE"

    if [[ $DRY_RUN -eq 1 ]]; then
      info "  $ref  $subject  stc=${stc_val:-n/a}  start=${start_iso:-n/a}  stop=${stop_iso:-n/a}"
    fi
  done

  if [[ $DRY_RUN -eq 1 ]]; then
    continue
  fi

  # D. Octopus merge
  site_merged=0
  site_failed=0
  NEWLY_MERGED=()

  # Try octopus merge first (all branches at once)
  branch_refs=("${UNMERGED[@]}")
  info "$SITE_PREFIX: attempting octopus merge of ${#branch_refs[@]} branch(es)..."

  if git -C "$SITE_DIR" merge --no-edit "${branch_refs[@]}" 2>/dev/null; then
    success "$SITE_PREFIX: octopus merge succeeded (${#branch_refs[@]} branches)"
    NEWLY_MERGED+=("${branch_refs[@]}")
    (( site_merged += ${#branch_refs[@]} )) || true
  else
    # Octopus failed — abort and fall back to sequential
    git -C "$SITE_DIR" merge --abort 2>/dev/null || true
    warn "$SITE_PREFIX: octopus merge failed — falling back to sequential"

    for ref in "${branch_refs[@]}"; do
      if git -C "$SITE_DIR" merge --no-edit "$ref" 2>/dev/null; then
        success "$SITE_PREFIX: merged $ref"
        NEWLY_MERGED+=("$ref")
        (( site_merged++ )) || true
        continue
      fi

      # Check if only CITATION files conflict
      conflicted="$(git -C "$SITE_DIR" diff --name-only --diff-filter=U)"
      only_citation=1
      while IFS= read -r cfile; do
        [[ -z "$cfile" ]] && continue
        case "$cfile" in
          logs/CITATION.md|logs/CITATION.html|logs/CITATION.tex) ;;
          *) only_citation=0; break ;;
        esac
      done <<< "$conflicted"

      if [[ $only_citation -eq 1 && -n "$conflicted" ]]; then
        # Resolve: keep master's CITATION files
        git -C "$SITE_DIR" checkout master -- logs/CITATION.md logs/CITATION.html logs/CITATION.tex 2>/dev/null || true
        git -C "$SITE_DIR" add logs/CITATION.md logs/CITATION.html logs/CITATION.tex 2>/dev/null || true
        git -C "$SITE_DIR" commit --no-edit 2>/dev/null
        success "$SITE_PREFIX: merged $ref (CITATION conflict resolved — kept master)"
        NEWLY_MERGED+=("$ref")
        (( site_merged++ )) || true
      else
        fail "$SITE_PREFIX: merge conflict in non-CITATION files for $ref — aborting"
        git -C "$SITE_DIR" merge --abort 2>/dev/null || true
        (( site_failed++ )) || true
      fi
    done
  fi

  (( total_merged += site_merged )) || true
  (( total_failed += site_failed )) || true

  # E. Update participants.tsv
  if [[ $site_merged -gt 0 && -s "$META_TMPFILE" ]]; then
    TSV="$SITE_DIR/participants.tsv"

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

    {
      printf 'participant_id\tstc_ref_time\tfmriprep_start\tfmriprep_stop\n'
      cat "${TSV}.tmp"
    } > "$TSV"
    rm -f "${TSV}.tmp"

    # Write participants.json sidecar
    TSV_JSON="$SITE_DIR/participants.json"
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

    tsv_count=$(( $(wc -l < "$TSV") - 1 ))
    info "$SITE_PREFIX: participants.tsv updated ($tsv_count subjects)"

    # Commit within site dataset
    git -C "$SITE_DIR" add participants.tsv participants.json
    if ! git -C "$SITE_DIR" diff --cached --quiet -- participants.tsv participants.json; then
      git -C "$SITE_DIR" commit -m "enh: update participants.tsv/json after merging $site_merged job branch(es)"
      success "$SITE_PREFIX: committed participants.tsv and participants.json"
    fi

    (( sites_modified++ )) || true
  fi

  # F. Push (if --push)
  if [[ $DO_PUSH -eq 1 && $site_merged -gt 0 ]]; then
    info "$SITE_PREFIX: pushing to github..."
    if datalad push -d "$SITE_DIR" --to github 2>/dev/null; then
      success "$SITE_PREFIX: pushed to github"
    else
      warn "$SITE_PREFIX: datalad push --to github failed"
    fi
  fi

  # G. Delete merged branches (unless --no-delete-branches)
  if [[ $DELETE_BRANCHES -eq 1 ]]; then
    TO_DELETE=()
    TO_DELETE+=("${ALREADY_MERGED[@]}")
    TO_DELETE+=(${NEWLY_MERGED[@]+"${NEWLY_MERGED[@]}"})

    if [[ ${#TO_DELETE[@]} -gt 0 ]]; then
      info "$SITE_PREFIX: deleting ${#TO_DELETE[@]} merged branch(es) from '$REMOTE'..."
      delete_refspecs=()
      for ref in "${TO_DELETE[@]}"; do
        branch_name="${ref#${REMOTE}/}"
        if git -C "$SITE_DIR" show-ref --verify --quiet "refs/remotes/${ref}" 2>/dev/null; then
          delete_refspecs+=(":refs/heads/${branch_name}")
        fi
      done

      if [[ ${#delete_refspecs[@]} -gt 0 ]]; then
        if git -C "$SITE_DIR" push "$REMOTE" "${delete_refspecs[@]}" 2>/dev/null; then
          success "$SITE_PREFIX: deleted ${#delete_refspecs[@]} branch(es) from '$REMOTE'"
        else
          warn "$SITE_PREFIX: some branch deletions failed"
        fi
      fi
      git -C "$SITE_DIR" remote prune "$REMOTE" 2>/dev/null || true
    fi
  fi
done

# -------------------------
# Phase 3 — Save superdataset
# -------------------------
if [[ $DRY_RUN -eq 0 && $sites_modified -gt 0 ]]; then
  info "Saving superdataset state..."
  datalad save -d "$PROJECT_ROOT" -m "Reconcile $total_merged subject(s) across $sites_modified site(s)"
  success "Superdataset saved."
fi

# -------------------------
# Phase 4 — Summary
# -------------------------
echo ""
info "===== Summary ====="
if [[ $DRY_RUN -eq 1 ]]; then
  info "Dry run — no changes made."
fi
success "Merged:          $total_merged"
warn    "Already merged:  $total_skipped"
if [[ $total_failed -gt 0 ]]; then
  fail  "Failed:          $total_failed"
fi
info    "Sites modified:  $sites_modified"
