#!/usr/bin/env bash
#
# drop_verified.sh — drop local annex content after verifying a remote has copies
#
# Iterates over subjects on master in the derivatives subdataset, verifies
# that all annexed files for each subject exist on the target remote (default:
# gin), and drops local copies only when safe. Without --force, operates in
# audit-only mode.
#
# Usage:
#   code/drop_verified.sh [--force] [--remote <name>] [-C <derivatives-path>]
#                         [--subject <sub-ID>]

set -euo pipefail

# -------------------------
# Helpers
# -------------------------
die()     { echo -e "\033[31m[FATAL]\033[0m $*" >&2; exit 2; }
info()    { echo -e "\033[36m[INFO]\033[0m $*" >&2; }
success() { echo -e "\033[32m[OK]\033[0m $*" >&2; }
warn()    { echo -e "\033[33m[SKIP]\033[0m $*" >&2; }

# -------------------------
# Args
# -------------------------
REMOTE="gin"
FORCE=0
DERIV_PATH=""
SUBJECT=""

usage() {
  cat <<EOF
Usage:
  drop_verified.sh [--force] [--remote <name>] [-C <derivatives-path>]
                   [--subject <sub-ID>]

Options:
  --force           Actually drop files (default: audit-only / dry-run)
  --remote <name>   Remote to verify against (default: gin)
  -C <path>         Path to the derivatives subdataset
                    (default: derivatives/fmriprep-25.2)
  --subject <id>    Process a single subject (default: all subjects on master)
  -h, --help        Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --force)   FORCE=1; shift ;;
    --remote)  REMOTE="$2"; shift 2 ;;
    -C)        DERIV_PATH="$2"; shift 2 ;;
    --subject) SUBJECT="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) die "Unknown arg: $1" ;;
  esac
done

# Default derivatives path
if [[ -z "$DERIV_PATH" ]]; then
  DERIV_PATH="derivatives/fmriprep-25.2"
fi

cd "$DERIV_PATH" || die "Cannot cd to $DERIV_PATH"

# Verify we are inside a git repo
[[ -d .git || -f .git ]] || die "Not a git repository: $(pwd)"

# -------------------------
# Collect subjects
# -------------------------
if [[ -n "$SUBJECT" ]]; then
  # Verify the requested subject exists on master
  if ! git ls-tree --name-only master | grep -qx "$SUBJECT"; then
    die "Subject $SUBJECT not found on master"
  fi
  SUBJECTS=("$SUBJECT")
else
  SUBJECTS=()
  while IFS= read -r d; do
    SUBJECTS+=("$d")
  done < <(git ls-tree --name-only master | grep '^sub-' | grep -v '\.html$')
fi

if [[ ${#SUBJECTS[@]} -eq 0 ]]; then
  info "No subjects found on master."
  exit 0
fi

info "Subjects on master: ${#SUBJECTS[@]}"
info "Remote: $REMOTE"
if [[ $FORCE -eq 0 ]]; then
  info "Mode: audit-only (use --force to actually drop)"
else
  info "Mode: DROP"
fi
echo ""

# -------------------------
# TSV header
# -------------------------
printf 'participant_id\tlocal_files\tmissing_on_%s\taction\tfreed_bytes\n' "$REMOTE"

# -------------------------
# Per-subject processing
# -------------------------
total_dropped=0
total_skipped=0

for sub in "${SUBJECTS[@]}"; do
  # Count annexed files present locally
  local_count=0
  local_count="$(git annex find --in here -- "${sub}/" 2>/dev/null | wc -l | tr -d ' ')" || true

  if [[ "$local_count" -eq 0 ]]; then
    printf '%s\t%d\t%d\t%s\t%s\n' "$sub" 0 0 "nothing" "0"
    continue
  fi

  # Count annexed files present locally but NOT on the remote
  missing_count=0
  missing_files=""
  missing_files="$(git annex find --in here --not --in "$REMOTE" -- "${sub}/" 2>/dev/null)" || true
  if [[ -n "$missing_files" ]]; then
    missing_count="$(echo "$missing_files" | wc -l | tr -d ' ')"
  fi

  if [[ "$missing_count" -gt 0 ]]; then
    warn "$sub — $missing_count file(s) NOT on $REMOTE:"
    echo "$missing_files" | head -10 | sed 's/^/    /' >&2
    if [[ "$missing_count" -gt 10 ]]; then
      info "    ... and $(( missing_count - 10 )) more"
    fi
    printf '%s\t%d\t%d\t%s\t%s\n' "$sub" "$local_count" "$missing_count" "skipped" "0"
    (( total_skipped++ )) || true
    continue
  fi

  # All files verified on remote
  if [[ $FORCE -eq 0 ]]; then
    printf '%s\t%d\t%d\t%s\t%s\n' "$sub" "$local_count" 0 "would-drop" "n/a"
    continue
  fi

  # Actually drop
  drop_output=""
  drop_output="$(git annex drop -- "${sub}/" 2>&1)" || {
    warn "$sub — git annex drop failed"
    printf '%s\t%d\t%d\t%s\t%s\n' "$sub" "$local_count" 0 "error" "0"
    echo "$drop_output" | head -5 | sed 's/^/    /' >&2
    continue
  }

  # Count how many bytes were freed (parse drop output)
  freed_bytes="$(echo "$drop_output" | sed -n 's/.*(\([0-9]*\) bytes freed).*/\1/p' \
    | awk '{ s += $1 } END { print s+0 }')"

  # Human-readable size
  if [[ "$freed_bytes" -gt 0 ]]; then
    freed_human="$(numfmt --to=iec-i --suffix=B "$freed_bytes" 2>/dev/null || echo "${freed_bytes}B")"
  else
    freed_human="0"
  fi

  success "$sub — dropped $local_count file(s), freed $freed_human"
  printf '%s\t%d\t%d\t%s\t%s\n' "$sub" "$local_count" 0 "dropped" "$freed_human"
  (( total_dropped++ )) || true
done

# -------------------------
# Summary
# -------------------------
echo "" >&2
info "===== Summary ====="
info "Subjects processed: ${#SUBJECTS[@]}"
if [[ $FORCE -eq 1 ]]; then
  success "Dropped: $total_dropped"
fi
if [[ $total_skipped -gt 0 ]]; then
  warn "Skipped (missing on $REMOTE): $total_skipped"
fi
