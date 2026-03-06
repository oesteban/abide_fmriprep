#!/usr/bin/env bash
#
# run_abide1_curnagl.sh — Runbook for processing remaining ABIDE 1 subjects on Curnagl
#
# Execute phases interactively from the Curnagl login node.
# Usage:
#   bash code/run_abide1_curnagl.sh <phase>
#
# Phases:
#   A   — Consolidate derivatives master (merge job branches, push to GIN)
#   B   — Pre-flight checks + submit 10-subject test batch
#   C   — Submit remaining ABIDE 1 subjects (full batch)
#   D   — Post-processing (merge new job branches after all jobs complete)
#
# All phases assume:
#   - CWD is the superdataset root: /scratch/oesteban/abide_fmriprep
#   - micromamba env "datalad" is available
#   - GIN SSH is configured and reachable

set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-/scratch/oesteban/abide_fmriprep}"
DERIV_REL="derivatives/fmriprep-25.2"
CONTAINER_NAME="fmriprep-apptainer"
DATASET="abide1"

# Colors
info()    { echo -e "\033[36m[INFO]\033[0m $*"; }
success() { echo -e "\033[32m[ OK ]\033[0m $*"; }
warn()    { echo -e "\033[33m[WARN]\033[0m $*"; }
die()     { echo -e "\033[31m[FATAL]\033[0m $*" >&2; exit 2; }

# -------------------------------------------------------------------
phase_A() {
  info "=== Phase A: Consolidate derivatives master ==="
  cd "$PROJECT_ROOT"

  # Use the existing merge_job_branches.sh script
  info "Running merge_job_branches.sh (fetches GIN, merges unmerged branches)..."
  bash code/merge_job_branches.sh \
    -C "$PROJECT_ROOT/$DERIV_REL" \
    --logs-dir "$PROJECT_ROOT/logs"

  # Push updated master to GIN
  cd "$PROJECT_ROOT/$DERIV_REL"
  info "Pushing derivatives master to GIN..."
  git push gin master

  info "Syncing annex content to GIN..."
  git annex sync gin --content --only-annex

  # Count subjects on master
  local done_count
  done_count=$(git ls-tree --name-only master | grep '^sub-v1' | grep -v '\.html$' | wc -l | tr -d ' ')
  success "ABIDE 1 subjects on derivatives master: $done_count"

  cd "$PROJECT_ROOT"
  local total_abide1
  total_abide1=$(awk -F'\t' 'NR>1 && $2=="abide1" { count++ } END { print count }' \
    inputs/abide-both/participants.tsv)
  local remaining=$(( total_abide1 - done_count ))
  success "Total ABIDE 1: $total_abide1 | Done: $done_count | Remaining: $remaining"
}

# -------------------------------------------------------------------
phase_B() {
  info "=== Phase B: Pre-flight checks + 10-subject test batch ==="
  cd "$PROJECT_ROOT"

  # B.1 — Pre-flight
  info "Checking Apptainer image..."
  if [[ -f .datalad/environments/fmriprep-apptainer.sif ]]; then
    success "SIF found: $(ls -lh .datalad/environments/fmriprep-apptainer.sif | awk '{print $5}')"
  else
    die "Missing .datalad/environments/fmriprep-apptainer.sif"
  fi

  info "Checking TemplateFlow templates..."
  local broken_links
  broken_links=$(find \
    inputs/templateflow/tpl-MNI152NLin2009cAsym \
    inputs/templateflow/tpl-MNI152NLin6Asym \
    inputs/templateflow/tpl-OASIS30ANTs \
    inputs/templateflow/tpl-fsLR \
    inputs/templateflow/tpl-fsaverage \
    -type l -exec test ! -e {} \; -print 2>/dev/null | head -5)
  if [[ -z "$broken_links" ]]; then
    success "All TemplateFlow symlinks resolve"
  else
    die "Broken TemplateFlow symlinks found:\n$broken_links\nRun: cd inputs/templateflow && datalad get -r tpl-MNI152NLin2009cAsym tpl-MNI152NLin6Asym tpl-OASIS30ANTs tpl-fsLR tpl-fsaverage"
  fi

  info "Checking FreeSurfer license..."
  if [[ -f env/secrets/fs_license.txt ]]; then
    success "FreeSurfer license found"
  else
    die "Missing env/secrets/fs_license.txt"
  fi

  info "Checking GIN SSH connectivity..."
  if ssh -o ConnectTimeout=5 -T git@gin.g-node.org 2>&1 | grep -qi "welcome\|success\|authenticated"; then
    success "GIN SSH reachable"
  else
    warn "GIN SSH test inconclusive — verify manually: ssh -T git@gin.g-node.org"
  fi

  # B.2 — Preview remaining subjects
  info "Computing remaining ABIDE 1 subject list..."
  local remaining_list
  remaining_list=$(comm -23 \
    <(awk -F'\t' 'NR>1 && $2=="abide1" { print $1 }' inputs/abide-both/participants.tsv | sort) \
    <(git -C "$DERIV_REL" ls-tree --name-only master | grep '^sub-v1' | grep -v '\.html$' | sort))
  local remaining_count
  remaining_count=$(echo "$remaining_list" | grep -c '^sub-' || true)
  info "Remaining ABIDE 1 subjects: $remaining_count"
  info "First 10 subjects (test batch):"
  echo "$remaining_list" | head -10

  # B.3 — Submit
  info ""
  info "Ready to submit 10-subject test batch."
  info "Command:"
  echo ""
  echo "  # No GIT_CONFIG_GLOBAL needed — Curnagl /users/ is GPFS-shared"
  echo ""
  echo "  sbatch --array=1-10 code/bootstrap_fmriprep_ARRAY.sbatch.sh \\"
  echo "    --project-root $PROJECT_ROOT \\"
  echo "    --container-name $CONTAINER_NAME \\"
  echo "    --dataset $DATASET"
  echo ""
  read -rp "Submit now? [y/N] " yn
  case "$yn" in
    [Yy]*)
      # No GIT_CONFIG_GLOBAL needed — Curnagl /users/ is GPFS-shared
      sbatch --array=1-10 code/bootstrap_fmriprep_ARRAY.sbatch.sh \
        --project-root "$PROJECT_ROOT" \
        --container-name "$CONTAINER_NAME" \
        --dataset "$DATASET"
      ;;
    *)
      info "Skipped. Run the command above manually."
      ;;
  esac
}

# -------------------------------------------------------------------
phase_C() {
  info "=== Phase C: Full ABIDE 1 submission ==="
  cd "$PROJECT_ROOT"

  # Compute remaining count
  local remaining_count
  remaining_count=$(comm -23 \
    <(awk -F'\t' 'NR>1 && $2=="abide1" { print $1 }' inputs/abide-both/participants.tsv | sort) \
    <(git -C "$DERIV_REL" ls-tree --name-only master | grep '^sub-v1' | grep -v '\.html$' | sort) \
    | wc -l | tr -d ' ')

  info "Remaining ABIDE 1 subjects: $remaining_count"

  if [[ $remaining_count -le 10 ]]; then
    warn "Only $remaining_count subjects remaining — no full batch needed."
    return
  fi

  # Start from index 11 to skip test batch
  local start_idx=11
  info "Submitting indices $start_idx through $remaining_count (skipping test batch 1-10)."

  if [[ $remaining_count -le 1000 ]]; then
    echo ""
    echo "  # No GIT_CONFIG_GLOBAL needed — Curnagl /users/ is GPFS-shared"
    echo ""
    echo "  sbatch --array=${start_idx}-${remaining_count} code/bootstrap_fmriprep_ARRAY.sbatch.sh \\"
    echo "    --project-root $PROJECT_ROOT \\"
    echo "    --container-name $CONTAINER_NAME \\"
    echo "    --dataset $DATASET"
    echo ""
  else
    # Split into chunks of 1000 for SLURM max array size
    local chunk_start=$start_idx
    local chunk_end
    while [[ $chunk_start -le $remaining_count ]]; do
      chunk_end=$(( chunk_start + 999 ))
      [[ $chunk_end -gt $remaining_count ]] && chunk_end=$remaining_count

      echo ""
      echo "  sbatch --array=${chunk_start}-${chunk_end} code/bootstrap_fmriprep_ARRAY.sbatch.sh \\"
      echo "    --project-root $PROJECT_ROOT \\"
      echo "    --container-name $CONTAINER_NAME \\"
      echo "    --dataset $DATASET"

      chunk_start=$(( chunk_end + 1 ))
    done
    echo ""
  fi

  read -rp "Submit now? [y/N] " yn
  case "$yn" in
    [Yy]*)
      # No GIT_CONFIG_GLOBAL needed — Curnagl /users/ is GPFS-shared
      if [[ $remaining_count -le 1000 ]]; then
        sbatch --array=${start_idx}-${remaining_count} \
          code/bootstrap_fmriprep_ARRAY.sbatch.sh \
          --project-root "$PROJECT_ROOT" \
          --container-name "$CONTAINER_NAME" \
          --dataset "$DATASET"
      else
        local chunk_start=$start_idx
        local chunk_end
        while [[ $chunk_start -le $remaining_count ]]; do
          chunk_end=$(( chunk_start + 999 ))
          [[ $chunk_end -gt $remaining_count ]] && chunk_end=$remaining_count
          sbatch --array=${chunk_start}-${chunk_end} \
            code/bootstrap_fmriprep_ARRAY.sbatch.sh \
            --project-root "$PROJECT_ROOT" \
            --container-name "$CONTAINER_NAME" \
            --dataset "$DATASET"
          chunk_start=$(( chunk_end + 1 ))
        done
      fi
      ;;
    *)
      info "Skipped. Run the command(s) above manually."
      ;;
  esac
}

# -------------------------------------------------------------------
phase_D() {
  info "=== Phase D: Post-processing (after all jobs complete) ==="
  cd "$PROJECT_ROOT"

  # Same as Phase A — merge new job branches, push to GIN
  phase_A

  info ""
  info "Done. Update the superdataset submodule pointer on your local workstation:"
  echo ""
  echo "  cd /Users/oesteban/workspace/abide_preproc"
  echo "  cd derivatives/fmriprep-25.2"
  echo "  git fetch gin"
  echo "  git merge gin/master"
  echo "  cd ../.."
  echo "  git add derivatives/fmriprep-25.2"
  echo "  git commit -m 'enh: update derivatives pointer after ABIDE 1 batch'"
  echo ""
}

# -------------------------------------------------------------------
# Monitor helper (not a phase — call with: bash code/run_abide1_curnagl.sh monitor <JOBID>)
monitor() {
  local jobid="${1:?Usage: $0 monitor <JOBID>}"

  echo "=== Job $jobid status ==="
  sacct -j "$jobid" --format=State --noheader | sort | uniq -c | sort -rn

  echo ""
  echo "=== Failed tasks ==="
  sacct -j "$jobid" --format=JobID,State,ExitCode,Elapsed --noheader | grep -i FAILED || echo "(none)"

  echo ""
  echo "=== Queue status ==="
  squeue -u "$USER" -j "$jobid" 2>/dev/null || echo "(no tasks in queue)"
}

# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------
case "${1:-}" in
  A|a) phase_A ;;
  B|b) phase_B ;;
  C|c) phase_C ;;
  D|d) phase_D ;;
  monitor) shift; monitor "$@" ;;
  *)
    echo "Usage: bash code/run_abide1_curnagl.sh <phase>"
    echo ""
    echo "Phases:"
    echo "  A  — Consolidate derivatives master (merge + push to GIN)"
    echo "  B  — Pre-flight checks + submit 10-subject test batch"
    echo "  C  — Submit remaining ABIDE 1 subjects (full batch)"
    echo "  D  — Post-processing (merge new job branches after completion)"
    echo ""
    echo "Helpers:"
    echo "  monitor <JOBID>  — Check job status and failures"
    exit 1
    ;;
esac
