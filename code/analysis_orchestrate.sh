#!/bin/bash
set -euo pipefail

# -------------------------------------------------------------------
# Orchestrate site-by-site time series extraction.
#
# For each site: stage BOLD data from GIN, submit array job, wait for
# completion, drop BOLD data to reclaim scratch.
#
# Usage:
#   bash code/analysis_orchestrate.sh -C /path/to/abide_fmriprep
#
# Prerequisites:
#   - Pre-screen QC completed (derivatives/connectivity/qc_prescreen.tsv)
#   - micromamba env 'abide-analysis' installed
#   - micromamba env 'datalad' installed (for datalad get/drop)
# -------------------------------------------------------------------

PROJECT_ROOT=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        -C) PROJECT_ROOT="$2"; shift 2 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

if [[ -z "$PROJECT_ROOT" ]]; then
    echo "ERROR: -C <project-root> is required"
    exit 1
fi

PROJECT_ROOT=$(cd "$PROJECT_ROOT" && pwd)
FMRIPREP_DIR="$PROJECT_ROOT/derivatives/fmriprep-25.2"
CONN_DIR="$PROJECT_ROOT/derivatives/connectivity"
QC_TSV="$CONN_DIR/qc_prescreen.tsv"

if [[ ! -f "$QC_TSV" ]]; then
    echo "ERROR: Pre-screen QC not found at $QC_TSV"
    echo "  Run: sbatch code/analysis_prescreen.sbatch --project-root $PROJECT_ROOT"
    exit 1
fi

# Extract unique site prefixes from subjects that passed QC
# (site prefix = v1s0, v1s1, ..., v2s18)
SITES=$(awk -F'\t' 'NR>1 && $NF=="pass" { match($1, /sub-(v[12]s[0-9]+)x/, a); print a[1] }' "$QC_TSV" | sort -u)

echo "=== ABIDE Analysis Orchestrator ==="
echo "Project root: $PROJECT_ROOT"
echo "Sites to process: $(echo $SITES | wc -w)"
echo ""

for site in $SITES; do
    echo "--- Site: $site ---"

    # 1. Generate subjects file for this site
    SITE_SUBJECTS="$CONN_DIR/subjects_${site}.txt"
    awk -F'\t' -v site="$site" \
        'NR>1 && $NF=="pass" && $1 ~ "sub-"site"x" { print $1 }' \
        "$QC_TSV" > "$SITE_SUBJECTS"
    N_SUBJECTS=$(wc -l < "$SITE_SUBJECTS")

    if [[ "$N_SUBJECTS" -eq 0 ]]; then
        echo "  No passing subjects, skipping."
        continue
    fi
    echo "  Subjects: $N_SUBJECTS"

    # 2. Stage BOLD data from GIN
    echo "  Staging BOLD data..."
    eval "$(micromamba shell hook -s bash)"
    micromamba activate datalad

    while IFS= read -r sub; do
        # Get the selected run from qc_prescreen.tsv (may include acq- entity, e.g. "acq-pedj_run-1")
        run=$(awk -F'\t' -v sub="$sub" 'NR>1 && $1==sub { print $5 }' "$QC_TSV")
        bold_pattern="${sub}/ses-1/func/${sub}_ses-1_task-rest_${run}_space-MNI152NLin2009cAsym_desc-preproc_bold.nii.gz"
        mask_pattern="${sub}/ses-1/func/${sub}_ses-1_task-rest_${run}_space-MNI152NLin2009cAsym_desc-brain_mask.nii.gz"
        bold_json="${sub}/ses-1/func/${sub}_ses-1_task-rest_${run}_space-MNI152NLin2009cAsym_desc-preproc_bold.json"

        cd "$FMRIPREP_DIR"
        datalad get -J 4 "$bold_pattern" "$mask_pattern" "$bold_json" 2>/dev/null || \
            echo "  WARNING: Failed to get data for $sub $run"
    done < "$SITE_SUBJECTS"

    # 3. Submit array job
    echo "  Submitting SLURM array job..."
    micromamba activate abide-analysis  # for sbatch submission

    JOB_ID=$(sbatch --parsable \
        --array="1-${N_SUBJECTS}" \
        "$PROJECT_ROOT/code/analysis_timeseries.sbatch" \
        --project-root "$PROJECT_ROOT" \
        --subjects-file "$SITE_SUBJECTS")
    echo "  Job ID: $JOB_ID (array 1-${N_SUBJECTS})"

    # 4. Wait for job completion
    echo "  Waiting for job $JOB_ID to complete..."
    while squeue -j "$JOB_ID" -h 2>/dev/null | grep -q .; do
        sleep 60
    done
    echo "  Job $JOB_ID completed."

    # 5. Drop BOLD data to reclaim scratch
    echo "  Dropping BOLD data for site $site..."
    micromamba activate datalad
    cd "$FMRIPREP_DIR"
    while IFS= read -r sub; do
        # selected_run may include acq- entity
        run=$(awk -F'\t' -v sub="$sub" 'NR>1 && $1==sub { print $5 }' "$QC_TSV")
        bold_pattern="${sub}/ses-1/func/${sub}_ses-1_task-rest_${run}_space-MNI152NLin2009cAsym_desc-preproc_bold.nii.gz"
        mask_pattern="${sub}/ses-1/func/${sub}_ses-1_task-rest_${run}_space-MNI152NLin2009cAsym_desc-brain_mask.nii.gz"
        datalad drop "$bold_pattern" "$mask_pattern" 2>/dev/null || true
    done < "$SITE_SUBJECTS"

    echo "  Site $site done."
    echo ""
done

echo "=== All sites processed ==="
echo ""
echo "Next steps:"
echo "  1. datalad save -d $CONN_DIR -m 'enh: Complete time series extraction'"
echo "  2. sbatch code/analysis_classify.sbatch --project-root $PROJECT_ROOT"
