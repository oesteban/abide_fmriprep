#!/usr/bin/env bash
# Initialize the derivatives/connectivity subdataset.
#
# Usage (from repo root):
#   bash code/analysis/setup_connectivity_dataset.sh [--project-root .]
#
# Prerequisites: datalad environment activated (micromamba activate datalad)

set -euo pipefail

PROJECT_ROOT="${1:-.}"
CONN_DIR="$PROJECT_ROOT/derivatives/connectivity"

if [[ -d "$CONN_DIR/.datalad" ]]; then
  echo "[INFO] derivatives/connectivity already initialized, skipping datalad create."
else
  echo "[INFO] Creating derivatives/connectivity subdataset..."
  datalad create -d "$PROJECT_ROOT" "$CONN_DIR"
fi

# -- dataset_description.json --
cat > "$CONN_DIR/dataset_description.json" <<'DDJSON'
{
  "Name": "ABIDE Functional Connectivity (MSDL Atlas)",
  "BIDSVersion": "1.9.0",
  "DatasetType": "derivative",
  "GeneratedBy": [
    {
      "Name": "abide_analysis_pipeline",
      "Description": "Replication of Abraham et al. 2017 functional connectivity biomarkers using fMRIPrep-preprocessed ABIDE I+II data.",
      "CodeURL": "https://github.com/oesteban/abide_fmriprep"
    }
  ],
  "SourceDatasets": [
    {
      "URL": "derivatives/fmriprep-25.2",
      "Version": "fMRIPrep 25.2.4"
    }
  ]
}
DDJSON

# -- .gitattributes --
cat > "$CONN_DIR/.gitattributes" <<'GITATTR'
# Metadata / small text -> git
*.json annex.largefiles=nothing
*.tsv annex.largefiles=nothing
*.csv annex.largefiles=nothing
*.txt annex.largefiles=nothing
*.md annex.largefiles=nothing
*.yml annex.largefiles=nothing
*.yaml annex.largefiles=nothing
*.bib annex.largefiles=nothing

# Binary / large data -> annex
*.parquet annex.largefiles=anything
*.h5 annex.largefiles=anything
*.npz annex.largefiles=anything
*.npy annex.largefiles=anything
*.nii.gz annex.largefiles=anything
*.png annex.largefiles=anything
*.svg annex.largefiles=anything
GITATTR

# -- .bidsignore --
cat > "$CONN_DIR/.bidsignore" <<'BIDSIGNORE'
figures/
classification/
extraction_report.tsv
phenotypic_summary.json
BIDSIGNORE

# -- Save the initial configuration --
datalad save -d "$CONN_DIR" \
  -m "Initialize connectivity derivatives dataset (BEP017 layout)" \
  dataset_description.json .gitattributes .bidsignore

echo "[INFO] derivatives/connectivity initialized."
echo "[INFO] Next steps:"
echo "  1. Run: python code/analysis/00_build_phenotypic.py"
echo "  2. Submit: sbatch --array=1-N code/analysis_ARRAY.sbatch.sh --project-root ."
echo "  3. Run: python code/analysis/02_build_connectomes.py"
echo "  4. Run: python code/analysis/03_classify.py"
echo "  5. Run: python code/analysis/04_visualize.py"
