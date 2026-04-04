# End-to-End Workflow

This document is the single reference for reproducing the entire ABIDE
fMRIPrep experiment — from raw inputs to classification results. For
architecture details, see [CLAUDE.md](../CLAUDE.md). For operational
pitfalls, see [HANDOVER.md](HANDOVER.md).

---

## Prerequisites

1. **Clone the superdataset:**
   ```bash
   datalad clone https://github.com/oesteban/abide_fmriprep.git abide_preproc
   cd abide_preproc
   ```

2. **Install micromamba environments:**
   ```bash
   micromamba create -f env/environment.yml        # DataLad + git-annex
   micromamba create -f env/analysis_environment.yml  # nilearn + sklearn + pandas
   ```

3. **FreeSurfer license:** Place at `env/secrets/fs_license.txt` or pass
   `--fs-license-file /path/to/license.txt` to sbatch.

4. **HPC access:** SLURM cluster with Apptainer (or Docker for local).
   Cluster-specific paths are configured in `code/cluster_config.sh`.

5. **GIN account + SSH keys:** Required for pushing derivatives.

---

## Phase 1: Input Preparation

```bash
# 1. Install input subdatasets (metadata only, no imaging data)
datalad get -n inputs/abide-both inputs/templateflow

# 2. Build the merged BIDS input view
micromamba run -n datalad python3 code/build_abide_both.py --project-root . --sidecars none

# 3. Pre-fetch TemplateFlow templates (required for Apptainer)
cd inputs/templateflow
datalad get -r tpl-MNI152NLin2009cAsym tpl-MNI152NLin6Asym tpl-OASIS30ANTs tpl-fsLR tpl-fsaverage
cd ../..
```

## Phase 2: Site Dataset Creation (one-time)

```bash
# Create 43 site-level derivative datasets + GIN/GitHub siblings
code/create_site_datasets.sh --project-root . --create-siblings --gin-org abide-fmriprep
```

## Phase 3: fMRIPrep Preprocessing

```bash
# 4. Prepare a subjects list (or use an existing one)
#    lists/curnagl-20260307.txt contains all 2,194 subjects
N=$(wc -l < lists/curnagl-20260307.txt)

# 5. Submit fMRIPrep (chunk into ≤1000 tasks per SLURM limit)
sbatch --array=1-1000 code/fmriprep-jobarray.sbatch \
  --project-root /path/to/abide_preproc \
  --container-name fmriprep-apptainer \
  --subjects-file lists/curnagl-20260307.txt \
  --fs-license-file /path/to/freesurfer/license.txt

sbatch --array=1001-$N code/fmriprep-jobarray.sbatch \
  --project-root /path/to/abide_preproc \
  --container-name fmriprep-apptainer \
  --subjects-file lists/curnagl-20260307.txt \
  --fs-license-file /path/to/freesurfer/license.txt

# 6. Reconcile job branches into master (after batch completes)
bash code/reconcile_subdatasets.sh -C . --push

# 7. Audit: verify annex content reached GIN
for site in derivatives/v[12]s*/; do
  code/drop_verified.sh -C "$site" --remote gin
done

# 8. Build the consumption overlay
python3 code/build_derivatives_overlay.py --project-root .
datalad push -d derivatives/fmriprep-25.2 --to origin
```

**Resubmit failures:** Build a new subjects list from audit output,
resubmit with `--subjects-file`, reconcile, audit again. See
[HANDOVER.md](HANDOVER.md) §1.1 for ghost subject handling.

## Phase 4: Analysis Pipeline

All analysis commands use `--project-root .` and require the
`abide-analysis` micromamba environment.

```bash
# 9. QC pre-screen (produces qc_prescreen.tsv)
sbatch code/analysis_prescreen.sbatch -C /path/to/abide_preproc --variant v1

# 10. Time-series extraction (orchestrated site-by-site to manage disk)
bash code/analysis_orchestrate.sh -C /path/to/abide_preproc --variant v1

# 11. Build connectomes + classify + visualize
sbatch code/analysis_classify.sbatch -C /path/to/abide_preproc --variant v1
# Or manually:
python3 code/analysis/03_build_connectomes.py --project-root .
python3 code/analysis/04_classify.py --project-root .
python3 code/analysis/05_visualize.py --project-root .
```

## Phase 5: Baseline Comparisons

```bash
# 12. C-PAC baseline (downloads PCP data via nilearn)
sbatch code/analysis_baseline_cpac.sbatch -C /path/to/abide_preproc
# Or: python3 code/analysis/06_baseline_cpac.py --project-root . --data-dir /path/to/cache

# 13. Abraham ablation study (A-E on C-PAC data)
python3 code/analysis/07_faithful_replication.py --project-root . --data-dir /path/to/cache

# 14. Variant E on fMRIPrep (Abraham-faithful fixes)
python3 code/analysis/08_fmriprep_variant_e.py --project-root . --data-dir /path/to/cache

# 15. Exact Abraham sample match
python3 code/analysis/09_exact_abraham_sample.py --project-root . --data-dir /path/to/cache
```

---

## Adapting for a Different Cluster

1. Edit `code/cluster_config.sh` — add a `case` entry for your cluster
   with the correct conda/mamba environment paths and module names.
2. Ensure `DATALAD_ENV` points to a directory containing `datalad`,
   `git-annex`, and `python3`.
3. Ensure `ANALYSIS_ENV` points to a directory containing `python3` with
   nilearn, scikit-learn, pandas, etc. (see `env/analysis_environment.yml`).
4. Set `NILEARN_DATA` to a writable cache directory for atlas downloads.

---

## AI Assistant Prompt Template

To reproduce this experiment with an AI assistant + HPC skill:

```
Clone the ABIDE fMRIPrep superdataset and preprocess all subjects.

Repository: https://github.com/oesteban/abide_fmriprep.git
Pipeline: fMRIPrep 25.2.4 (container already registered)
Layout: YODA superdataset with 43 site-level derivative datasets

Read docs/WORKFLOW.md, docs/HANDOVER.md, and CLAUDE.md before acting.

Steps:
1. Clone to HPC scratch, install inputs, pre-fetch TemplateFlow
2. Create site datasets (if not already done)
3. Submit fMRIPrep via SLURM array (--subjects-file, chunks ≤1000)
4. Reconcile, audit, resubmit failures
5. Build the consumption overlay
6. Run analysis pipeline: prescreen → extract (v1) → classify → visualize
7. Run baselines: C-PAC, Abraham ablation, variant E, exact sample

Critical pitfalls (see docs/HANDOVER.md):
- Site subdatasets cloned from GitHub lack a 'gin' remote
- Ghost subjects: git ls-tree sees them but annex content is lost
- Cluster paths: edit code/cluster_config.sh for your HPC
```
