# Analysis Variants and Results

This document records all experimental variants explored in the replication
of Abraham et al. (2017) "Deriving reproducible biomarkers from multi-site
resting-state data: An Autism-based example" (NeuroImage 147:736-745).

## 1. Overview

We replicate Abraham's functional connectivity biomarker pipeline on ABIDE
I+II data preprocessed with fMRIPrep 25.2.4. The pipeline extracts MSDL
atlas (39 regions) time series, computes tangent embedding (741 features),
and classifies ASD vs TC using RidgeClassifier and SVC with leave-one-site-out
cross-validation.

Three extraction variants and multiple classification configurations were
tested to understand how preprocessing and analysis choices affect accuracy.

## 2. Methods

### 2.0.1 QC Criteria

Constants defined in `code/analysis/_helpers.py`:

| Threshold | Value | Rationale |
|-----------|-------|-----------|
| Maximum mean FD | 0.5 mm | Per Abraham et al. (2017) |
| Minimum usable volumes | 120 | After excluding FD > 0.5 mm frames |
| Minimum atlas coverage | 80% | Fraction of MSDL regions with non-zero signal |

### 2.0.2 Run Selection

When a subject has multiple BOLD runs, the run with the lowest mean FD is selected (from `qc_prescreen.tsv` produced by step 01).

### 2.0.3 Execution Order

```
01_prescreen_qc.py        →  qc_prescreen.tsv
02_extract_timeseries.py   →  per-subject parquets, JSONs, HDF5 (via orchestrate.sh)
03_build_connectomes.py    →  group tangent features (NPZ)
04_classify.py             →  classification results (JSON)
05_visualize.py            →  figures
```

Steps 01-05 form a sequential dependency chain. Steps 06-09 are independent baseline and replication scripts that can run in any order after step 01.

### 2.0.4 Output Schema (BEP017-style)

```
derivatives/connectivity-{variant}/
├── dataset_description.json
├── qc_prescreen.tsv                           # Per-run QC (mean FD, usable vols)
├── sub-{id}/ses-1/func/
│   ├── {stem}_stat-mean_timeseries.parquet    # ROI × time matrix (39 regions)
│   ├── {stem}_stat-mean_timeseries.json       # Sidecar: TR, atlas, confounds, coverage
│   ├── {stem}_stat-coverage_bold.tsv          # Per-region signal coverage fractions
│   └── {stem}_stat-pearsoncorrelation_relmat.h5  # 39×39 correlation matrix
├── group/
│   ├── group_atlas-MSDL_stat-tangent_relmat.npz  # Tangent-embedded group features
│   └── group_atlas-MSDL_stat-tangent_relmat.json  # Metadata (subjects, labels, N)
└── classification/
    └── results_*.json                         # CV accuracy, per-fold scores
```

### 2.0.5 External Data Dependencies

Scripts 06-09 download data at runtime (cached in `$NILEARN_DATA`):

| Resource | URL / Source | Used by |
|----------|-------------|---------|
| C-PAC preprocessed ABIDE I | `nilearn.datasets.fetch_abide_pcp()` | 06, 07 |
| Abraham CV splits | `https://team.inria.fr/parietal/files/2016/04/cv_abide.zip` | 07, 08, 09 |
| MSDL atlas | `nilearn.datasets.fetch_atlas_msdl()` | 02, 03 |

## 2. Time Series Extraction Variants

All variants use the MSDL atlas (39 probabilistic regions) and produce
BEP017-compliant outputs.

### 2.1 v1: Single-stage ROI-level confound regression

The standard nilearn approach. Confounds are regressed at the ROI level
(after spatial averaging) via `NiftiMapsMasker(confounds=..., low_pass=0.1)`.

| Parameter | Value |
|-----------|-------|
| Motion confounds | 24 Friston (`motion="full"`) |
| CompCor | 5 aCompCor (combined WM+CSF mask) |
| High-pass | Cosine basis regressors |
| Low-pass | 0.1 Hz Butterworth (in masker) |
| Standardize | `zscore_sample` |
| Cleaning stage | ROI-level only |

### 2.2 v2: v1 + high-variance voxel confounds

Adds 5 PCs from the 2% highest-variance voxels (within brain mask) as
additional confound regressors, following Abraham Section 2.3.

| Additional parameter | Value |
|---------------------|-------|
| High-variance PCs | 5 (top 2% voxel variance) |

### 2.3 v3: Two-stage denoising

Replicates Abraham's full denoising pipeline:
- **Stage 1 (voxel-level):** `clean_img()` with 24 motion + 5 aCompCor +
  cosine HP + band-pass 0.01-0.1 Hz. Mimics C-PAC voxel-level cleaning.
- **Stage 2 (ROI-level):** Extract MSDL time series from clean BOLD, then
  regress 5 tCompCor (fMRIPrep temporal CompCor) from ROI signals.

## 3. Classification Approaches

### 3.1 Baseline

- `TangentEmbeddingTransformer` re-fitted per CV fold (no information leakage)
- `RidgeClassifier(alpha=1.0)` and `SVC(kernel="linear", C=1.0)` -- default hyperparameters
- Leave-One-Group-Out CV (one site per fold)
- No group-level confound regression

### 3.2 Variant E (Abraham-faithful)

All fixes identified by IAsser audit of Abraham et al. (2017):

| Fix | Description |
|-----|-------------|
| CV splits | Abraham's exact 10-fold assignments (from `cv_abide.zip`) |
| Confound regression | Group-level regression of site + age + sex from tangent features |
| Hyperparameter tuning | Nested 5-fold GridSearchCV for Ridge alpha / SVC C |
| Covariance estimator | `LedoitWolf(assume_centered=True)` |

## 4. C-PAC Baseline Comparison

### 4.1 Direct baseline (`06_baseline_cpac.py`)

Same classification pipeline applied to C-PAC preprocessed ABIDE I data
from the Preprocessed Connectomes Project (PCP). Data fetched via
`nilearn.datasets.fetch_abide_pcp(quality_checked=True)`.

### 4.2 Ablation study (`07_faithful_replication.py`)

Incremental application of Abraham-faithful fixes on C-PAC data:

| Step | What changes |
|------|-------------|
| A | Baseline (LOGO, defaults) |
| B | + Abraham's 10-fold CV splits |
| C | + Confound regression (site+age+sex) |
| D | + Nested CV hyperparameter tuning |
| E | + LedoitWolf(assume_centered=True) |

## 5. Master Results Table

### Inter-site classification accuracy (unweighted mean across sites/folds)

| Variant | Data | CV | N | Sites | Ridge | SVC |
|---------|------|----|---|-------|-------|-----|
| **Abraham (2017)** | **C-PAC** | **10-fold** | **871** | **16** | **66.8%** | -- |
| | | | | | | |
| *C-PAC baseline* | C-PAC | LOGO-20 | 871 | 20 | 56.9% | 58.5% |
| *Ablation A* | C-PAC | LOGO | 871 | 20 | 56.9% | 58.5% |
| *Ablation B* | C-PAC | Abraham 10-fold | 714 | -- | 59.1% | 58.9% |
| *Ablation C* | C-PAC | +confound reg | 714 | -- | 59.7% | 61.1% |
| *Ablation D* | C-PAC | +tuning | 714 | -- | 63.1% | 65.4% |
| *Ablation E* | C-PAC | +LW centered | 714 | -- | 63.1% | 65.4% |
| | | | | | | |
| *v1 baseline* | fMRIPrep I | LOGO-23 | 879 | 23 | 57.7% | 60.5% |
| *v2 (+HV)* | fMRIPrep I | LOGO-23 | 879 | 23 | 56.5% | 58.1% |
| *v3 (2-stage)* | fMRIPrep I | LOGO-23 | 879 | 23 | 54.4% | 55.3% |
| *v1 + var E* | fMRIPrep I | LOGO-23 | 879 | 23 | 61.3% | 62.7% |
| *v1 + var E* | fMRIPrep I | 10-fold | 582 | -- | 62.9% | 60.4% |
| *v3 + var E* | fMRIPrep I+II | LOGO-36 | 1447 | 36 | 62.2% | 58.0% |
| *v3 exact Abraham* | fMRIPrep I | 10-fold | ~713 | -- | *pending* | *pending* |

### Key findings

1. **Preprocessing pipeline effect is small:** C-PAC baseline (56.9%) vs
   fMRIPrep v1 baseline (57.7%) differ by <1%. The accuracy gap vs Abraham
   is in the **analysis code**, not preprocessing.

2. **Hyperparameter tuning is the largest single factor:** +3.4% from
   ablation C to D (nested CV for Ridge alpha).

3. **CV scheme matters:** Abraham's grouped 10-fold vs pure LOGO changes
   accuracy by ~2%.

4. **More aggressive denoising hurts:** v2 and v3 reduce accuracy compared
   to v1. The two-stage approach (v3) removes discriminative signal.

5. **ABIDE II extension:** Adding ABIDE II (1447 total) maintains accuracy
   (62.2% vs 61.3% for ABIDE I alone with variant E + LOGO).

## 6. Abraham et al. (2017) Reference Values

From Table 2, subsample #1 (all subjects, MSDL atlas, tangent embedding):

| Metric | Value |
|--------|-------|
| Inter-site accuracy (Ridge) | 66.8% (+/- 5.4%) |
| Inter-site specificity | 72.3% |
| Inter-site sensitivity | 61.0% |
| Intra-site accuracy | 66.9% |
| N subjects | 871 |
| N sites | 16 |
| Software | Python 2.7, scikit-learn 0.17.1, nilearn 0.1.5 |

## 7. Software Versions

| Package | Version |
|---------|---------|
| fMRIPrep | 25.2.4 |
| nilearn | 0.13.1 |
| scikit-learn | 1.8.0 |
| pandas | 3.0.1 |
| numpy | 2.4.3 |
| h5py | 3.16.0 |
| Python | 3.12 |

## 8. Reproduction Commands

```bash
# Pre-screen QC
python3 code/analysis/01_prescreen_qc.py --project-root .

# Extract time series (variant v1)
bash code/analysis_orchestrate.sh -C . --variant v1

# Build connectomes + classify + visualize
python3 code/analysis/03_build_connectomes.py --project-root .
python3 code/analysis/04_classify.py --project-root .
python3 code/analysis/05_visualize.py --project-root .

# C-PAC baseline
python3 code/analysis/06_baseline_cpac.py --project-root . --data-dir /path/to/cache

# Faithful replication (ablation A-E on C-PAC)
python3 code/analysis/07_faithful_replication.py --project-root . --data-dir /path/to/cache

# Variant E on fMRIPrep with Abraham's CV
python3 code/analysis/08_fmriprep_variant_e.py --project-root . --data-dir /path/to/cache

# Exact Abraham sample (extracts missing subjects + classifies)
python3 code/analysis/09_exact_abraham_sample.py --project-root . --data-dir /path/to/cache
```
