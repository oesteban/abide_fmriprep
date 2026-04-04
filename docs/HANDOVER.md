# HANDOVER.md — Operational Playbook for ABIDE fMRIPrep Replication

This document captures project-specific operational knowledge from
preprocessing ~2,194 ABIDE subjects with fMRIPrep 25.2.4 on HPC.

For architecture, commands, dataset layout, and general workflow, see
[CLAUDE.md](../CLAUDE.md). For cluster-specific rules (login node
restrictions, SLURM accounting, quotas), see the HPC skill.

---

## 1. Pitfalls Catalog

### 1.1 Ghost subjects: metadata on master, annex content lost

**Symptom:** `git ls-tree master` shows `sub-XXX/` in a site dataset, but
`git annex whereis` reports content only at ephemeral compute-node paths
(e.g., `oesteban@dna079:/tmp/59417494/.../derivatives/v1s12`). `datalad get`
fails because no reachable remote holds the data.

**Root cause:** The fMRIPrep job completed and the `containers-run` commit
landed on the job branch, but the push to GIN failed (network, race, timeout).
The rescue-to-`$SCRATCH` mechanism may not have triggered, or the rescued repo
was cleaned up before recovery. During reconciliation, the job branch was merged
into master (the git commit exists), but the annex content never left the
node-local tmpdir.

**Detection:**
```bash
# Inside a site dataset — find subjects with zero copies on any reachable remote
for sub in $(git ls-tree --name-only master | grep '^sub-' | grep -v '\.html$'); do
  missing=$(git annex find --in here --not --copies-of web -- "$sub/" 2>/dev/null \
    | wc -l)
  [[ "$missing" -gt 0 ]] && echo "$sub: $missing files with no remote copy"
done
```

Or use `code/drop_verified.sh` which reports subjects missing on a given remote.

**Fix:** Remove the ghost entries from master and reprocess:
```bash
git rm -r sub-XXX sub-XXX.html sourcedata/freesurfer/sub-XXX_ses-*
git commit -m "fix: remove ghost subject sub-XXX (annex content lost)"
```
Then push the cleaned master and resubmit fMRIPrep for those subjects.

**Prevention:** The skip check in `fmriprep-jobarray.sbatch` (`list_done_subjects`)
uses `git ls-tree --name-only master` — it checks directory existence in the git
tree, not annex content availability. The pragmatic approach is to audit after
reconciliation using `drop_verified.sh`.

### 1.2 GIN remote missing in cloned subdatasets

**Symptom:** `datalad push --to gin` fails with
`Unknown push target 'gin'. Known targets: 'origin'`.

**Root cause:** Site subdatasets in `.gitmodules` point to GitHub (HTTPS) as
their `origin`. When DataLad installs a subdataset (`datalad get -n`), it clones
from `origin` (GitHub). The `gin` remote must be added explicitly.

**Fix:**
```bash
git -C derivatives/<site> remote add gin "git@gin.g-node.org:/abide-fmriprep/<site>.git"
```

This doesn't bite during fMRIPrep jobs (the sbatch script adds `gin`
automatically), but ad-hoc operations on the persistent superdataset
(manual pushes, reconciliation) need the remote added first.

### 1.3 Stale RIA store references

Harmless `[INFO] RIA store unavailable` warnings appear in stderr during
DataLad clones on compute nodes. The superdataset references a Calypso-era
RIA store path that doesn't exist on Curnagl. The `fmriprep-jobarray.sbatch`
script already marks it as dead. The warnings are cosmetic.

### 1.4 FreeSurfer license not at default path

The default license path (`env/secrets/fs_license.txt`) may not exist on
the cluster. Always pass `--fs-license-file` explicitly when submitting jobs.

---

## 2. Replication Playbook

See [CLAUDE.md](../CLAUDE.md) for full command reference. This section
provides the sequential operator workflow.

### Prerequisites

1. Clone the superdataset and install the DataLad environment (see CLAUDE.md)
2. HPC access with SLURM, Apptainer, and outbound SSH from compute nodes
3. GIN account + SSH key + organization for site repos
4. FreeSurfer license on a shared filesystem path

### Workflow

1. **Prepare inputs:** `datalad get -n inputs/abide-both`, pre-fetch TemplateFlow
2. **Create site datasets** (one-time): `code/create_site_datasets.sh --project-root . --create-siblings --gin-org abide-fmriprep`
3. **Prepare subjects list** in `lists/`, commit, push to origin, pull on HPC
4. **Submit fMRIPrep:** `sbatch --array=1-N code/fmriprep-jobarray.sbatch --project-root ... --subjects-file ... --container-name fmriprep-apptainer --fs-license-file ...`
   - Chunk arrays into ≤1000 tasks (SLURM limit)
5. **Reconcile:** `code/reconcile_subdatasets.sh -C . --push`
6. **Audit:** `code/drop_verified.sh -C derivatives/<site> --remote gin` — any "skipped (missing on gin)" subjects are ghosts (see §1.1)
7. **Resubmit failures** — build new subjects list, resubmit
8. **Build overlay:** `python3 code/build_derivatives_overlay.py --project-root .` then push `derivatives/fmriprep-25.2` to origin

---

## 3. Current Status (as of 2026-04-03)

### 3.1 Preprocessing: 4 ghost subjects

Four subjects have lost all fMRIPrep derivatives due to failed GIN pushes
(see §1.1). Their git commits reached site dataset master branches via
reconciliation, but the annex content only ever existed on ephemeral
compute-node tmpdirs that have since been purged.

| Participant ID | Site dataset | Site |
|----------------|-------------|------|
| `sub-v1s12x0050109` | `derivatives/v1s12` | Pitt |
| `sub-v1s12x0050113` | `derivatives/v1s12` | Pitt |
| `sub-v1s13x0050005` | `derivatives/v1s13` | SBL |
| `sub-v2s10x28995` | `derivatives/v2s10` | ABIDE II, Olin |

**Status:** These subjects must be removed from their site dataset master
branches and reprocessed. The `fmriprep-jobarray.sbatch` skip check sees them
as "done" because their directories exist in the git tree.

**To fix:**
```bash
# In each site dataset, remove the ghost entries:
git -C derivatives/v1s12 rm -r sub-v1s12x0050109 sub-v1s12x0050109.html \
  sourcedata/freesurfer/sub-v1s12x0050109_ses-1
git -C derivatives/v1s12 rm -r sub-v1s12x0050113 sub-v1s12x0050113.html \
  sourcedata/freesurfer/sub-v1s12x0050113_ses-1
git -C derivatives/v1s12 commit -m "fix: remove ghost subjects (annex content lost)"

git -C derivatives/v1s13 rm -r sub-v1s13x0050005 sub-v1s13x0050005.html \
  sourcedata/freesurfer/sub-v1s13x0050005_ses-1
git -C derivatives/v1s13 commit -m "fix: remove ghost subject (annex content lost)"

git -C derivatives/v2s10 rm -r sub-v2s10x28995 sub-v2s10x28995.html \
  sourcedata/freesurfer/sub-v2s10x28995_ses-1
git -C derivatives/v2s10 commit -m "fix: remove ghost subject (annex content lost)"

# Push cleaned master, then resubmit fMRIPrep for the 4 subjects
```

### 3.2 Analysis: Abraham et al. (2017) replication

The project replicates and extends the functional connectivity classification
from Abraham et al. (2017) "Deriving reproducible biomarkers from multi-site
resting-state data" (NeuroImage 147:736–745). The original study reported
66.8% inter-site accuracy on ABIDE I (871 subjects, 16 sites) using C-PAC
preprocessing, MSDL atlas tangent connectivity, and RidgeClassifier.

Full results and methodology are in [ANALYSIS.md](ANALYSIS.md). Below is a
narrative summary of the iterative approach.

#### Phase 1: Initial fMRIPrep baseline (LOGO CV)

We first applied the Abraham pipeline (MSDL atlas, tangent embedding,
RidgeClassifier with default hyperparameters) to fMRIPrep-preprocessed
ABIDE I data using leave-one-site-out (LOGO) cross-validation. Three
extraction variants were tested — single-stage ROI confound regression (v1),
added high-variance confounds (v2), and two-stage voxel+ROI denoising (v3).

Results showed baseline accuracy around 55–58%, substantially below
Abraham's 66.8%. More aggressive denoising (v2, v3) hurt rather than helped,
suggesting that the accuracy gap was not in the preprocessing but in the
analysis code.

#### Phase 2: C-PAC baseline to isolate the gap

To disentangle preprocessing effects from analysis-code effects, we ran the
same classification pipeline on the original C-PAC preprocessed data (from
nilearn's `fetch_abide_pcp`). The C-PAC baseline yielded 56.9% (Ridge) /
58.5% (SVC) with LOGO CV — nearly identical to the fMRIPrep baseline (57.7%).

**Key finding:** The preprocessing pipeline (C-PAC vs fMRIPrep) accounts for
less than 1% of accuracy difference. The ~10% gap vs Abraham is entirely in
the analysis methodology.

#### Phase 3: Ablation study on C-PAC data

Using C-PAC data, we incrementally applied the methodological choices from
Abraham (2017) to identify which factors close the gap. Steps:

| Step | Change | Ridge | SVC |
|------|--------|-------|-----|
| A | Baseline (LOGO, defaults) | 56.9% | 58.5% |
| B | + Abraham's exact 10-fold CV splits | 59.1% | 58.9% |
| C | + Group-level confound regression (site+age+sex) | 59.7% | 61.1% |
| D | + Nested CV hyperparameter tuning | 63.1% | 65.4% |
| E | + LedoitWolf(assume_centered=True) | 63.1% | 65.4% |

**Key finding:** Hyperparameter tuning (step D) is the single largest factor,
accounting for +3.4% (Ridge). The CV scheme (LOGO vs Abraham's grouped
10-fold) adds ~2%. Covariance estimator choice (step E) has no measurable
effect.

#### Phase 4: fMRIPrep + full Abraham methodology ("Variant E")

Applying all Abraham-faithful fixes (exact CV folds, confound regression,
nested tuning, LedoitWolf) to fMRIPrep-preprocessed ABIDE I data:

- LOGO-23 CV: 61.3% (Ridge) / 62.7% (SVC)
- Abraham 10-fold CV: 62.9% (Ridge) / 60.4% (SVC)

This closes the gap from the raw baseline (~57%) to within ~4% of Abraham's
66.8%, with the residual likely attributable to software version differences
(Python 2.7 / scikit-learn 0.17.1 / nilearn 0.1.5 vs modern stack).

#### Phase 5: Extension to full ABIDE I+II

Extending to all available ABIDE I+II subjects (1,447 subjects, 36 sites)
with variant E + v3 extraction + LOGO CV yields 62.2% (Ridge) / 58.0% (SVC).
Accuracy is maintained despite the larger, more heterogeneous sample.

#### Summary

| Configuration | Data | N | Accuracy (Ridge) |
|--------------|------|---|------------------|
| Abraham (2017) reported | C-PAC, ABIDE I | 871 | **66.8%** |
| C-PAC baseline (LOGO) | C-PAC, ABIDE I | 871 | 56.9% |
| C-PAC + all Abraham fixes | C-PAC, ABIDE I | 714 | 63.1% |
| fMRIPrep v1 baseline (LOGO) | fMRIPrep, ABIDE I | 879 | 57.7% |
| fMRIPrep + all Abraham fixes | fMRIPrep, ABIDE I | 879 | 61.3% |
| fMRIPrep + Abraham fixes, ABIDE I+II | fMRIPrep, ABIDE I+II | 1,447 | 62.2% |

The replication demonstrates that (a) the preprocessing choice between C-PAC
and fMRIPrep has minimal effect on classification accuracy, (b) the accuracy
gap vs. the original paper is driven by analysis methodology (hyperparameter
tuning, CV scheme, confound regression), and (c) the biomarker generalizes
to the combined ABIDE I+II sample.

---

## 4. AI Assistant Prompt Template

To replicate this experiment with Claude Code + an HPC skill:

```
Clone the ABIDE fMRIPrep superdataset and preprocess all subjects.

Repository: https://github.com/oesteban/abide_fmriprep.git
Pipeline: fMRIPrep 25.2.4 (container already registered)
Layout: YODA superdataset with 43 site-level derivative datasets

Read docs/HANDOVER.md and CLAUDE.md before taking any action.

Steps:
1. Clone to HPC scratch
2. Install inputs (abide-both metadata, templateflow assets)
3. Create a subjects list from participants.tsv
4. Submit fMRIPrep via SLURM array (code/fmriprep-jobarray.sbatch)
   - Use --subjects-file, --container-name fmriprep-apptainer
   - Pass --fs-license-file explicitly
   - Chunk arrays into <=1000 tasks
5. After each batch: reconcile with code/reconcile_subdatasets.sh -C . --push
6. Audit with code/drop_verified.sh to find ghost subjects
7. Resubmit failures
8. Build the consumption overlay with code/build_derivatives_overlay.py

CRITICAL project-specific pitfalls (see docs/HANDOVER.md):
- Site subdatasets cloned from GitHub lack a 'gin' remote — add it before pushing
- The skip check uses git ls-tree (metadata), not annex whereis (content) —
  ghost subjects will be skipped unless removed from master first
- After reconciliation, verify annex content reached GIN before declaring done
```
