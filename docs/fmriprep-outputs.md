# fMRIPrep derivatives: from monolithic dataset to site-level subdatasets

## Motivation

The original architecture stored all fMRIPrep and FreeSurfer outputs for ~2,194
subjects in a single DataLad/git-annex dataset (`derivatives/fmriprep-25.2`).
Every SLURM job cloned this dataset, and clone times grew with each processed
subject as the git history accumulated commits and annex metadata.

Two alternative decompositions were considered:

- **Per-subject subdatasets** (~4,400 entries: one fMRIPrep + one FreeSurfer per
  subject).
  Rejected because `.gitmodules` with thousands of entries degrades
  `git status` and `datalad get -n` performance at the superdataset level.

- **Per-site subdatasets** (43 entries: 24 ABIDE I sites + 19 ABIDE II sites).
  Each site dataset is a self-contained fMRIPrep BIDS derivatives root.
  Subject outputs live as regular directories inside the site dataset (no nested
  subdatasets).
  Adopted as the final architecture.

## Tree changes

### Before

```
derivatives/
└── fmriprep-25.2/           ← single monolithic DataLad dataset (~2,194 subjects)
    ├── sub-v1s0x0050642/
    ├── sub-v1s0x0050646/
    ├── ...
    ├── sourcedata/freesurfer/
    │   ├── fsaverage/
    │   ├── sub-v1s0x0050642_ses-1/
    │   └── ...
    ├── dataset_description.json
    └── participants.tsv
```

### After

```
derivatives/
├── _legacy-fmriprep-25.2/   ← renamed submodule (old monolithic dataset, read-only archive)
├── v1s0/                     ← ABIDE I, CMU_a (site-level DataLad dataset)
│   ├── sub-v1s0x0050642/     ← regular directory (not a subdataset)
│   ├── sub-v1s0x0050646/
│   ├── sourcedata/freesurfer/
│   ├── dataset_description.json
│   └── .bidsignore
├── v1s1/                     ← ABIDE I, CMU_b
├── ...
├── v1s23/                    ← ABIDE I, Yale
├── v2s0/                     ← ABIDE II, BNI_1
├── ...
└── v2s18/                    ← ABIDE II, USM_1
```

The old monolithic dataset was renamed in-place via git index manipulation
(`git rm --cached` + `git update-index --add --cacheinfo 160000,...`) to avoid
re-cloning the large repository.
The submodule key in `.gitmodules` retains the original name
(`submodule "derivatives/fmriprep-25.2"`) while the path points to the new
location (`derivatives/_legacy-fmriprep-25.2`).

## Repository URLs

### Legacy dataset

| Platform | URL |
|----------|-----|
| GitHub | <https://github.com/oesteban/abide-fmriprep-derivatives> |
| GIN | <https://gin.g-node.org/oesteban/abide-fmriprep> |

### Site-level datasets

Each site dataset has two remotes, hosted in the `abide-fmriprep` organizations
(see next section).
The `.gitmodules` URL points to GitHub HTTPS for portability; the GIN remote
is configured locally with SSH push URLs.

| Site prefix | Dataset | Site name | GitHub | GIN |
|-------------|---------|-----------|--------|-----|
| `v1s0` | ABIDE I | CMU_a | <https://github.com/abide-fmriprep/v1s0> | <https://gin.g-node.org/abide-fmriprep/v1s0> |
| `v1s1` | ABIDE I | CMU_b | <https://github.com/abide-fmriprep/v1s1> | <https://gin.g-node.org/abide-fmriprep/v1s1> |
| `v1s2` | ABIDE I | Caltech | <https://github.com/abide-fmriprep/v1s2> | <https://gin.g-node.org/abide-fmriprep/v1s2> |
| `v1s3` | ABIDE I | KKI | <https://github.com/abide-fmriprep/v1s3> | <https://gin.g-node.org/abide-fmriprep/v1s3> |
| `v1s4` | ABIDE I | Leuven_1 | <https://github.com/abide-fmriprep/v1s4> | <https://gin.g-node.org/abide-fmriprep/v1s4> |
| `v1s5` | ABIDE I | Leuven_2 | <https://github.com/abide-fmriprep/v1s5> | <https://gin.g-node.org/abide-fmriprep/v1s5> |
| `v1s6` | ABIDE I | MaxMun_a | <https://github.com/abide-fmriprep/v1s6> | <https://gin.g-node.org/abide-fmriprep/v1s6> |
| `v1s7` | ABIDE I | MaxMun_b | <https://github.com/abide-fmriprep/v1s7> | <https://gin.g-node.org/abide-fmriprep/v1s7> |
| `v1s8` | ABIDE I | MaxMun_c | <https://github.com/abide-fmriprep/v1s8> | <https://gin.g-node.org/abide-fmriprep/v1s8> |
| `v1s9` | ABIDE I | MaxMun_d | <https://github.com/abide-fmriprep/v1s9> | <https://gin.g-node.org/abide-fmriprep/v1s9> |
| `v1s10` | ABIDE I | NYU | <https://github.com/abide-fmriprep/v1s10> | <https://gin.g-node.org/abide-fmriprep/v1s10> |
| `v1s11` | ABIDE I | OHSU | <https://github.com/abide-fmriprep/v1s11> | <https://gin.g-node.org/abide-fmriprep/v1s11> |
| `v1s12` | ABIDE I | Olin | <https://github.com/abide-fmriprep/v1s12> | <https://gin.g-node.org/abide-fmriprep/v1s12> |
| `v1s13` | ABIDE I | Pitt | <https://github.com/abide-fmriprep/v1s13> | <https://gin.g-node.org/abide-fmriprep/v1s13> |
| `v1s14` | ABIDE I | SBL | <https://github.com/abide-fmriprep/v1s14> | <https://gin.g-node.org/abide-fmriprep/v1s14> |
| `v1s15` | ABIDE I | SDSU | <https://github.com/abide-fmriprep/v1s15> | <https://gin.g-node.org/abide-fmriprep/v1s15> |
| `v1s16` | ABIDE I | Stanford | <https://github.com/abide-fmriprep/v1s16> | <https://gin.g-node.org/abide-fmriprep/v1s16> |
| `v1s17` | ABIDE I | Trinity | <https://github.com/abide-fmriprep/v1s17> | <https://gin.g-node.org/abide-fmriprep/v1s17> |
| `v1s18` | ABIDE I | UCLA_1 | <https://github.com/abide-fmriprep/v1s18> | <https://gin.g-node.org/abide-fmriprep/v1s18> |
| `v1s19` | ABIDE I | UCLA_2 | <https://github.com/abide-fmriprep/v1s19> | <https://gin.g-node.org/abide-fmriprep/v1s19> |
| `v1s20` | ABIDE I | UM_1 | <https://github.com/abide-fmriprep/v1s20> | <https://gin.g-node.org/abide-fmriprep/v1s20> |
| `v1s21` | ABIDE I | UM_2 | <https://github.com/abide-fmriprep/v1s21> | <https://gin.g-node.org/abide-fmriprep/v1s21> |
| `v1s22` | ABIDE I | USM | <https://github.com/abide-fmriprep/v1s22> | <https://gin.g-node.org/abide-fmriprep/v1s22> |
| `v1s23` | ABIDE I | Yale | <https://github.com/abide-fmriprep/v1s23> | <https://gin.g-node.org/abide-fmriprep/v1s23> |
| `v2s0` | ABIDE II | BNI_1 | <https://github.com/abide-fmriprep/v2s0> | <https://gin.g-node.org/abide-fmriprep/v2s0> |
| `v2s1` | ABIDE II | EMC_1 | <https://github.com/abide-fmriprep/v2s1> | <https://gin.g-node.org/abide-fmriprep/v2s1> |
| `v2s2` | ABIDE II | ETHZ_1 | <https://github.com/abide-fmriprep/v2s2> | <https://gin.g-node.org/abide-fmriprep/v2s2> |
| `v2s3` | ABIDE II | GU_1 | <https://github.com/abide-fmriprep/v2s3> | <https://gin.g-node.org/abide-fmriprep/v2s3> |
| `v2s4` | ABIDE II | IP_1 | <https://github.com/abide-fmriprep/v2s4> | <https://gin.g-node.org/abide-fmriprep/v2s4> |
| `v2s5` | ABIDE II | IU_1 | <https://github.com/abide-fmriprep/v2s5> | <https://gin.g-node.org/abide-fmriprep/v2s5> |
| `v2s6` | ABIDE II | KKI_1 | <https://github.com/abide-fmriprep/v2s6> | <https://gin.g-node.org/abide-fmriprep/v2s6> |
| `v2s7` | ABIDE II | KUL_3 | <https://github.com/abide-fmriprep/v2s7> | <https://gin.g-node.org/abide-fmriprep/v2s7> |
| `v2s8` | ABIDE II | NYU_1 | <https://github.com/abide-fmriprep/v2s8> | <https://gin.g-node.org/abide-fmriprep/v2s8> |
| `v2s9` | ABIDE II | NYU_2 | <https://github.com/abide-fmriprep/v2s9> | <https://gin.g-node.org/abide-fmriprep/v2s9> |
| `v2s10` | ABIDE II | OHSU_1 | <https://github.com/abide-fmriprep/v2s10> | <https://gin.g-node.org/abide-fmriprep/v2s10> |
| `v2s11` | ABIDE II | ONRC_2 | <https://github.com/abide-fmriprep/v2s11> | <https://gin.g-node.org/abide-fmriprep/v2s11> |
| `v2s12` | ABIDE II | SDSU_1 | <https://github.com/abide-fmriprep/v2s12> | <https://gin.g-node.org/abide-fmriprep/v2s12> |
| `v2s13` | ABIDE II | TCD_1 | <https://github.com/abide-fmriprep/v2s13> | <https://gin.g-node.org/abide-fmriprep/v2s13> |
| `v2s14` | ABIDE II | UCD_1 | <https://github.com/abide-fmriprep/v2s14> | <https://gin.g-node.org/abide-fmriprep/v2s14> |
| `v2s15` | ABIDE II | UCLA_1 | <https://github.com/abide-fmriprep/v2s15> | <https://gin.g-node.org/abide-fmriprep/v2s15> |
| `v2s16` | ABIDE II | UCLA_Long | <https://github.com/abide-fmriprep/v2s16> | <https://gin.g-node.org/abide-fmriprep/v2s16> |
| `v2s17` | ABIDE II | UPSM_Long | <https://github.com/abide-fmriprep/v2s17> | <https://gin.g-node.org/abide-fmriprep/v2s17> |
| `v2s18` | ABIDE II | USM_1 | <https://github.com/abide-fmriprep/v2s18> | <https://gin.g-node.org/abide-fmriprep/v2s18> |

## Organizations

Two new organizations were created to host the 43 site-level repositories:

| Platform | Organization | URL |
|----------|-------------|-----|
| GitHub | `abide-fmriprep` | <https://github.com/abide-fmriprep> |
| GIN | `abide-fmriprep` | <https://gin.g-node.org/abide-fmriprep> |

These organizations group all derivative repositories under a single namespace,
keeping the personal GitHub/GIN accounts clean and making it straightforward to
grant collaborator access to the full collection.

## Creating the site datasets and siblings

### Step 1 — Local DataLad datasets

The script `code/create_site_datasets.sh` reads site prefixes from
`inputs/abide-both/participants.tsv` and creates one DataLad subdataset per site
under `derivatives/`.
Each dataset is initialized with `cfg_fmriprep` (`.gitattributes` for metadata
in git, imaging in annex), a `dataset_description.json`, a `.bidsignore`, and a
`sourcedata/freesurfer/` placeholder.

```bash
bash code/create_site_datasets.sh --project-root .
```

### Step 2 — Credential setup

#### GIN

DataLad's `create-sibling-gin` looks up tokens via the `--credential` flag.
The credential name must not contain dots, so the short name `gin` was used:

```bash
datalad credentials set gin type=token secret=****************************
```

The command `--credential gin` is then passed to `create-sibling-gin`.

#### GitHub

DataLad's `create-sibling-github` defaults to looking up a credential named
after the API host (`api.github.com`).
Setting a credential under that name did not work — the dotted name prevented
token retrieval.
The workaround was to set a credential under the short name `github`:

```bash
datalad credentials set github type=token secret=ghp_************************************AEek
```

However, the stored credential was still not picked up by
`create-sibling-github --credential github` due to residual legacy
`user_password` properties on the same credential name.
The final working approach was to pass the token via environment variable:

```bash
export DATALAD_CREDENTIAL_GITHUB_TOKEN=ghp_************************************AEek
```

With the environment variable set, `create-sibling-github` authenticated
successfully.

### Step 3 — Creating siblings

Both GIN and GitHub siblings use `https-ssh` access protocol: HTTPS URLs for
anonymous reads, SSH push URLs for authenticated writes.
The GitHub sibling declares `--publish-depends gin` so that pushing to GitHub
first pushes annex content to GIN.

Each repository's `--description` was set at creation time to indicate
provenance (e.g., `"fMRIPrep 25.2 derivatives — ABIDE I / CMU_a"`).
The GitHub descriptions were additionally set via `gh api` for the repos that
were created before the `--description` flag was added to the workflow.

**GIN sibling** (per-site dataset):

```bash
datalad create-sibling-gin -d derivatives/<site> \
    --name gin --access-protocol https-ssh \
    --existing skip --credential gin \
    --description "fMRIPrep 25.2 derivatives — <DATASET> / <SITE_NAME>" \
    abide-fmriprep/<site>
```

**GitHub sibling** (per-site dataset, with `DATALAD_CREDENTIAL_GITHUB_TOKEN` set):

```bash
datalad create-sibling-github -d derivatives/<site> \
    --name github --access-protocol https-ssh \
    --existing skip --credential github \
    --publish-depends gin \
    abide-fmriprep/<site>
```

**Initial push** (per-site dataset):

```bash
datalad push -d derivatives/<site> --to gin --data anything
datalad push -d derivatives/<site> --to github
```

After all siblings were created and pushed, the `.gitmodules` URL for each
site subdataset was updated to the GitHub HTTPS URL for portability:

```bash
git config -f .gitmodules "submodule.derivatives/<site>.url" \
    "https://github.com/abide-fmriprep/<site>.git"
```

The full process (43 GIN repos + 43 GitHub repos + initial pushes) is automated
by `code/create_site_datasets.sh --create-siblings`.

## SLURM job workflow and reconciliation

### How jobs produce results

Each SLURM array task (submitted via `code/fmriprep-jobarray.sbatch`) processes
a single subject.
The job clones the superdataset to `$SLURM_TMPDIR`, installs the relevant site
subdataset (`datalad get -n derivatives/<site_prefix>`), creates a dedicated
branch (`job/sub-<id>`), runs fMRIPrep via `datalad containers-run --explicit`,
and pushes the job branch + annex content to the site's GIN remote.

Key properties:

- **One branch per subject:** `job/sub-v1s0x0050642`, `job/sub-v1s0x0050646`, etc.
- **Disjoint outputs:** `--explicit` output declarations limit each job to its
  own subject directory, HTML report, and FreeSurfer session directory.
  Shared files (`dataset_description.json`, `CITATION.*`, `fsaverage/`) are
  excluded, preventing merge conflicts.
- **Timing metadata:** The sbatch script records `fmriprep_start=` and
  `fmriprep_stop=` ISO 8601 timestamps in the SLURM log.

### Reconciling job branches (`code/reconcile_subdatasets.sh`)

After a batch of SLURM jobs completes, all job branches must be merged into
each site dataset's `master`.
The `code/reconcile_subdatasets.sh` script automates this.

#### Usage

```bash
# Dry run across all 43 sites
bash code/reconcile_subdatasets.sh -C . --dry-run

# Reconcile a single site
bash code/reconcile_subdatasets.sh -C . --site v1s0

# Full reconciliation with push and branch cleanup
bash code/reconcile_subdatasets.sh -C . --push

# Multiple site filters
bash code/reconcile_subdatasets.sh -C . --site v1s0 --site v2s6 --push
```

#### Flags

| Flag | Default | Purpose |
|------|---------|---------|
| `-C <path>` | `.` | Superdataset root |
| `--site <prefix>` | all | Filter to specific site(s) (repeatable) |
| `--dry-run` | off | Show what would happen without making changes |
| `--push` | off | Push merged master to `github` (triggers `gin` via publish-depends) |
| `--no-delete-branches` | delete on | Keep merged branches on remote after merging |
| `--remote <name>` | `gin` | Remote to fetch job branches from |
| `--logs-dir <path>` | `<root>/logs` | SLURM logs directory for timing metadata extraction |

#### Processing phases

**Phase 0 — Setup and SLURM log index.**
Validates the superdataset root and builds a subject-to-logfile index from
`logs/fmriprep_*.out` files that contain "fMRIPrep finished successfully".

**Phase 1 — Discover site datasets.**
Globs `derivatives/v[12]s*/` directories that have `.git`, then applies
`--site` filters if any were given.

**Phase 2 — Per-site processing.** For each site dataset:

1. **Fetch** — `git fetch <remote>` to get all job branches.
2. **Discover unmerged branches** — Lists `<remote>/job/*` refs and filters
   out those already ancestor of `master` (via `git merge-base --is-ancestor`).
3. **Extract metadata** — For each unmerged branch:
   - Subject ID from branch name (`gin/job/sub-v1s0x0050642` → `sub-v1s0x0050642`)
   - `stc_ref_time` from `logs/CITATION.md` on the branch
   - Timing with three-level fallback:
     (a) direct `fmriprep_start=`/`fmriprep_stop=` ISO lines from SLURM logs,
     (b) nipype timestamps (`YYMMDD-HH:MM:SS`),
     (c) `fmriprep.toml` directory name (`YYYYMMDD-HHMMSS`)
4. **Octopus merge** — Attempts a single `git merge --no-edit` with all
   unmerged branches at once.
   Because outputs are disjoint (each branch only touches its own subject
   directory), this succeeds in the common case.
   If it fails (e.g., CITATION files were accidentally committed), the script
   aborts and falls back to sequential merges with CITATION conflict resolution
   (keeps master's `logs/CITATION.*`).
   Non-CITATION conflicts abort that individual branch and report failure.
5. **Update `participants.tsv`** — Merges existing TSV with extracted metadata
   (new data wins for duplicate subjects).
   Writes a `participants.json` sidecar describing the columns.
   Commits both files within the site dataset.
6. **Push** (if `--push`) — `datalad push --to github`, which triggers GIN
   first via the `--publish-depends gin` configuration.
7. **Delete merged branches** (unless `--no-delete-branches`) — Pushes delete
   refspecs to the remote and prunes stale tracking refs.

**Phase 3 — Save superdataset.**
A single `datalad save` at the end registers all site-level changes (updated
submodule commit hashes) in the superdataset.

**Phase 4 — Summary.**
Reports per-status counts: merged, already merged (skipped), and failed.

#### `participants.tsv` (per site)

Each site dataset gets its own `participants.tsv` with four columns:

| Column | Description |
|--------|-------------|
| `participant_id` | Subject identifier (e.g., `sub-v1s0x0050642`) |
| `stc_ref_time` | Slice-timing correction reference time in seconds |
| `fmriprep_start` | ISO 8601 timestamp when fMRIPrep started |
| `fmriprep_stop` | ISO 8601 timestamp when fMRIPrep finished |

The companion `participants.json` sidecar provides BIDS-style descriptions
and units for each column.

#### Why octopus merge works

The sbatch script declares outputs with `--explicit`, restricting each job's
commit to:

- `<site>/<participant_id>/` — fMRIPrep derivatives
- `<site>/<participant_id>.html` — QC report
- `<site>/sourcedata/freesurfer/<participant_id>_<ses>/` — FreeSurfer output

Shared files (`dataset_description.json`, `CITATION.*`, `fsaverage/`,
`.bidsignore`) are never committed by individual jobs, so branches are
guaranteed to touch disjoint paths.
Git's octopus merge strategy handles this efficiently in a single merge commit.

#### Typical post-batch workflow

```bash
# 1. Submit a batch of subjects
sbatch --array=1-50 code/fmriprep-jobarray.sbatch \
  --project-root /path/to/abide_fmriprep \
  --container-name fmriprep-apptainer

# 2. After all jobs complete, reconcile
bash code/reconcile_subdatasets.sh -C /path/to/abide_fmriprep --push

# 3. Verify
cd /path/to/abide_fmriprep/derivatives/v1s0
git log --oneline -5
cat participants.tsv
```
