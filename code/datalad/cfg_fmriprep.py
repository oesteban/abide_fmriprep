#!/usr/bin/env python3
"""
Procedure to apply a sensible BIDS default setup to fMRIPrep's derivatives.

Installation::

    PYTHON_SITE_PACKAGES=$( python -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])' )
    ln -s <path>/code/datalad/cfg_fmriprep.py ${PYTHON_SITE_PACKAGES}/datalad/resources/procedures/

Check installation::
    datalad run-procedure --discover

"""

import sys

from datalad.distribution.dataset import require_dataset
from datalad.support import path as op

ds = require_dataset(
    sys.argv[1],
    check_installed=True,
    purpose='BIDS dataset configuration')

# unless taken care of by the template already, each item in here
# will get its own .gitattributes entry to keep it out of the annex
# give relative path to dataset root (use platform notation)
force_in_git = [
    '.bidsignore',
    '*.bval',
    '*.bvec',
    '*.json',
    'CHANGES',
    'code/**',
    'dataset_description.json',
    'LICENSE',
    'logs/**',
    'README*',
    # comment out the line below to not put participants or scan info into Git
    # (might contain sensitive information)
    '*.bib',
    '*.html',
    '*.md',
    '*.tex',
    '*.toml',
    '*.tsv',
    '*.txt',
    '*.yaml',
    '*.yml',
    # FreeSurfer outputs
    '*.annot',
    '*.cmd',
    '*.csv',
    '*.dat',
    '*.done',
    '*.label',
    '*.local-copy",'
    '*.log.bak',
    '*.log',
    '*.lta',
    '*.stats',
    '*.touch',
]

force_in_annex = [
    '*.env.bak',  # Protect the environment file from being added into Git
    '*.env',  # Protect the environment file from being added into Git
    '*.gii',
    '*.h5',
    '*.parquet',
    '*.svg',
    '*.x5',
]

# make an attempt to discover the prospective change in .gitattributes
# to decide what needs to be done, and make this procedure idempotent
# (for simple cases)
attr_fpath = op.join(ds.path, '.gitattributes')
if op.lexists(attr_fpath):
    with open(attr_fpath, 'rb') as f:
        attrs = f.read().decode()
else:
    attrs = ''

# amend gitattributes, if needed
ds.repo.set_gitattributes([
    (path, {'annex.largefiles': 'nothing'})
    for path in force_in_git
    if '{} annex.largefiles=nothing'.format(path) not in attrs
])

# amend gitattributes, if needed
ds.repo.set_gitattributes([
    (path, {'annex.largefiles': 'anything'})
    for path in force_in_annex
    if '{} annex.largefiles=anything'.format(path) not in attrs
])

# leave clean
ds.save(
    path=['.gitattributes'],
    message="Apply default BIDS dataset setup",
    to_git=True,
)


