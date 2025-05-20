# CCI Opensearch Worker repository

![Static Badge](https://img.shields.io/badge/cci%20tagger%20workflow-8AD6F6)
![GitHub Release](https://img.shields.io/github/v/release/cedadev/cci-os-worker)
[![PyPI version](https://badge.fury.io/py/cci-os-worker.svg)](https://pypi.python.org/pypi/cci-os-worker/)

## CEDA Dependencies

![Static Badge](https://img.shields.io/badge/elasticsearch%20v8-3BBEB1)
![Static Badge](https://img.shields.io/badge/MOLES%20API%20v2-00628D)

**See release notes for change history**

This package serves as a wrapper for the CCI Opensearch Workflow, which involves several independent packages with multiple dependencies. Primarily the CCI Tagger (cci-tag-scanner) and Facet scanner (cci-facet-scanner) are combined, with elements from the CEDA FBS (ceda-fbs-cci) package to create the components for Opensearch records in Elasticsearch.

**NOTE:** When publishing a new tagged release of this package, please make sure to rebuild the corresponding Docker image, in the CEDA gitlab repository `cci_opensearch_base`. This repository has a single build-image step that should be rerun (following the steps found there) to ensure any changes to this package are picked up by the OS worker deployment.

![CCI Opensearch Workflow](https://github.com/cedadev/cci-os-worker/blob/main/images/CCI_Workflow.png)

## 1. Installation

This package can be cloned directly or used as a dependency in a pyproject file.

Set up a python virtual environment:
```
 $ python -m venv .venv
 $ source .venv/bin/activate
 $ pip install cci-os-worker
```

NOTE: As of 22nd Jan 2025 the `cci-os-worker` repository has been upgraded for use with Poetry version 2. The temporary solution to use a `requirements_fix.txt` file has been removed as this package is now on Pypi.

### 1.1. Use in other packages

**Poetry 1.8.5 and older**
For use in another package as a dependency, use the following in your pyproject `[tool.poetry.dependencies]`:
```
cci-os-worker = { git = "https://github.com/cedadev/cci-os-worker.git", tag="v0.3.1"}
```

**Poetry 2.0.1 and later**
This package is now a pip-installable published package as of 11th April 2025! That means for packages using Poetry 2 or higher, the cci-os-worker can be added via Poetry at version 0.5.0 or higher.

```
poetry add cci-os-worker^0.5.0
```

## 2. Usage

## 2.1 Find datasets

Determining the set of files to operate over can be done in two ways using built-in scripts here, or indeed by any other means. If the intention is to submit to a rabbit queue however, this script is required with the additional `-R` parameter to submit to a queue, and the configuration for the queue given by a yaml file provided as `--conf`.

```
rescan_dir path/to/json/directory/ --extension nc -l 1 -o path/to/dataset/filelist.txt
```

**NOTE**: As of v0.5.0 this changed from `fbi_rescan_dir` to simply `rescan_dir`.

In the above command:
 - `r` represents a recursive look through identified directories.
 - `l` means the scan level. Scan level 1 will involve finding all the JSON files and expanding each `datasets` path into a list.
 - `o` is the output file to send the list of datasets.
 - `--extension` applies to the files identified and added to the output file. `nc` is the default value so is redundant here.
 - `--file-regex` alternative to supplying just the extension, if a valid regex pattern can be matched to identify specific files it can be submitted here.

This command can also be run for a known directory to expand into a list of datasets:

```
rescan_dir my/datasets/path/ -l 2 -o path/to/dataset/filelist.txt
```

In this case we specify `l` as 2 since there are no JSON files involved. The extension/file_regex options can also be added here, but as the `nc` option is a default value we have omitted it here.

## 2.2 Run the facet scan workflow

The facet scanner workflow utilises both the facet and tag scanners to produce the set of facets under `project.opensearch` in the resulting opensearch records. This workflow can be run using the `facetscan` entrypoint script installed with this package.

The environment variable `JSON_TAGGER_ROOT` should be set, which should be the path to the top-level directory under which all JSON files are placed. These JSON files provide defaults and mappings to values placed in the opensearch records - supplementary material to aid facet scanning or replace found values.

As of v0.5.0 the two workflows (facet and FBI) have been combined into one singular workflow to generate all portions of the Opensearch records. This can be run with the following command:

```
 $ cci_os_update path/to/dataset/filelist.txt path/to/config/file.yaml
```
(Note: Verbose flag -v can be added to the above command.)

Where the yaml file should look something like this:

```
elasticsearch:
  # Fill in with key value
  x-api-key: ""
facet_files_index:
  name: facet-index-staging
facet_files_test_index:
  name: facet-index-staging
ldap_configuration:
  hosts:
    - ldap://homer.esc.rl.ac.uk
    - ldap://marge.esc.rl.ac.uk
```

