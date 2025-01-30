# CCI Opensearch Worker repository

__Latest version: v0.4.0 (22nd January 2025)__

**see release notes for change history**

This package serves as a wrapper for the CCI Opensearch Workflow, which involves several independent packages with multiple dependencies. Primarily the CCI Tagger (cci-tag-scanner) and Facet scanner (facet-scanner) are combined, with elements from the CEDA FBS (ceda-fbs-cci) package to create the components for Opensearch records in Elasticsearch.

![CCI Opensearch Workflow](https://github.com/cedadev/cci-os-worker/blob/main/_images/CCI_Workflow.png)

## 1. Installation

This package can be cloned directly or used as a dependency in a pyproject file.

```
 $ git clone git@github.com:cedadev/cci-os-worker.git
 $ cd cci-os-worker
```
Set up a python virtual environment
```
 $ python -m venv .venv
 $ source .venv/bin/activate
 $ pip install .
```

NOTE: As of 22nd Jan 2025 the `cci-os-worker` repository has been upgraded for use with Poetry version 2. This requires the use of an additional `requirements_fix.txt` patch while a solution for poetry dependencies in github is worked on. The above installation MUST be supplemented with:

```
 $ pip install -r requirements_fix.txt
```
This is a temporary fix and will be removed when poetry is patched.

### 1.1. Use in other packages

**Poetry 1.8.5 and older**
For use in another package as a dependency, use the following in your pyproject `[tool.poetry.dependencies]`:
```
cci-os-worker = { git = "https://github.com/cedadev/cci-os-worker.git", tag="v0.3.1"}
```

**Poetry 2.0.1 and later**
The exact package address and tag should be added to a `requirements_fix.txt` file, which should then be installed as an additional step when using this package. This is in reference to the note on Poetry version 2 above.

## 2. Usage

## 2.1 Find datasets

Determining the set of files to operate over can be done in two ways using built-in scripts here, or indeed by any other means. If the intention is to submit to a rabbit queue however, this script is required with the additional `-R` parameter to submit to a queue, and the configuration for the queue given by a yaml file provided as `--conf`.

```
fbi_rescan_dir path/to/json/directory/ -r -l 1 -o path/to/dataset/filelist.txt
```

In the above command:
 - `r` represents a recursive look through identified directories.
 - `l` means the scan level. Scan level 1 will involve finding all the JSON files and expanding each `datasets` path into a list.
 - `o` is the output file to send the list of datasets.

This command can also be run for a known directory to expand into a list of datasets:

```
fbi_rescan_dir my/datasets/path/**/*.nc -r -l 2 -o path/to/dataset/filelist.txt
```

In this case we specify `l` as 2 since there are no JSON files involved.

## 2.2 Run the facet scan workflow

The facet scanner workflow utilises both the facet and tag scanners to produce the set of facets under `project.opensearch` in the resulting opensearch records. This workflow can be run using the `facetscan` entrypoint script installed with this package.

The environment variable `JSON_TAGGER_ROOT` should be set, which should be the path to the top-level directory under which all JSON files are placed. These JSON files provide defaults and mappings to values placed in the opensearch records - supplementary material to aid facet scanning or replace found values.

```
 $ facetscan path/to/dataset/filelist.txt path/to/config/file.yaml
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

### 2.3 Run the FBI/Info workflow

As of 17/12/2024 this workflow is still referred to as the FBI workflow despite not pushing to the `ceda-fbi` index. This section deals with extracting information from the provided files (temporal/spatial/phenomena) and adding to the Opensearch records. If the file-opening mechanism is not known or well defined, this information is limited to what can be extracted from the file name.

```
 $ fbi_update path/to/dataset/filelist.txt path/to/config/file.yaml
```

