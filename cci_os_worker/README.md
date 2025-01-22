# CCI OS Worker Source

This package has now combined elements from the following packages, which have been dropped as dependencies:
 - CEDA FBS
 - Nappy

The source code provided here should accomplish all tasks related to these dependency packages. Contact [Daniel Westwood](daniel.westwood@stfc.ac.uk) for more details

## Entrypoint Scripts
It is recommended to use the entrypoint scripts to interface with this package once installed:
 - facetscan
 - fbi_update

NOTE: In a future update, these two scripts will be combined into a single workflow which generates the entire record for one dataset as a single process, rather than generating half the record in each case.

### Facetscan

Use this entrypoint to perform a facet scan given a set of dataset files and the worker config. This is automatically configured to create records in the elasticsearch index given by `facet_files_index` in the worker config.
 - `datafile_path`: A path to a `datasets.txt` list of files.
 - `conf`: Path to the worker config file.
 - [OPTIONAL] `output`: Outputs failed datasets to a text file.

Use `facetscan --help` for more information.

### FBI Update

This entrypoint completes the second part of the Opensearch record for each dataset, with the same configuration options as above:
 - `datafile_path`: A path to a `datasets.txt` list of files.
 - `conf`: Path to the worker config file.
 - [OPTIONAL] `output`: Outputs failed datasets to a text file.

Use `fbi_update --help` for more information.