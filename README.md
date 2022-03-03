# timeseries-importer

Modules for extracting timeseries from various sources.

Note: The code has been extracted from https://github.com/metno/havvarsel-data-driven-pred and still needs some refactoring before it can function as a more general module library.

## Requirements and set-up 

This sandbox requires an installation of python3 and some related packages:

Using Miniconda, a suitable environment is created by
```
conda env create -f conda_environment.yml
conda activate timeseries-importer
```

It is, of course, also possible to install the requirements without using conda, e.g., using pip or other package managers, or by installing from source.

## How to use

Each file contains further information and a small example call in its header. To get familiar with the code, we recommend to take a look at those. 
