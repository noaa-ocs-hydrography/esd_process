# esd_process

## Overview

A utility for crawing the [NCEI ship data server](https://data.ngdc.noaa.gov/platforms/ocean/ships/) for Kongsberg data to process using Kluster.

As NCEI archives data processed using MB-System, the multibeam files we download will have the following extension:

- .mb58.gz/.mb59.gz = Kongsberg current multibeam format (.all) 

esd_process will:

1. Find and download any multibeam files that have this extension, skip surveys that have no relevant files.
2. Log the metadata for that ship / survey to the backend (sqlite3 the only option currently) so that it only looks for new data each run.
3. Run [Kluster](https://github.com/noaa-ocs-hydrography/kluster) to organize and process the files, exporting a new BAG (or other GDAL format)


## Installation

esd_process is not on PyPi, but can be installed using pip.

(For Windows Users) Download and install Visual Studio Build Tools 2019 (If you have not already): [MSVC Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/)

Download and install conda (If you have not already): [conda installation](https://docs.conda.io/projects/conda/en/latest/user-guide/install/)

Download and install git (If you have not already): [git installation](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)

Some dependencies need to be installed from the conda-forge channel.  I have an example below of how to build this environment using conda.

Perform these in order:

`conda create -n esd_process_test python=3.8.8 `

`conda activate esd_process_test `

`conda install -c conda-forge qgis=3.18.0 vispy=0.6.6 pyside2=5.13.2 gdal=3.2.1`

`pip install git+https://github.com/noaa-ocs-hydrography/kluster.git#egg=hstb.kluster `

`pip install git+https://github.com/noaa-ocs-hydrography/esd_process.git#egg=esd_process `

## Quickstart - using scrape_variables.py to initialize

Configure the scrape_variables.py for the settings you want to use.

Run by calling the ncei_scrape.py file:

`conda activate esd_process_test `

`python -m esd_process`

## Quickstart - using command line

Run by calling the ncei_scrape command line utility

`conda activate esd_process_test `

See help text

`python -m esd_process nceiscrape -h`

Example running and setting the output directory manually

`python -m esd_process nceiscrape -o C:\source\esd_process\myoutputdirectory`