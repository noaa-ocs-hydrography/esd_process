# esd_process

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

`conda install -c conda-forge qgis=3.18.0 vispy=0.6.6 pyside2=5.13.2 gdal=3.2.1 h5py `

`pip install git+https://github.com/noaa-ocs-hydrography/kluster.git#egg=hstb.kluster `

`pip install git+https://github.com/noaa-ocs-hydrography/esd_process.git#egg=esd_process `

##### Required for GDAL < 3.2.3 and BAG output

If you use GDAL < 3.2.3 (which this currently uses due to a requirement by QGIS), you need to replace:

path_to_envs_folder\envs\esd_process_test\Library\share\gdal\bag_template.xml

with the bag_template file in this repository.  This will write the correct bag metadata, required by CARIS to view.

## Quickstart - using scrape_variables.py to initialize

Configure the scrape_variables.py for the settings you want to use.

Run by calling the ncei_scrape.py file:

`conda activate esd_process_test `

`python -m esd_process`

## Quickstart - nceiscrape

Run by calling the ncei_scrape command line utility

`conda activate esd_process_test `

See help text

`python -m esd_process nceiscrape -h`

Example running and setting the output directory manually

`python -m esd_process nceiscrape -o C:\source\esd_process\myoutputdirectory`

Example limiting to a specific region (setting region directory is only necessary if it is not the esd_process/region_geopackages directory)

`python -m esd_process nceiscrape -r MCD_GL_WGS84 -rdir "C:\source\esd_process\esd_process\region_geopackages" -o "C:\source\esd_process\myoutputdirectory"`

## Quickstart - ncei_query

nceiscrape will query the NCEI service for surveys within the time range/extents using ncei_query.  You can also use it by itself

```
from esd_process.ncei_query import MultibeamQuery, BagQuery

# get the ship and survey name for all surveys in the given region that have raw multibeam files on NCEI
query = MultibeamQuery()
rawmbes_data = query.query(region_name='LA_LongBeach_WGS84', include_fields=('PLATFORM', 'SURVEY_ID'))
> Discovered 19 regions from region folder C:\Users\eyou1\source\esd_process\esd_process\region_geopackages
> Operating on area extents number 1...
> Chunk 1 of 1...
> NCEI query complete, found 16 surveys matching this query

# get the download link for all the surveys in the given region that have BAG files on NCEI
query = BagQuery()
links_to_bagfiles = query.query(region_name='LA_LongBeach_WGS84', include_fields=('DOWNLOAD_URL',))
> Discovered 19 regions from region folder C:\Users\eyou1\source\esd_process\esd_process\region_geopackages
> Operating on area extents number 1...
> Chunk 1 of 1...
> NCEI query complete, found 27 surveys matching this query
```

## Quickstart - regions

You can also directly access the data for all region geopackages found

```
from esd_process.regions import Regions

reg = Regions()  # can include the path to a remote region geopackages directory
envelope = reg.return_region_by_name('PBG_Gulf_UTM14N_MLLW')
envelope
> [{'xmin': -98.4, 'ymin': 24.0, 'xmax': -95.99, 'ymax': 31.2}]
region_path = reg.return_region_by_name('PBG_Gulf_UTM14N_MLLW', return_bounds=False)
region_path
> 'C:\\source\\esd_process\\esd_process\\region_geopackages\\PBG_Gulf_UTM14N_MLLW.gpkg'
```
