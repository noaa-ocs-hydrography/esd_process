import os
import logging
import shutil
from datetime import datetime

try:
    from HSTB.kluster.fqpr_intelligence import intel_process
    from HSTB.kluster.fqpr_convenience import generate_new_surface
    from HSTB.kluster.dask_helpers import dask_find_or_start_client, Client
    kluster_enabled = True
except:
    kluster_enabled = False
from esd_process import scrape_variables


def _delete_raw_multibeam(multibeam_files: list):
    if multibeam_files:
        for fil in multibeam_files:
            os.remove(fil)


def _delete_processed_multibeam(outfolder: str, grid_folder: str):
    for fldrs in os.listdir(outfolder):
        fldrpath = os.path.join(outfolder, fldrs)
        if fldrpath != grid_folder:
            try:  # for directories
                shutil.rmtree(fldrpath)
            except NotADirectoryError:  # for files
                os.remove(fldrpath)


def run_kluster(multibeam_files: list, outfold: str, logger: logging.Logger = None, coordinate_system: str = None,
                vertical_reference: str = None, grid_type: str = None, resolution: float = None, grid_format: str = None):
    """
    Run the kluster routines to process the provided multibeam files and output processed kluster formats for pings and
    surface, as well as an exported grid in the format you choose.

    Parameters
    ----------
    multibeam_files
        list of absolute paths to the multibeam files that you want to process
    outfold
        the directory that you want to use to keep the kluster converted data instances
    logger
        optional logger to log the info/warnings
    coordinate_system
        optional, processed coordinate system to use, one of NAD83 and WGS84, default is NAD83
    vertical_reference
        optional, vertical reference to use for the processed data, one of 'ellipse' 'waterline' 'NOAA_MLLW' 'NOAA_MHW' (NOAA references require vdatum which isn't hooked up in here just yet), default is waterline
    grid_type
        optional, the grid type you want to build with Kluster for each dataset, one of 'single_resolution', 'variable_resolution_tile', default is single_resolution
    resolution
        optional (only for single_resolution), the resolution of the grid in meters, set this if you do not want to use the auto-resolution option
    grid_format
        optional, the grid format exported by kluster, one of 'csv', 'geotiff', 'bag', default is bag
    """

    dclient = dask_find_or_start_client(number_of_workers=scrape_variables.kluster_number_of_workers,
                                        threads_per_worker=scrape_variables.kluster_threads_per_worker,
                                        memory_per_worker=scrape_variables.kluster_memory_per_worker)
    os.makedirs(outfold, exist_ok=True)
    try:
        _, converted_data_list = run_kluster_intel_process(multibeam_files, outfold, coordinate_system=coordinate_system,
                                                           vertical_reference=vertical_reference, logger=logger, client=dclient)
        processed = True
    except Exception as e:
        if logger:
            logger.log(logging.ERROR, f'ERROR: run_kluster_intel_process: {type(e).__name__} - {e}')
        converted_data_list = []
        processed = False
    if logger:
        if len(converted_data_list) > 0:
            logger.log(logging.INFO, f'run_kluster_intel_process - processed {len(multibeam_files)} multibeam files into '
                                     f'{len(converted_data_list)} kluster converted instances')
        else:
            logger.log(logging.ERROR, f'run_kluster_intel_process - error processing {len(multibeam_files)} multibeam files,'
                                      f' did not get any processed kluster data in return')
    try:
        grid_outfold = os.path.join(outfold, 'grid')
        surf, export_path = build_kluster_surface(converted_data_list, grid_outfold, grid_type=grid_type,
                                                  resolution=resolution, grid_format=grid_format, logger=logger, client=dclient)
        _delete_processed_multibeam(outfold, grid_outfold)
        _delete_raw_multibeam(multibeam_files)
        gridded = True
    except Exception as e:
        if logger:
            logger.log(logging.ERROR, f'ERROR: build_kluster_surface {type(e).__name__} - {e}')
        surf, export_path = None, ''
        gridded = False
    if logger:
        if surf and export_path:
            logger.log(logging.INFO, f'run_kluster - generated new surface, exported grid to {export_path}')
        elif surf:
            logger.log(logging.ERROR, f'run_kluster - generated new surface, but the export to {export_path} failed')
        else:
            logger.log(logging.ERROR, f'run_kluster - unable to generate new surface, error in the processing.')
    return processed, gridded


def run_kluster_intel_process(multibeam_files: list, outfold: str = None, coordinate_system: str = None,
                              vertical_reference: str = None, logger: logging.Logger = None, client: Client = None):
    """
    Process the list of multibeam files provided and return the kluster converted data

    Parameters
    ----------
    multibeam_files
        list of absolute paths to the multibeam files that you want to process
    outfold
        the directory that you want to use to keep the kluster converted data instances
    coordinate_system
        optional, processed coordinate system to use, one of NAD83 and WGS84, default is NAD83
    vertical_reference
        optional, vertical reference to use for the processed data, one of 'ellipse' 'waterline' 'NOAA_MLLW' 'NOAA_MHW' (NOAA references require vdatum which isn't hooked up in here just yet), default is waterline
    logger
        optional logger to log the info/warnings
    client
        optional dask Client to pass into the process function

    Returns
    -------
    Intelligence module instance
        the kluster intelligence module instance containing the converted metadata/paths
    list
        list of kluster Fqpr objects for each modelnumber_serialnumber_day combination in the raw multibeam files
    """

    if coordinate_system:
        cs = coordinate_system
    else:
        cs = scrape_variables.kluster_coordinate_system

    if vertical_reference:
        vf = vertical_reference
    else:
        vf = scrape_variables.kluster_vertical_reference

    intel, converted_data_list = intel_process(multibeam_files, outfold, coord_system=cs, vert_ref=vf, logger=logger, client=client)
    # need to pull the list of converted days from the project to include all converted data, not just converted data from this run
    converted_data_list = list(intel.project.fqpr_instances.values())
    return intel, converted_data_list


def build_kluster_surface(converted_data_list: list, outfold: str = None, grid_type: str = None,
                          resolution: float = None, grid_format: str = None, logger: logging.Logger = None, client: Client = None):
    """
    Take the converted Kluster data and build a new surface from it.  Export a GDAL format from the surface instance
    using the options in scrape_variables.

    Parameters
    ----------
    converted_data_list
        list of kluster Fqpr objects for each modelnumber_serialnumber_day combination in the raw multibeam files
    outfold
        the directory that you want to use to keep the kluster converted data instances
    grid_type
        optional, the grid type you want to build with Kluster for each dataset, one of 'single_resolution', 'variable_resolution_tile', default is single_resolution
    resolution
        optional (only for single_resolution), the resolution of the grid in meters, set this if you do not want to use the auto-resolution option
    grid_format
        optional, the grid format exported by kluster, one of 'csv', 'geotiff', 'bag', default is bag
    logger
        optional logger to log the info/warnings
    client
        optional dask Client to pass into the process function

    Returns
    -------
    BathyGrid object
        a new BathyGrid object generated from the Kluster processed data
    str
        path to the exported grid GDAL file
    """

    if grid_type:
        kgt = grid_type
    else:
        kgt = scrape_variables.kluster_grid_type

    if resolution:
        kgr = resolution
    else:
        kgr = scrape_variables.kluster_resolution

    if grid_format:
        kgf = grid_format
    else:
        kgf = scrape_variables.kluster_grid_format

    if kgr is None:
        formatted_kgr = 'AUTO'
    else:
        formatted_kgr = kgr

    basegridname = f'kluster_export_{kgf}_{kgt}_{formatted_kgr}'
    export_path = os.path.join(outfold, basegridname)
    output_path = os.path.join(outfold, f'kluster_surface')
    if os.path.exists(output_path):
        output_path = os.path.join(outfold, f'kluster_surface_{datetime.now().strftime("%H%M%S")}')

    if logger:
        logger.log(logging.INFO, f'run_kluster - generating new surface {output_path}')
        logger.log(logging.INFO, f'run_kluster - surface grid_type = {kgt}, surface resolution = {kgr}')
    bg = generate_new_surface(converted_data_list, grid_type=kgt, tile_size=1024.0, resolution=kgr, output_path=output_path, use_dask=False,
                              export_path=export_path, export_format=kgf, client=client)

    # check for successful exports, they will have name=basegridname with a _index addition (the first export of 8 would have a _1)
    found = False
    for fil in os.listdir(outfold):
        if fil.find(basegridname) != -1:
            found = True
            break
    if not found:
        export_path = None

    return bg, export_path
