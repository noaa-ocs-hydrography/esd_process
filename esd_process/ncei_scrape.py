import os
from bs4 import BeautifulSoup
import requests
import shutil
import urllib.request
import gzip
import logging
from datetime import datetime

from esd_process import scrape_variables
from esd_process.ncei_backend import SqlBackend
from esd_process.ncei_query import MultibeamQuery
from esd_process.kluster_process import kluster_enabled, run_kluster

# enable debug logging of the server connection
# import http.client
# http.client.HTTPConnection.debuglevel = 1


class NceiScrape(SqlBackend):
    """
    Stores the metadata for the ncei scraping, run ncei_scrape to start the operation.
    """
    def __init__(self, output_folder: str = None, coordinate_system: str = None, vertical_reference: str = None,
                 region: str = None, region_directory: str = None, grid_type: str = None, resolution: float = None,
                 grid_format: str = None):
        super().__init__()
        # start a new session, should help with pulling from the server many times in a row
        self.session = requests.Session()
        # if you ever have to change this url, it will probably mess up a lot of the logic used to find the survey/shipname
        self.ncei_url = "https://data.ngdc.noaa.gov/platforms/ocean/ships/"

        self.kluster_coordinate_system = 'NAD83'

        self.output_folder = output_folder
        self.coordinate_system = coordinate_system
        self.vertical_reference = vertical_reference
        self.region = region
        self.region_directory = region_directory
        self.grid_type = grid_type
        self.resolution = resolution
        self.grid_format = grid_format

        self.region_ship_name = []
        self.region_survey_name = []

        self._validate_inputs()
        self._configure_logger()
        self._configure_backend()
        self._check_kluster()
        self._check_regions()

    def _validate_inputs(self):
        """
        Build and validate the input derived objects

        Returns
        -------
        str
            desired output folder for the ship/survey data folders
        """

        if self.output_folder is not None:
            self.output_folder = self.output_folder
        elif scrape_variables.output_directory:
            self.output_folder = scrape_variables.output_directory
        else:
            self.output_folder = scrape_variables.default_output_directory

        os.makedirs(self.output_folder, exist_ok=True)
        logger = logging.getLogger(scrape_variables.logger_name)
        logger.log(logging.INFO, f'Output directory set to {self.output_folder}')

    def _check_kluster(self):
        """
        Simple info message for whether or not Kluster is installed and found
        """
        if kluster_enabled:
            self.logger.log(logging.INFO, f'_check_kluster: Kluster module found, kluster processing enabled')
        else:
            self.logger.log(logging.WARNING, f'_check_kluster: Unable to find Kluster!')

    def _check_regions(self):
        """
        If a region is provided, either in scrape_variables or as an argument, perform the query and get all the
        ship/survey names in the area.
        """

        if self.region is not None:
            self.region = self.region
        elif scrape_variables.region:
            self.region = scrape_variables.region
        if self.region_directory is not None:
            self.region_directory = self.region_directory
        elif scrape_variables.region_folder:
            self.region_directory = scrape_variables.region_folder
        if self.region:
            self.region_ship_name, self.region_survey_name = survey_names_in_region(self.region, self.region_directory,
                                                                                    self.logger)

    def _configure_logger(self):
        """
        Configure the scraper logger and the optional urllib3 logger to output to stdout and to a file.  The urllib3 logger is
        used in conjunction with the http.client.HTTPConnection.debuglevel ( you need to uncomment this line up near the
        import statements ) to get server connection messages for debugging.
        """

        logging.basicConfig()
        logfile = os.path.join(self.output_folder, f'logfile_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt')

        # base log, we can get to it later by getting the 'scraper' log by name
        self.logger = logging.getLogger(scrape_variables.logger_name)
        self.logger.setLevel(scrape_variables.logger_level)

        # used if you use the http connection debug, see import statements
        requests_log = logging.getLogger("requests.packages.urllib3")
        requests_log.setLevel(logging.DEBUG)
        requests_log.propagate = True

        filelogger = logging.FileHandler(logfile)
        filelogger.setLevel(scrape_variables.logger_level)
        fmat = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        filelogger.setFormatter(logging.Formatter(fmat))
        logging.getLogger().addHandler(filelogger)

    def _allow_shipname_surveyname(self, ncei_url: str):
        """
        Check if this url is to a survey/ship that we have searched already.  If so, we skip by returning False.  Otherwise
        we return True to continue searching this survey.

        Parameters
        ----------
        ncei_url
            path to the base of a ship/survey name

        Returns
        -------
        bool
            True means proceed with searching the NCEI site, False means stop at this point because the survey name and ship
            name have been searched already
        """

        urldata = ncei_url.split('/')
        if len(urldata) == 9:
            # first add the metadata you have for the survey you just iterated through to the database (first time through skips this)
            self._add_survey()
            self.ship_name = urldata[6]
            self.survey_name = urldata[7]
            self.survey_url = ncei_url
            self.raw_data_path = ''
            if not self.region_survey_name:
                self.logger.log(logging.WARNING, f'_allow_shipname_surveyname: region list is empty, were no regions found for your query?')
                return False

            # these two checks only if a region was provided
            if self.region_survey_name and (self.survey_name.lower() not in self.region_survey_name):
                self.logger.log(logging.INFO, f'_allow_shipname_surveyname: Skipping {self.ship_name}/{self.survey_name}, survey name not found in region list')
                return False
            elif self.region_ship_name[self.region_survey_name.index(self.survey_name.lower())].replace(' ', '_') != self.ship_name:
                self.logger.log(logging.INFO, f'_allow_shipname_surveyname: Skipping {self.ship_name}/{self.survey_name}, survey name found but ship name does not match')
                return False

            if self._check_for_grid(self.ship_name, self.survey_name):  # survey exists in metadata and a grid has successfully been made
                self.logger.log(logging.INFO, f'_allow_shipname_surveyname: Skipping {self.ship_name}/{self.survey_name}, already processed once')
                return False
            self.logger.log(logging.INFO, f'_allow_shipname_surveyname: Searching for files in {self.ship_name}/{self.survey_name}')
            return True
        return True

    def _skip_to_gridding(self, ncei_url: str):
        """
        ncei scrape is a three step process: download raw multibeam, process to kluster format, build and export grid.  The completion of
        the process to kluster format step ends with deleting the raw multibeam.  If we have already processed and deleted the raw
        data, we don't want to then restart downloading the multibeam again.  Here we see if we should skip to gridding.

        Parameters
        ----------
        ncei_url
            path to the base of a ship/survey name

        Returns
        -------
        bool
            True if we should skip to gridding
        """

        if ncei_url[-8:] in scrape_variables.extensions:
            shipname, surveyname, filename = _parse_multibeam_file_link(ncei_url)
            output_path = _build_output_path(self.output_folder, ncei_url[-8:], shipname, surveyname, filename, skip_make_dir=True)
            raw_data_path = os.path.dirname(output_path)
            processed_data_path = raw_data_path + '_processed'
            if os.path.exists(processed_data_path) and not os.path.exists(raw_data_path):
                self.raw_data_path = raw_data_path
                self.processed_data_path = processed_data_path
                return True
            else:
                return False
        else:
            return False

    def _download_file_url(self, nceifile: str):
        """
        We hit a URL that is a file link, so figure out if it is a file we want and download it.  Maintain the globals
        for this survey/shipname for how many files we downloaded/didnt download.

        Parameters
        ----------
        nceifile
            URL to the file
        """

        # this is a link to a file matching one of our extensions
        if nceifile[-8:] in scrape_variables.extensions:
            shipname, surveyname, filename = _parse_multibeam_file_link(nceifile)
            # get the output path for the file we are downloading, make all the directories if necessary
            output_path = _build_output_path(self.output_folder, nceifile[-8:], shipname, surveyname, filename)
            # download the file and track if the download was successful
            success = self.download_multibeam_file(nceifile, output_path)
            if success:
                self.downloaded_success_count += 1
                self.raw_data_path = os.path.dirname(output_path)
                self.processed_data_path = self.raw_data_path + '_processed'
                if self.grid_type:
                    kgt = self.grid_type
                else:
                    kgt = scrape_variables.kluster_grid_type

                if self.resolution:
                    kgr = self.resolution
                else:
                    kgr = scrape_variables.kluster_resolution

                if self.grid_format:
                    kgf = self.grid_format
                else:
                    kgf = scrape_variables.kluster_grid_format
                self.grid_path = os.path.join(self.processed_data_path, f'kluster_export_{kgt}_{kgr}.{kgf}')
            else:
                self.downloaded_error_count += 1
        elif nceifile[-3:] == '.gz':
            self.ignored_count += 1

    def ncei_scrape(self):
        """
        base site contains links for all ships in NCEI datastorage.  Scrape each ship directory for files with extensions
        matching provided tuple.
        """

        self._ncei_scrape(self.ncei_url, True)
        self.close()

    def _ncei_scrape(self, nceisite: str, shiplevel: bool = False):
        """
        worker for scrape, run recursively through the folders

        Parameters
        ----------
        nceisite
            base site for the ncei ship ftp store
        shiplevel
            first run is ship level, all other runs are recursive over that ship address
        """

        if self._allow_shipname_surveyname(nceisite):
            resp = self.connect_to_server(nceisite)  # response object from request
            bsoup = BeautifulSoup(resp.text, "html.parser")  # parse the html text
            for i in bsoup.find_all("a"):  # get all the hyperlink tags
                try:
                    if i.attrs:
                        href = i.attrs['href']
                        data = i.text
                        if not href.endswith(r'/'):
                            nceifile = nceisite + href.lstrip(r'/')
                            # only look at downloading raw multibeam files if we don't have a processed directory yet
                            if self._skip_to_gridding(nceifile):
                                break
                            self._download_file_url(nceifile)

                        # Found that they will make the link and the text the same when it is a link to a subpage.  For example,
                        # href='ahi/' and data='ahi/' for the link to the ahi ship subpage.  This check seems to work pretty well
                        # througout the site
                        elif href == data:
                            if shiplevel:
                                if data.rstrip('/') in scrape_variables.exclude_vessels:
                                    continue
                                self.logger.log(logging.INFO, 'Crawling for ship {}'.format(data))
                            self._ncei_scrape(nceisite=nceisite + href)
                except Exception as e:
                    self.logger.log(logging.ERROR, f'_ncei_scrape ERROR: {type(e).__name__} - {e}')
            self.kluster_process()

    def connect_to_server(self, ncei_url: str):
        """
        Keep getting 504 errors when trying to access the NCEI server to get the HTTP data for a page that is a huge list
        of data files/links.  It appears that by using a session (persists the connection across multiple get statements) and
        by applying some retry logic, we can get a reliable connection even with this issue.

        Parameters
        ----------
        ncei_url
            URL to the page we are trying to access
        """

        retries = scrape_variables.server_reconnect_retries
        current_tries = 0
        while current_tries < retries:
            if self.session:
                resp = self.session.get(ncei_url)  # response object from request
            else:
                resp = requests.get(ncei_url)  # response object from request
            if (resp.status_code >= 200) and (resp.status_code < 300):  # range for successful responses
                return resp
            retries += 1
            self.logger.log(logging.WARNING, f'Retrying {ncei_url}, received status code {resp.status_code}')
        self.logger.log(logging.ERROR, f'Unable to connect to {ncei_url}, tried {retries} times without success')
        return None

    def kluster_process(self):
        """
        Run the kluster routines on the data we just downloaded.
        """

        if self.raw_data_path:
            if kluster_enabled:
                if self.processed_data_path:
                    try:
                        multibeamfiles = [os.path.join(self.raw_data_path, fil) for fil in os.listdir(self.raw_data_path) if os.path.splitext(fil)[1] in scrape_variables.processing_extensions]
                    except:
                        multibeamfiles = []
                    os.makedirs(self.processed_data_path, exist_ok=True)
                    processed, gridded = run_kluster(multibeamfiles, self.processed_data_path, logger=self.logger,
                                                     coordinate_system=self.coordinate_system, vertical_reference=self.vertical_reference,
                                                     grid_type=self.grid_type, resolution=self.resolution, grid_format=self.grid_format)
                    if not processed:
                        self.processed_data_path = ''
                    if not gridded:
                        self.grid_path = ''
                else:
                    self.logger.log(logging.ERROR, f'kluster_process: Unable to find the processed data path, which is set during file transfer, were files not transferred?')
            else:
                self.logger.log(logging.WARNING, f'kluster_process: Kluster not found, skipping the processing for this survey')
        else:
            # no data transferred, we process nothing
            pass

    def download_multibeam_file(self, ncei_url: str, output_path: str, decompress: bool = True):
        """
        Download the file from the url to the provided output_path.  decompress if necessary.  With the .all.mb58.gz files
        we are getting from ncei, we should always decompress.  Leaving the alternative here just in case

        Parameters
        ----------
        ncei_url
            http link to the file
        output_path
            file path to where we want the downloaded file
        decompress
            if True, will decompress with gzip first

        Returns
        -------
        bool
            True if file now exists on the file system
        """

        if os.path.exists(output_path):
            self.logger.log(logging.WARNING, f'{output_path} already exists, skipping this file')
            return True

        retries = 0
        while retries < scrape_variables.download_retries:
            try:
                with urllib.request.urlopen(ncei_url) as response:
                    with open(output_path, 'wb') as outfile:
                        if decompress:
                            with gzip.GzipFile(fileobj=response) as uncompressed:
                                data = uncompressed.read()
                                outfile.write(data)
                        else:
                            shutil.copyfileobj(response, outfile)
                        assert os.path.exists(output_path)
                        self.logger.log(logging.INFO, 'Downloaded file {}'.format(output_path))
                        return True
            except Exception as e:
                self.logger.log(logging.WARNING, f'Try {retries}: {type(e).__name__}: {e}')
                retries += 1
        self.logger.log(logging.WARNING, f'Unable to download file, tried {retries} times')
        return False

    def close(self):
        """
        Should always call this after a session to close the logger and backend
        """

        handlers = self.logger.handlers[:]
        for handler in handlers:
            handler.close()
            self.logger.removeHandler(handler)
        self._close_backend()


def survey_names_in_region(region: str, region_directory: str = None, logger: logging.Logger = None):
    """
    Return all of the surveys with raw multibeam data found in the region provided.

    Parameters
    ----------
    region
        string name of one of the geopackages in region_geopackages
    region_directory
        string path to the region geopackages folder
    logger
        optional logger if you want the query to log to your logger

    Returns
    -------
    list
        list of ship names corrsponding to the survey names
    list
        list of survey names for all surveys with raw multibeam data in the region
    """

    mq = MultibeamQuery(logger=logger, regions_folder=region_directory)
    unique_ship_name, unique_survey_name = [], []
    rawmbes_data = mq.query(region_name=region, include_fields=('PLATFORM', 'SURVEY_ID'))
    if rawmbes_data:
        ship_name = [feat['attributes']['PLATFORM'] for feat in rawmbes_data['features']]
        survey_name = [feat['attributes']['SURVEY_ID'] for feat in rawmbes_data['features']]
        for cnt, surv in enumerate(survey_name):
            if surv not in unique_survey_name:
                unique_survey_name.append(surv.lower())
                unique_ship_name.append(ship_name[cnt].lower())
    return unique_ship_name, unique_survey_name


def _parse_multibeam_file_link(filelink: str):
    """
    Return the relevant data from the multibeam file link.  EX:

    https://data.ngdc.noaa.gov/platforms/ocean/ships/henry_b._bigelow/HB1901L4/multibeam/data/version1/MB/me70/0000_20190501_150651_HenryBigelow.all.mb58.gz

    shipname = henry_b._bigelow
    surveyname = HB1901L4
    filename = 0000_20190501_150651_HenryBigelow.all.mb58.gz

    Parameters
    ----------
    filelink
        http link to the file

    Returns
    -------
    str
        ship name
    str
        survey name
    str
        file name
    """

    splitdata = filelink.split('/')
    shipname = splitdata[6]
    surveyname = splitdata[7]
    filename = splitdata[-1]
    return shipname, surveyname, filename


def _build_output_path(output_folder, file_extension, shipname, surveyname, filename, skip_make_dir: bool = False):
    """
    We want the file to go into a subfolder for survey/shipname, so we take the filename and build the path using
    the provided attributes and the output_folder.

    Parameters
    ----------
    output_folder
        folder to contain the ship data
    file_extension
        extension of the provided file (we assume that we need to convert something like .all.mb58.gz to .all.  In that
        example, file_extension would equal .mb58.gz
    shipname
        name of the ship
    surveyname
        name of the survey
    filename
        name of the file including the full extension

    Returns
    -------
    str
        built path for where we want the downloaded file to go
    """

    basefile = filename[:-len(file_extension)]
    pth = os.path.join(output_folder, shipname, surveyname, basefile)
    if not skip_make_dir:
        os.makedirs(os.path.join(output_folder, shipname, surveyname), exist_ok=True)
    return pth


def main(output_folder: str = None, coordinate_system: str = None, vertical_reference: str = None, region: str = None,
         region_directory: str = None, grid_type: str = None, resolution: float = None, grid_format: str = None):
    """
    Run the ncei scrape utility

    Parameters
    ----------
    output_folder
        optional, a path to an empty folder (it will create if it doesnt exist) to hold the downloaded/processed data, default is the current working directory
    coordinate_system
        optional, processed coordinate system to use, one of NAD83 and WGS84, default is NAD83
    vertical_reference
        optional, vertical reference to use for the processed data, one of 'ellipse' 'mllw' 'NOAA_MLLW' 'NOAA_MHW' (NOAA references require vdatum which isn't hooked up in here just yet), default is waterline
    region
        optional, the name of one of the region_geopackages that you want to limit your search to
    region_directory
        optional, the directory that contains the region geopackages, default is the esd_process/region_geopackages directory
    grid_type
        optional, the grid type you want to build with Kluster for each dataset, one of 'single_resolution', 'variable_resolution_tile', default is single_resolution
    resolution
        optional (only for single_resolution), the resolution of the grid in meters, set this if you do not want to use the auto-resolution option
    grid_format
        optional, the grid format exported by kluster, one of 'csv', 'geotiff', 'bag', default is bag
    """

    nc = NceiScrape(output_folder=output_folder, coordinate_system=coordinate_system, vertical_reference=vertical_reference,
                    region=region, region_directory=region_directory, grid_type=grid_type, resolution=resolution,
                    grid_format=grid_format)
    if nc.region_survey_name:
        nc.ncei_scrape()
    else:
        nc.logger.log(logging.WARNING, f'No regions found in {region}, skipping download')


if __name__ == "__main__":
    main()
