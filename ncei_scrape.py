import os
from bs4 import BeautifulSoup
import requests
import shutil
import urllib.request
import gzip
import logging
from datetime import datetime

import scrape_variables
from ncei_backend import SqlBackend

# enable debug logging of the server connection
# import http.client
# http.client.HTTPConnection.debuglevel = 1


class NceiScrape(SqlBackend):
    """
    Stores the metadata for the ncei scraping, run ncei_scrape to start the operation.
    """
    def __init__(self, output_folder: str = None):
        super().__init__()
        # start a new session, should help with pulling from the server many times in a row
        self.session = requests.Session()
        # if you ever have to change this url, it will probably mess up a lot of the logic used to find the survey/shipname
        self.ncei_url = "https://data.ngdc.noaa.gov/platforms/ocean/ships/"

        self.output_folder = output_folder
        self._validate_inputs()
        self._configure_logger()
        self._configure_backend()

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
            if self._check_for_survey(self.ship_name, self.survey_name):  # survey exists in metadata
                self.logger.log(logging.INFO, f'Skipping {self.ship_name}/{self.survey_name}')
                return False
            else:
                self.logger.log(logging.INFO, f'Searching through {self.ship_name}/{self.survey_name}')
                return True
        return True

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
            else:
                self.downloaded_error_count += 1
            # self.logger.log(logging.INFO, (shipname, surveyname, filename, output_path))
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
                    href = i.attrs['href']
                    data = i.text
                    if not href.endswith(r'/'):
                        nceifile = nceisite + href.lstrip(r'/')
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
                    self.logger.log(logging.ERROR, f'ERROR: {type(e).__name__} - {e}')

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
            self.logger.log(logging.ERROR, f'{output_path} already exists, cannot download file')
            return False

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
                        return True
            except Exception as e:
                self.logger.log(logging.WARNING, f'Try {retries}: {type(e).__name__}: {e}')
                retries += 1
        return False

    def close(self):
        self._close_backend()


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


def _build_output_path(output_folder, file_extension, shipname, surveyname, filename):
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
    os.makedirs(os.path.join(output_folder, shipname, surveyname), exist_ok=True)
    return pth


if __name__ == "__main__":
    nc = NceiScrape()
    nc.ncei_scrape()
