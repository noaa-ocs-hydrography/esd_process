import os
from bs4 import BeautifulSoup
import requests
import shutil
import urllib.request
import gzip

import scrape_variables


def ncei_scrape(output_folder: str = None, nceisite: str = "https://data.ngdc.noaa.gov/platforms/ocean/ships/",
                shiplevel: bool = False):
    """
    base site contains links for all ships in NCEI datastorage.  Scrape each ship directory for files with extensions
    matching provided tuple.

    Parameters
    ----------
    output_folder
        output_folder for the data downloaded, if not provided will use the scrape_variables options
    nceisite
        base site for the ncei ship ftp store
    shiplevel
        first run is ship level, all other runs are recursive over that ship address
    """

    output_folder = _validate_inputs(output_folder)
    _ncei_scrape(output_folder, nceisite, shiplevel)


def _ncei_scrape(output_folder: str, nceisite: str, shiplevel: bool = False):
    """
    worker for scrape, run recursively through the folders

    Parameters
    ----------
    output_folder
        output_folder for the data downloaded
    nceisite
        base site for the ncei ship ftp store
    shiplevel
        first run is ship level, all other runs are recursive over that ship address
    """

    resp = requests.get(nceisite)  # response object from request
    bsoup = BeautifulSoup(resp.text, "html.parser")  # parse the html text
    for i in bsoup.find_all("a"):  # get all the hyperlink tags
        try:
            href = i.attrs['href']
            data = i.text
            if not href.endswith(r'/'):
                nceifile = nceisite + href.lstrip(r'/')
                print('- ' + nceifile, data[-8:])
                if data[-8:] in scrape_variables.extensions:  # this is a link to a file matching one of our extensions
                    shipname, surveyname, filename = _parse_multibeam_file_link(nceifile)
                    output_path = _build_output_path(output_folder, data[-8:], shipname, surveyname, filename)
                    # success = download_multibeam_file(nceifile, output_path)
                    print(shipname, surveyname, filename, output_path)
                else:
                    pass

            # Found that they will make the link and the text the same when it is a link to a subpage.  For example,
            # href='ahi/' and data='ahi/' for the link to the ahi ship subpage.  This check seems to work pretty well
            # througout the site
            elif href == data:
                if shiplevel:
                    if data.rstrip('/') in scrape_variables.exclude_vessels:
                        continue
                    print('Crawling for ship {}'.format(data))
                # nceisite = nceisite + href
                print(nceisite + href)
                _ncei_scrape(output_folder=output_folder, nceisite=nceisite + href)
                # if shiplevel:  # after crawling the whole ship directory, go back to the base site path for the next ship
                #     nceisite = nceisite[:-len(href)]
        except:
            print('ERRRORR')


def _validate_inputs(output_folder: str = None):
    """
    Build and validate the input derived objects

    Parameters
    ----------
    output_folder
        output_folder for the data downloaded, optional, if not provided will use the scrape_variables variable

    Returns
    -------
    str
        desired output folder for the ship/survey data folders
    """

    if output_folder is not None:
        output_folder = output_folder
    elif scrape_variables.output_directory:
        output_folder = scrape_variables.output_directory
    else:
        output_folder = scrape_variables.default_output_directory
    os.makedirs(output_folder, exist_ok=True)

    return output_folder


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
    return pth


def download_multibeam_file(ncei_url: str, output_path: str, decompress: bool = True):
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
        raise IOError(f'{output_path} already exists, cannot download file')

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
                    assert os.path.exists(outfile)
                    return True
        except Exception as e:
            print(f'Try {retries}: {type(e).__name__}: {e}')
            retries += 1
    return False


if __name__ == "__main__":
    ncei_scrape(shiplevel=True)
