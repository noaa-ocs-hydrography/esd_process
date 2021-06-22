import os
import numpy as np
from typing import Union
from datetime import datetime
import requests
import logging

from esd_process.regions import Regions
from esd_process import scrape_variables


class QueryBase:
    """
    Provides all the basic attribution and methods that go with all query classes
    """
    def __init__(self):
        self.start_date = None
        self.end_date = None
        self.date_string_format = '%m/%d/%y'
        self.envelope_extents = None
        self.region_name = None
        self.logger = None

    def _return_formatted_date(self, datedata: Union[str, datetime]):
        """
        Take either a string or a datetime object and return the date formatted in the way that is needed by the
        NCEI query

        Parameters
        ----------
        datedata
            Either a date as a string matching the provided date_string_format or a datetime object

        Returns
        -------
        str
            date formatted as 2021-01-23
        """

        if isinstance(datedata, str):
            datedata = datetime.strptime(datedata, self.date_string_format)
        datedata = f'{datedata.strftime("%Y-%m-%d")}'
        return datedata

    def _print(self, msg: str, lvl: int = logging.INFO):
        """
        Allow for printing to console (if no logger) or printing to logger if logger provided

        Parameters
        ----------
        msg
            log message
        lvl
            one of the logging levels
        """

        if self.logger:
            self.logger.log(lvl, msg)
        else:
            print(msg)

    def _dates_to_text(self):
        """
        Return the class attribute start and end date as formatted strings that match what the NCEI query wants
        """

        start_date = ''
        end_date = ''
        if self.start_date is not None:
            start_date = self._return_formatted_date(self.start_date)
        if self.end_date is not None:
            end_date = self._return_formatted_date(self.end_date)
        return start_date, end_date

    def _extents_to_hex_formatting(self, extent: dict):
        """
        converts string to ascii hex codes for the rest query.

        Parameters
        ----------
        extent

        Returns
        -------

        """
        if extent:
            new_extent = f"%7B%27xmin%27%3A+{extent['xmin']}%2C+%27ymin%27%3A+{extent['ymin']}%2C+%27xmax%27%3A+{extent['xmax']}%2C+%27ymax%27%3A+{extent['ymax']}%7D"
        else:
            new_extent = ''
        return new_extent


class NceiQuery(QueryBase):
    """
    The shared attributes and methods for all the NCEI query classes, you should not use this directly, instead use one
    of the query classes depending on the data type you are interested in.  See MultibeamQuery as an example
    """
    def __init__(self, logger: logging.Logger = None, regions_folder: str = None):
        super().__init__()
        self.logger = logger
        self.regions = Regions(logger=self.logger, regions_folder=regions_folder)
        self.data_type = ''
        self.rest_level = ''
        self.fields = []

        # start a new session, should help with pulling from the server many times in a row
        self.session = requests.Session()

        self.geometry_type = 'esriGeometryEnvelope'
        self.input_coordinate_system = '4326'  # wgs84
        self.geometry_query = 'esriSpatialRelIntersects'
        self.include_fields = ''
        self.where_statement = ''

        self.output_format = 'json'

    @property
    def rest_url(self):
        """
        Return the correct URL for the data type / rest level of this class.
        """
        return f'https://gis.ngdc.noaa.gov/arcgis/rest/services/web_mercator/{self.data_type}/MapServer/{self.rest_level}'

    def _build_date_query(self):
        raise NotImplementedError('Please choose one of the Query classes, do not run this class directly')

    def _build_extents_query(self):
        """
        Build out the extents depending on if the user provided a region_name (which must have a matching gpkg file) or
        if they manually provided extents as a dict matching the esri envelope feature schema.
        """

        if self.envelope_extents and self.region_name:
            self._print('Both region name and envelope extents provided, region name will be used to supersede the region name', logging.WARNING)
        if self.region_name:
            self.envelope_extents = self.regions.return_region_by_name(self.region_name, return_bounds=True)
        if isinstance(self.envelope_extents, dict):
            self.envelope_extents = [self.envelope_extents]
        if not self.envelope_extents:
            self.envelope_extents = [{}]

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
            self._print(f'Retrying {ncei_url}, received status code {resp.status_code}', logging.WARNING)
        self._print(f'Unable to connect to {ncei_url}, tried {retries} times without success', logging.ERROR)
        return None

    def _build_query_url(self, envelope: dict, return_geometry: bool, return_ids_only: bool, return_count_only: bool, return_extent_only: bool,
                         only_these_object_ids: list = []):
        """
        Return the query URL for all the settings provided in this query session.  The end result is something you can
        enter into a web browser or use requests to get the data.  Should provide the requested data for all surveys
        that are within these parameters.

        Parameters
        ----------
        envelope
            area extents as a dict matching the esri envelope feature specification
        return_geometry
            if True, will return the geometry of the survey as well.
        return_ids_only
            if True, will only return the object ids of the surveys matching this query
        return_count_only
            if True, will only return the number of surveys matching this survey
        return_extent_only
            if True, will only return the extents of the surveys matching this query
        only_these_object_ids
            if True, will only search for the surveys that have the provided object ids

        Returns
        -------
        str
            new query URL for the parameters given
        """

        query_url = self.rest_url + f'/query?where={self.where_statement}&text=&objectIds={",".join([str(l) for l in only_these_object_ids])}'
        query_url += f'&time=&geometry={self._extents_to_hex_formatting(envelope)}'
        query_url += f'&geometryType={self.geometry_type}&inSR={self.input_coordinate_system}&spatialRel={self.geometry_query}'
        query_url += f'&relationParam=&outFields={self.include_fields}&returnGeometry={str(return_geometry).lower()}&returnTrueCurves=false'
        query_url += f'&maxAllowableOffset=&geometryPrecision=&outSR=&having=&returnIdsOnly={str(return_ids_only).lower()}'
        query_url += f'&returnCountOnly={str(return_count_only).lower()}&orderByFields=&groupByFieldsForStatistics='
        query_url += f'&outStatistics=&returnZ=false&returnM=false&gdbVersion=&historicMoment=&returnDistinctValues=false&resultOffset='
        query_url += f'&resultRecordCount=&queryByDistance=&returnExtentOnly={str(return_extent_only).lower()}&datumTransformation='
        query_url += f'&parameterValues=&rangeValues=&quantizationParameters=&f={self.output_format}'
        return query_url

    def _validate_query_parameters(self, start_date: Union[str, datetime] = None, end_date: Union[str, datetime] = None,
                                   date_string_format: str = '%m/%d/%y', envelope_extents: Union[list, dict] = None,
                                   region_name: str = None, include_fields: tuple = ()):
        """
        Take the parameters provided and convert to formats required by the query.  At the end of this, you should be
        able to run _build_query_url and get a correct URL.

        Parameters
        ----------
        start_date
            Optional, either a string in the date_string_format format, or a datetime object representing the start date of the query
        end_date
            Optional, either a string in the date_string_format format, or a datetime object representing the end date of the query
        date_string_format
            Optional, if you are providing start or end dates as string, you must provide this (see datetime strftime/strptime format)
        envelope_extents
            Optional, either a list of dicts or a dict, dict is in esri envelope feature specification
        region_name
            Optional, if provided will override envelope_extents and use the extents of the matching gpkg file
        include_fields
            Optional, if provided, will only return these fields for each survey queried
        """

        self.start_date = start_date
        self.end_date = end_date
        self.date_string_format = date_string_format
        self.envelope_extents = envelope_extents
        self.region_name = region_name
        if include_fields:
            invalid_fields = [field for field in include_fields if field not in self.fields]
            if invalid_fields:
                self._print(f'Invalid field(s) provided {invalid_fields}, must be one of {self.fields}', logging.ERROR)
                raise ValueError(f'Invalid field(s) provided {invalid_fields}, must be one of {self.fields}')
            self.include_fields = '%2C'.join(include_fields)
        else:
            self.include_fields = ''
        self.where_statement = self._build_date_query()
        self._build_extents_query()

    def _query_object_ids(self, envelope: dict):
        """
        Due to the length limit of querying the ncei server, we need to first do a smaller query to just get the integer
        object ids of the surveys in our query.  We can then use the object ids to query by object id later to get the
        full dataset.

        Parameters
        ----------
        envelope
            the extent of the query in esri envelope format, envelope={} for queries that are not area based

        Returns
        -------
        list
            list of object ids matching this survey query
        """

        query_url = self._build_query_url(envelope, False, True, False, False)
        query_data = self.connect_to_server(query_url)  # response object from request
        object_ids = []
        if query_data is not None:
            json_data = query_data.json()
            if 'objectIds' in json_data and json_data['objectIds'] is not None:
                object_ids.extend(json_data['objectIds'])
            elif 'error' in json_data:
                self._print(f"Error in query response: {query_data} for query {query_url}", logging.ERROR)
        else:
            self._print(f'Unable to connect to ncei server, using {query_url}', logging.ERROR)
        return object_ids

    def query(self, start_date: Union[str, datetime] = None, end_date: Union[str, datetime] = None, date_string_format: str = '%m/%d/%y',
              envelope_extents: Union[list, dict] = None, region_name: str = None, return_geometry: bool = False,
              return_ids_only: bool = False, return_count_only: bool = False, return_extent_only: bool = False, include_fields: tuple = ()):
        """
        Query NCEI to get the data of all surveys that are within these parameters.  The surveys returned depend on the
        Query class you are using (you can't use this base class).  For instance, MultibeamQuery will return the surveys
        that have multibeam files on NCEI.

        Parameters
        ----------
        start_date
            Optional, either a string in the date_string_format format, or a datetime object representing the start date of the query
        end_date
            Optional, either a string in the date_string_format format, or a datetime object representing the end date of the query
        date_string_format
            Optional, if you are providing start or end dates as string, you must provide this (see datetime strftime/strptime format)
        envelope_extents
            Optional, either a list of dicts or a dict, dict is in esri envelope feature specification
        region_name
            Optional, if provided will override envelope_extents and use the extents of the matching gpkg file
        return_geometry
            if True, will return the geometry of the survey as well.
        return_ids_only
            if True, will only return the object ids of the surveys matching this query
        return_count_only
            if True, will only return the number of surveys matching this survey
        return_extent_only
            if True, will only return the extents of the surveys matching this query
        include_fields
            Optional, if provided, will only return these fields for each survey queried

        Returns
        -------
        dict
            json dict of the data matching the query
        """

        self._validate_query_parameters(start_date, end_date, date_string_format, envelope_extents, region_name, include_fields)
        object_count = 0
        feature_data = {}
        for cnt, envelope in enumerate(self.envelope_extents):
            self._print(f'Operating on area extents number {cnt + 1}...')
            # first pass, see if the return is going to be larger than 1000 records, just get the IDs first
            object_ids = self._query_object_ids(envelope)
            # now query for the data, with a query for each chunk of object ids
            total_length = len(object_ids)
            object_count += total_length
            if object_ids:
                start_index = 0
                end_index = min(500, total_length)
                runs = int(np.ceil(total_length / end_index))
                for run_idx in range(runs):
                    self._print(f'Chunk {run_idx + 1} of {runs}...')
                    chunk_ids = object_ids[start_index:end_index]  # the object ids for this chunk
                    query_url = self._build_query_url(envelope, return_geometry, return_ids_only, return_count_only,
                                                      return_extent_only, only_these_object_ids=chunk_ids)
                    query_data = self.connect_to_server(query_url)  # response object from request
                    if query_data is not None:
                        json_data = query_data.json()
                        if not feature_data:
                            feature_data = json_data
                        else:
                            feature_data['features'].extend(json_data['features'])
                    start_index = end_index
                    end_index = min(end_index + 500, total_length)
            else:
                self._print(f'Unable to find any surveys with the following query: {self._build_query_url(envelope, False, True, False, False)}', logging.ERROR)
        self._print(f'NCEI query complete, found {object_count} surveys matching this query')
        return feature_data


class MultibeamQuery(NceiQuery):
    """
    Query for all surveys on NCEI that have multibeam files available for download.  You can take the returned download_url
    to lookup the data here:

    https://data.ngdc.noaa.gov/platforms/ocean/ships
    """

    def __init__(self, logger: logging.Logger = None, regions_folder: str = None):
        super().__init__(logger=logger, regions_folder=regions_folder)
        self.data_type = 'multibeam_dynamic'
        self.data_format = 'MB'
        self.rest_level = 0
        self.fields = ['SURVEY_ID', 'PLATFORM', 'SURVEY_YEAR', 'SOURCE', 'NGDC_ID', 'CHIEF_SCIENTIST', 'INSTRUMENT',
                       'FILE_COUNT', 'TRACK_LENGTH', 'TOTAL_TIME', 'BATHY_BEAMS', 'AMP_BEAMS', 'SIDESCANS', 'ENTERED_DATE',
                       'DOWNLOAD_URL', 'SHAPE', 'START_TIME', 'END_TIME', 'OBJECTID']

    def _build_date_query(self):
        where_statement = ''
        start_date, end_date = self._dates_to_text()
        if start_date:
            where_statement += f"START_TIME+>%3D+date%27{start_date}%27"
            if end_date:
                where_statement += '+AND+'
        if end_date:
            where_statement += f'END_TIME+<%3D+date%27{end_date}%27'
        if not where_statement:
            where_statement = '1%3D1'
        return where_statement


class BagQuery(NceiQuery):
    """
    Query for all surveys on NCEI that have BAG files available for download.  You can take the returned download_url
    to lookup the data here:

    https://www.ngdc.noaa.gov/nos/
    """

    def __init__(self, logger: logging.Logger = None, regions_folder: str = None):
        super().__init__(logger=logger, regions_folder=regions_folder)
        self.data_type = 'nos_hydro_dynamic'
        self.data_format = 'BAG'
        self.rest_level = 0
        self.fields = ['SURVEY_ID', 'DATE_SURVEY_BEGIN', 'DATE_SURVEY_END', 'DATE_MODIFY_DATA', 'DATE_SURVEY_APPROVAL',
                       'DATE_ADDED', 'SURVEY_YEAR', 'DIGITAL_DATA', 'LOCALITY', 'SUBLOCALITY', 'PLATFORM', 'PRODUCT_ID',
                       'BAGS_EXIST', 'DOWNLOAD_URL', 'DECADE', 'PUBLISH', 'OBJECTID', 'SHAPE']

    def _build_date_query(self):
        where_statement = ''
        start_date, end_date = self._dates_to_text()
        if start_date:
            where_statement += f"DATE_SURVEY_BEGIN+>%3D+date%27{start_date}%27"
            if end_date:
                where_statement += '+AND+'
        if end_date:
            where_statement += f'DATE_SURVEY_END +<%3D+date%27{end_date}%27'
        if not where_statement:
            where_statement = '1%3D1'
        return where_statement


class BpsQuery(NceiQuery):
    """
    Query for all surveys on NCEI that have point data available for download.  You can take the returned download_url
    to lookup the data here:

    https://www.ngdc.noaa.gov/nos/
    """

    def __init__(self, logger: logging.Logger = None, regions_folder: str = None):
        super().__init__(logger=logger, regions_folder=regions_folder)
        self.data_type = 'nos_hydro_dynamic'
        self.data_format = 'BPS'
        self.rest_level = 1
        self.fields = ['SURVEY_ID', 'DATE_SURVEY_BEGIN', 'DATE_SURVEY_END', 'DATE_MODIFY_DATA', 'DATE_SURVEY_APPROVAL',
                       'DATE_ADDED', 'SURVEY_YEAR', 'DIGITAL_DATA', 'LOCALITY', 'SUBLOCALITY', 'PLATFORM', 'PRODUCT_ID',
                       'BAGS_EXIST', 'DOWNLOAD_URL', 'DECADE', 'PUBLISH', 'OBJECTID', 'SHAPE']

    def _build_date_query(self):
        where_statement = ''
        start_date, end_date = self._dates_to_text()
        if start_date:
            where_statement += f"DATE_SURVEY_BEGIN+>%3D+date%27{start_date}%27"
            if end_date:
                where_statement += '+AND+'
        if end_date:
            where_statement += f'DATE_SURVEY_END +<%3D+date%27{end_date}%27'
        if not where_statement:
            where_statement = '1+%3D+1'
        return where_statement


if __name__ == '__main__':
    # get the ship and survey name for all surveys in the given region that have raw multibeam files on NCEI
    # include the regions_folder argument if you want to use something other than scrape_variables.region_folder
    # query = MultibeamQuery(regions_folder=r"C:\source\esd_process\esd_process\region_geopackages")
    query = MultibeamQuery()
    rawmbes_data = query.query(region_name='LA_LongBeach_WGS84', include_fields=('PLATFORM', 'SURVEY_ID'))

    # get the download link for all the surveys in the given region that have BAG files on NCEI
    query = BagQuery()
    links_to_bagfiles = query.query(region_name='LA_LongBeach_WGS84', include_fields=('DOWNLOAD_URL',))

    # get the download link for all the surveys in the given region that have point files on NCEI
    query = BpsQuery()
    links_to_pointfiles = query.query(region_name='LA_LongBeach_WGS84', include_fields=('DOWNLOAD_URL',))
