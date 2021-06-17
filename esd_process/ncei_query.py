import os
from typing import Union
from datetime import datetime
import requests
import logging

from esd_process.regions import Regions


class QueryBase:
    def __init__(self):
        self.start_date = None
        self.end_date = None
        self.date_string_format = '%m/%d/%y'
        self.envelope_extents = None
        self.region_name = None
        self.logger = None

    def _return_formatted_date(self, datedata: Union[str, datetime]):
        if isinstance(datedata, str):
            datedata = datetime.strptime(datedata, self.date_string_format)
        datedata = f'{datedata.strftime("%Y-%m-%d")}'
        return datedata

    def _print(self, msg: str, lvl: int = logging.INFO):
        if self.logger:
            self.logger.log(lvl, msg)
        else:
            print(msg)

    def _dates_to_text(self):
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
        new_extent = f"%7B%27xmin%27%3A+{extent['xmin']}%2C+%27ymin%27%3A+{extent['ymin']}%2C+%27xmax%27%3A+{extent['xmax']}%2C+%27ymax%27%3A+{extent['ymax']}%7D"
        return new_extent


class MultibeamQuery(QueryBase):
    def __init__(self):
        super().__init__()
        self.data_type = 'multibeam_dynamic'
        self.data_format = 'MB'
        self.rest_level = 0

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


class BagQuery(QueryBase):
    def __init__(self):
        super().__init__()
        self.data_type = 'nos_hydro_dynamic'
        self.data_format = 'BAG'
        self.rest_level = 0

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


class BpsQuery(QueryBase):
    def __init__(self):
        super().__init__()
        self.data_type = 'nos_hydro_dynamic'
        self.data_format = 'BPS'
        self.rest_level = 1
        r"\NOAA_NCEI_OCS\BPS\Original"

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


class NceiQuery(MultibeamQuery):
    def __init__(self, logger: logging.Logger = None):
        super().__init__()
        self.logger = logger
        self.regions = Regions(logger=self.logger)
        self.rest_url = f'https://gis.ngdc.noaa.gov/arcgis/rest/services/web_mercator/{self.data_type}/MapServer/{self.rest_level}'

        self.geometry_type = 'esriGeometryEnvelope'
        self.input_coordinate_system = '4326'  # wgs84
        self.geometry_query = 'esriSpatialRelIntersects'

        self.return_geometry = False
        self.return_ids_only = True
        self.return_count_only = False
        self.return_extent_only = False

        self.output_format = 'json'

    def _build_extents_query(self):
        if self.envelope_extents and self.region_name:
            self._print('Both region name and envelope extents provided, region name will be used to supersede the region name', logging.WARNING)
        if self.region_name:
            self.envelope_extents = self.regions.return_region_by_name(self.region_name, return_bounds=True)
        if isinstance(self.envelope_extents, dict):
            self.envelope_extents = [self.envelope_extents]
        if not self.envelope_extents:
            self.envelope_extents = [""]


    def query(self, start_date: Union[str, datetime] = None, end_date: Union[str, datetime] = None, date_string_format: str = '%m/%d/%y',
              envelope_extents: Union[list, dict] = None, region_name: str = None):
        self.start_date = start_date
        self.end_date = end_date
        self.date_string_format = date_string_format
        self.envelope_extents = envelope_extents
        self.region_name = region_name
        where_statement = self._build_date_query()
        self._build_extents_query()
        for envelope in self.envelope_extents:
            query_url = self.rest_url + f'/query?where={where_statement}&text=&objectIds=&time=&geometry={self._extents_to_hex_formatting(envelope)}'
            query_url += f'&geometryType={self.geometry_type}&inSR={self.input_coordinate_system}&spatialRel={self.geometry_query}'
            query_url += f'&relationParam=&outFields=&returnGeometry={str(self.return_geometry).lower()}&returnTrueCurves=false'
            query_url += f'&maxAllowableOffset=&geometryPrecision=&outSR=&having=&returnIdsOnly={str(self.return_ids_only).lower()}'
            query_url += f'&returnCountOnly={str(self.return_count_only).lower()}&orderByFields=&groupByFieldsForStatistics='
            query_url += f'&outStatistics=&returnZ=false&returnM=false&gdbVersion=&historicMoment=&returnDistinctValues=false&resultOffset='
            query_url += f'&resultRecordCount=&queryByDistance=&returnExtentOnly={str(self.return_extent_only).lower()}&datumTransformation='
            query_url += f'&parameterValues=&rangeValues=&quantizationParameters=&f={self.output_format}'

            query_data = requests.get(query_url)
            json_data = query_data.json()
            print(json_data)

            # if 'objectIds' in dataListRequestJSON and dataListRequestJSON['objectIds'] is not None:
            #     objectIDs.extend(dataListRequestJSON['objectIds'])
            #     objectNum += len(objectIDs) - 1
            # elif 'error' in dataListRequestJSON:
            #     LOGGER.error(f"Error in query response: {dataListRequestJSON} for query {dataList}")
            # else:
            #     LOGGER.info(f"No Object IDs returned by query: {dataList}")
            #
            # paramString = f'\tParameters:\n\t\tStart Date: {start_date}' + \
            #               f'\n\t\tEnd Date: {end_date}\n\t\tDate Fields: {query_fields}'
            # LOGGER.info(paramString)


if __name__ == '__main__':
    query = NceiQuery()
    query.query(region_name='LA_LongBeach_WGS84')
