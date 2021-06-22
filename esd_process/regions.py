import os
import numpy as np
import logging
from osgeo import ogr

from shapely import wkt
from shapely.ops import cascaded_union

from esd_process import scrape_variables


class Regions:
    """
    Class to manage the region geopackages, will build the extents of the geopackages and allow for querying by name
    and position to return the correct region
    """
    def __init__(self, regions_folder: str = None, logger: logging.Logger = None):
        self.logger = logger
        if regions_folder:
            self.regions_folder = regions_folder
        else:
            self.regions_folder = scrape_variables.region_folder
        self.region_paths = [os.path.join(self.regions_folder, pkg) for pkg in os.listdir(self.regions_folder) if os.path.splitext(pkg)[1] == '.gpkg']
        self._print(f'Discovered {len(self.region_paths)} regions from region folder {self.regions_folder}')
        self.region_bounds = []
        self.region_wkt = []
        self._build_region_lists()

    def _print(self, msg: str, lvl: int = logging.INFO):
        """
        print a message to the console or to the logging handlers if a logging instance is in this class

        Parameters
        ----------
        msg
            message to print
        lvl
            logging level of the message
        """

        if self.logger:
            self.logger.log(lvl, msg)
        else:
            print(msg)

    def _build_region_lists(self):
        """
        on init, builds the extents of each region as an Esri envelope feature, adds them to a region_bounds list
        ex: {'xmin': -118.35, 'ymin': 33.6, 'xmax': -118.05, 'ymax': 33.83}
        on init, builds the wkt of each region as a list of wkt strings, adds to the region_wkt list
        ex: 'POLYGON ((-118.3499997 33.8250042,-118.1249774 33.8249921,...))
        """

        for regi in self.region_paths:
            try:
                self.region_bounds.append(region_envelope_from_geopackage(regi))
            except:
                self._print(f'Unable to build envelope bounds from geopackage: {regi}', logging.WARNING)
            try:
                self.region_wkt.append(region_wkt_from_geopackage(regi))
            except:
                self._print(f'Unable to build wkt from geopackage: {regi}', logging.WARNING)

    def return_region_by_name(self, region_name: str, return_bounds: bool = True, return_wkt: bool = False):
        """
        Query by name to return the region.  if return_bounds is true, returns the region bounds instead of the region
        """
        # try the full path if region_name is a full path
        match_region = [regi for regi in self.region_paths if regi == region_name]
        if not match_region:  # try with region_name just being the file name of the region
            # remove extension just in case it was provided in the region name
            region_name = os.path.splitext(region_name)[0]
            match_region = [regi for regi in self.region_paths if os.path.split(os.path.splitext(regi)[0])[1] == region_name]
        if len(match_region) > 1:
            self._print(f'Found multiple region matches with region_name {region_name}, returning the first: {match_region}', logging.ERROR)
        if len(match_region) == 0:
            self._print(f'No matching region for region name: {region_name}', logging.WARNING)
            return None

        if return_wkt:
            return self.region_wkt[self.region_paths.index(match_region[0])]
        elif return_bounds:
            return self.region_bounds[self.region_paths.index(match_region[0])]
        else:
            return match_region[0]

    def return_regions_by_position(self, lon: float, lat: float):
        """
        Query by position to return the region that contains that position within its extents
        """
        regions = []
        for cnt, rb in enumerate(self.region_bounds):
            for bounds in rb:
                if (lon >= bounds['xmin']) and (lon <= bounds['xmax']) and (lat >= bounds['ymin']) and (lat <= bounds['ymax']):
                    regions.append(self.region_paths[cnt])
        return regions

    def region_intersects(self, region_name: str, wkt_string: str):
        """
        Return True if the provided wkt_string intersects the given region
        """
        region_wkt = self.return_region_by_name(region_name, return_wkt=True)
        if region_wkt:
            return survey_intersects(wkt_string, region_wkt)


def region_envelope_from_geopackage(region_file: str):
    """
    Return the extent of a geopackage as an envelope feature.  Taken from National Bathymetric Source project codebase.

    Envelope is a dictionary containing the min/max x/y of a layer within a geopackage

    Parameters
    ----------
    region_file
        The filepath of the input region's geopackage polygon

    Returns
    -------
    list of dict
        A list of the bounding area(s) of a region

    """
    region_vector = ogr.Open(region_file)
    layer = region_vector.GetLayer()

    bounds = {}
    bounds_index = 0
    for feature in layer:
        if feature is not None:
            geom = feature.GetGeometryRef()
            points = geom.GetBoundary()
            if points.GetGeometryName() == 'MULTILINESTRING':
                for i in range(geom.GetGeometryCount()):
                    inner_geom = geom.GetGeometryRef(i)
                    inner_points = inner_geom.GetBoundary()
                    x_points, y_points = [], []
                    for j in range(inner_points.GetPointCount()):
                        # GetPoint returns a tuple not a Geometry
                        point = inner_points.GetPoint(j)
                        x_points.append(round(point[0], 2))
                        y_points.append(round(point[1], 2))
                    bounds[bounds_index] = (x_points, y_points)
                    bounds_index += 1
            else:
                x_points, y_points = [], []
                for i in range(0, points.GetPointCount()):
                    # GetPoint returns a tuple not a Geometry
                    point = points.GetPoint(i)
                    x_points.append(round(point[0], 2))
                    y_points.append(round(point[1], 2))
                bounds[bounds_index] = (x_points, y_points)
                bounds_index += 1
                break
    del region_vector

    return_bounds = []
    for key in bounds.keys():
        x_points, y_points = bounds[key]
        return_bounds.append({
            'xmin': np.nanmin(x_points),
            'ymin': np.nanmin(y_points),
            'xmax': np.nanmax(x_points),
            'ymax': np.nanmax(y_points)
            })

    return return_bounds


def region_wkt_from_geopackage(region_file: str):
    """
    Return the wkt of a geopackage.  Taken from National Bathymetric Source project codebase.

    the wkt is a string representation of the feature geometry
    ex: 'POLYGON ((-118.3499997 33.8250042,-118.1249774 33.8249921,...))

    Parameters
    ----------
    region_file
        The filepath of the input region's geopackage polygon

    Returns
    -------
    list of str
        A list of the wkt for each layer

    """
    region_vector = ogr.Open(region_file)
    layer = region_vector.GetLayer()

    region_geoms = []
    for feature in layer:
        if feature is not None:
            geom = feature.GetGeometryRef()
            geom_wkt = geom.ExportToWkt()
            region_geoms.append(geom_wkt)
    return region_geoms


def survey_intersects(survey_wkt: str, region_geoms: list):
    """
    Determine if the provided survey_wkt intersects with the region geometry

    Parameters
    ----------
    survey_wkt
        the wkt for the survey
    region_geoms
        list of wkt for the region

    Returns
    -------
    bool
        True if intersects
    """

    survey_geom = wkt.loads(survey_wkt)
    region_geom = []
    for r_wkt in region_geoms:
        region_geom.append(wkt.loads(r_wkt))
    region_geom = cascaded_union(region_geom)
    if region_geom.intersects(survey_geom):
        return True
    return False


if __name__ == '__main__':
    regi = Regions()
    print(regi.regions_folder)
    for cnt, regpath in enumerate(regi.region_paths):
        print(regpath + ': ' + str(regi.region_bounds[cnt]))
