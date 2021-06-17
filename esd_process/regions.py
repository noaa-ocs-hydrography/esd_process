import os
import numpy as np
import logging
from osgeo import ogr


class Regions:
    def __init__(self, regions_folder: str = None, logger: logging.Logger = None):
        self.logger = logger
        if regions_folder:
            self.regions_folder = regions_folder
        else:
            self.regions_folder = os.path.join(os.path.dirname(__file__), 'region_geopackages')
        self.regions = [os.path.join(self.regions_folder, pkg) for pkg in os.listdir(self.regions_folder) if os.path.splitext(pkg)[1] == '.gpkg']
        self._print(f'Discovered {len(self.regions)} regions from region folder {self.regions_folder}')
        self.region_bounds = []
        self._build_region_bounds()

    def __str__(self):
        print(f'Regions: Contains {len(self.regions)} regions:\n')
        for cnt, reg in enumerate(self.regions):
            print(f'{reg}: {self.region_bounds[cnt]}')

    def _print(self, msg: str, lvl: int = logging.INFO):
        if self.logger:
            self.logger.log(lvl, msg)
        else:
            print(msg)

    def _build_region_bounds(self):
        for reg in self.regions:
            try:
                self.region_bounds.append(region_envelope_from_geopackage(reg))
            except:
                self._print(f'Unable to build envelope bounds from geopackage: {reg}', logging.WARNING)

    def return_region_by_name(self, region_name: str, return_bounds: bool = True):
        # remove extension just in case it was provided in the region name
        region_name = os.path.splitext(region_name)[0]
        match_region = [reg for reg in self.regions if os.path.split(os.path.splitext(reg)[0])[1] == region_name]
        if len(match_region) > 1:
            self._print(f'Found multiple region matches with region_name {region_name}, returning the first: {match_region}', logging.ERROR)
        if len(match_region) == 0:
            self._print(f'No matching region for region name: {region_name}', logging.WARNING)
            return None
        if not return_bounds:
            return match_region[0]
        else:
            return self.region_bounds[self.regions.index(match_region[0])]

    def return_regions_by_position(self, lon: float, lat: float):
        regions = []
        for cnt, rb in enumerate(self.region_bounds):
            for bounds in rb:
                if (lon >= bounds['xmin']) and (lon <= bounds['xmax']) and (lat >= bounds['ymin']) and (lat <= bounds['ymax']):
                    regions.append(self.regions[cnt])
        return regions


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


if __name__ == '__main__':
    reg = Regions()
    reg.__str__()
