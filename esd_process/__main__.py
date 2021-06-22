import sys
import argparse

from esd_process.ncei_scrape import main


def str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


class SmartFormatter(argparse.HelpFormatter):

    def _split_lines(self, text, width):
        if text.startswith('R|'):
            return text[2:].splitlines()
        # this is the RawTextHelpFormatter._split_lines
        return argparse.HelpFormatter._split_lines(self, text, width)


if __name__ == "__main__":  # run from command line
    parser = argparse.ArgumentParser(formatter_class=SmartFormatter)
    subparsers = parser.add_subparsers(help='Available processing commands within esd_process currently',
                                       dest='esd_function')

    nceiscrape_help = 'R|Crawl the NCEI site, download all multibeam files matching the extension and process using Kluster (if available)\n'
    nceiscrape_help += r'example: nceiscrape -o c:\path\to\output\folder -cs NAD83 -vf waterline -gtype single_resolution -res 8.0 -gf bag'
    nceiscrape = subparsers.add_parser('nceiscrape', help=nceiscrape_help)
    nceiscrape.add_argument('-o', '--output_directory', required=False, type=str, nargs='?', const='', default='',
                            help='optional, a path to an empty folder (it will create if it doesnt exist) to hold the downloaded/processed data, default is the current working directory')
    nceiscrape.add_argument('-cs', '--coordinate_system', required=False, type=str, nargs='?', const='NAD83', default='NAD83',
                            help='optional, processed coordinate system to use, one of NAD83 and WGS84, default is NAD83')
    nceiscrape.add_argument('-vf', '--vertical_reference', required=False, type=str, nargs='?', const='waterline', default='waterline',
                            help="optional, vertical reference to use for the processed data, one of 'ellipse' 'mllw' 'NOAA_MLLW' 'NOAA_MHW' (NOAA references require vdatum which isn't hooked up in here just yet), default is waterline")
    nceiscrape.add_argument('-r', '--region', required=False, type=str, nargs='?', const=None, default=None,
                            help="optional, the name of one of the region_geopackages that you want to limit your search to")
    nceiscrape.add_argument('-rdir', '--region_directory', required=False, type=str, nargs='?', const=None, default=None,
                            help="optional, the directory that contains the region geopackages, default is the esd_process/region_geopackages directory")
    nceiscrape.add_argument('-gtype', '--grid_type', required=False, type=str, nargs='?', const='single_resolution', default='single_resolution',
                            help="optional, the grid type you want to build with Kluster for each dataset, one of 'single_resolution', 'variable_resolution_tile', default is single_resolution")
    nceiscrape.add_argument('-res', '--resolution', required=False, type=float, nargs='?', const=None, default=None,
                            help='optional (only for single_resolution), the resolution of the grid in meters, set this if you do not want to use the auto-resolution option')
    nceiscrape.add_argument('-gf', '--grid_format', required=False, type=str, nargs='?', const='bag', default='bag',
                            help="optional, the grid format exported by kluster, one of 'csv', 'geotiff', 'bag', default is bag")

    args = parser.parse_args()
    if not args.esd_function:
        main()
    else:
        main(args.output_directory, args.coordinate_system, args.vertical_reference, args.region, args.region_directory,
             args.grid_type, args.resolution, args.grid_format)
