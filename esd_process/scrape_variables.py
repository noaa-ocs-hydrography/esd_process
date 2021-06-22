import os
import logging

# where you want the downloaded data to go, otherwise uses default_output_directory
output_directory = 'D:'
# set this to only download data within a specific region
region_folder = os.path.join(os.path.dirname(__file__), 'region_geopackages')
region = 'LA_LongBeach_WGS84'

default_output_directory = os.path.join(os.path.dirname(__file__), 'working_directory')
download_retries = 20
server_reconnect_retries = 10
query_chunk_size = 500  # max number of records we can query at once
extensions = ('.mb58.gz', '.mb59.gz')
logger_level = logging.INFO
logger_name = 'scraper'

# kluster specific variables
kluster_coordinate_system = 'NAD83'  # one of NAD83 and WGS84
kluster_vertical_reference = 'waterline'  # one of 'ellipse' 'mllw' 'NOAA_MLLW' 'NOAA_MHW' (NOAA references require vdatum which isn't hooked up in here just yet)
kluster_grid_type = 'single_resolution'  # one of 'single_resolution', 'variable_resolution_tile'
kluster_resolution = None  # set this to pick the resolution of the grid, None will auto pick, variable resolution must be None
kluster_grid_format = 'bag'  # one of 'csv', 'geotiff', 'bag'

# exclude vessels that are decommissioned or are not likely to ever get a modern kongsberg sonar system, shortens the
# time necessary to crawl the site
exclude_vessels = (
    'ahi',  # NOAA Research Vessel Acoustic Habitat Investigator, has a Reson
    'akademik_tryoshnikov',  # Russian scientific vessel, ELAC system
    'amundsen',  # Canadian CG, has an EM302, but all the data is in the canadian arctic
    # 'atlantic_surveyor',  # Independent vessel?  I see Leidos projects with them, appear to have a Reson system
    # 'atlantis',  # WHOI vessel, has an EM122
    'atlantis_ii',  # Used to be WHOI?  Now owned by someone else, had a SeaBeam
    'auriga',  # Used to run a towed sidescan for a PMEL project?  I see one project with a SeaBeam
    'baruna_jaya_iv',  # Indonesian Research vessel with a ELAC sonar
    # 'bay_hydrographer',  # not sure if this is the BHI or the BHII, leaving it unexcluded
    'bellows',  # Florida Inst of Oceanography, replaced by the Hogarth, had the older Simrad RAW format
    # 'bligh',  # Irish Seabed Survey vessel, has an older Simrad system, but is still operational
    'boris_petrov',  # Russian scientific vessel, unknown system, generates DAT files
    # 'bowditch',  # Still active USNS vessel, has older SIMRAD system
    # 'bruce_c._heezen',  # Still active USNS vessel, has older SIMRAD system
    # 'cape_fear',  # UNC Wilmington vessel, appears to have an EM3002, logging the older RAW format
    # 'celtic_explorer',  # Marine Institute in Ireland, Runs an EM302 and an EM2040
    # 'celtic_voyager',  # Marine Institute in Ireland, Runs an EM2040
    # 'coastal_surveyor',  # CCOM vessel, can have multibeam mounted, leave it in just in case
    # 'conamara',  # not sure about this one, there is a connemara, is that it?  Leave it in just in case
    # 'concat',  # Marine Institute in Ireland, Runs an older SIMRAD system, but it is still active
    # 'cosantoir_bradan',  # Ireland IFI, has a Reson system
    'davidson',  # decommissioned NOAA vessel, had some URI sonar system
    # 'derek_m._baylis',  # Wylie Charters vessel, has a RESON system, but it is still active
    'discoverer',  # decommissioned NOAA vessel, had some SEABEAM sonar system
    'ducer',  # unsure, submits gsf files, thats all i know
    'endurance',  # scrapped British ship, had a RESON system
    # 'falkor',  # Schmidt Ocean vessel, has an EM710 and an EM302
    # 'fugro_americas',  # Fugro ship, has an EM302
    # 'fugro_brasilis',  # Fugro ship, has an EM302
    # 'fugro_discovery',  # Fugro ship, has an EM122
    # 'fugro_equator',  # Fugro ship, has an EM302
    # 'fugro_gauss',  # Fugro ship, has an EM122
    # 'fugro_searcher',  # Fugro ship, has an EM302
    # 'fugro_supporter',  # Fugro ship, has an EM122
    # 'geo',  # geological survey Ireland, has a Reson T20-P, but it is still active
    # 'granuaile',  # Irish vessel, has some ELAC system I think
    # 'harold_heath',  # Monterey Bay Univ, has a Reson system, but is still active
    # 'hawkbill',
    # 'healy',
    # 'henry_b._bigelow',
    # 'henson',  # USNS vessel
    # 'hi_ialakai',
    # 'inland_surveyor',
    # 'island_c',
    # 'james_clark_ross',
    # 'jean_charcot',
    # 'ka_imikai-o-kanaloa',
    # 'kairei',
    # 'keary',
    # 'kilo_moana',
    # 'knorr',
    # 'l_atalante',
    # 'laney_chouest',
    # 'littlehales',
    # 'look_down',
    # 'lost_coast_explorer',
    # 'louis_s._st-laurent',
    # 'luna_sea',
    # 'macginitie',
    # 'marcus_g._langseth',
    # 'maria_s._merian',
    # 'mary_sears',
    # 'maurice_ewing',
    # 'mcdonnell',
    # 'melville',
    # 'meteor',
    # 'moana_wave',
    # 'mt._mitchell',
    # 'nancy_foster',
    # 'nathaniel_b._palmer',
    # 'natsushima',
    # 'nautilus',
    # 'navo_vohs',
    # 'neil_armstrong',
    # 'nikolaj_strakhov',
    'nonpublic',  # I do not have access to this folder
    # 'northern_resolution',
    # 'ocean_alert',
    # 'ocean_surveyor',
    # 'okeanos_explorer',
    # 'onnuri',
    # 'oscar_elton_sette',
    # 'osprey',
    # 'pacific_star',
    # 'parke_snavely',
    # 'pathfinder',
    # 'pelagia',
    # 'potawaugh',
    # 'quicksilver',
    # 'rainier',
    # 'robert_d._conrad',
    # 'rocinante',
    # 'roger_revelle',
    # 'ronald_h._brown',
    # 'rude',
    # 'sally_ride',
    # 'sea_ark2',
    # 'sea_ducer',
    # 'seaark_1210',
    # 'seaark_s3001',
    # 'sikuliaq',
    # 'sirius',
    # 'sonne',
    # 'sumner',
    # 'suncoaster',
    # 'surf_surveyor',
    # 'surveyor',
    # 'swamp_fox',
    'test_mbsystem_kmall',  # I don't know what the hell this is
    # 'thomas_dowell',
    # 'thomas_g._thompson',
    # 'thomas_jefferson',
    # 'thomas_washington',
    # 'tonn',
    'unknown',  # appears to be a bunch of old data from who knows where
    # 'ventresca',
    # 'vidal_gormaz',
    # 'weatherbird_ii',
    # 'whiting',
    # 'yokosuka',
    # 'zephyr',
)
