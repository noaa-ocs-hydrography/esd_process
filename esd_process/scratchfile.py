from esd_process.ncei_scrape import *

logging.basicConfig()
logfile = os.path.join('D:', f'logfile_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt')

# base log, we can get to it later by getting the 'scraper' log by name
logger = logging.getLogger(scrape_variables.logger_name)
logger.setLevel(scrape_variables.logger_level)

# used if you use the http connection debug, see import statements
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True

filelogger = logging.FileHandler(logfile)
filelogger.setLevel(scrape_variables.logger_level)
fmat = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
filelogger.setFormatter(logging.Formatter(fmat))
logging.getLogger().addHandler(filelogger)

raw_data_path = r'D:\falkor\FK005B'
multibeamfiles = [os.path.join(raw_data_path, fil) for fil in os.listdir(raw_data_path) if
                  os.path.splitext(fil)[1] in scrape_variables.processing_extensions]
processed_data_path = r'D:\falkor\FK005B_processed'
processed, gridded = run_kluster(multibeamfiles, processed_data_path, logger=logger, vertical_reference='ellipse')

############################################################################################

from HSTB.kluster.fqpr_convenience import convert_multibeam
fq = convert_multibeam([r"D:\falkor\FK005B\0000_20120917_155941_FK_EM710.all",
                        r"D:\falkor\FK005B\0001_20120917_161100_FK_EM710.all",
                        r"D:\falkor\FK005B\0002_20120917_164100_FK_EM710.all",
                        r"D:\falkor\FK005B\0003_20120917_171100_FK_EM710.all",
                        r"D:\falkor\FK005B\0004_20120917_174100_FK_EM710.all",
                        r"D:\falkor\FK005B\0015_20120917_230218_FK_EM710.all",
                        r"D:\falkor\FK005B\0016_20120917_233128_FK_EM710.all",
                        r"D:\falkor\FK005B\0017_20120917_233353_FK_EM710.all",
                        r"D:\falkor\FK005B\0018_20120917_233451_FK_EM710.all",
                        r"D:\falkor\FK005B\0019_20120917_233451_FK_EM710.all",
                        r"D:\falkor\FK005B\0020_20120917_233937_FK_EM710.all",
                        r"D:\falkor\FK005B\0005_20120917_181100_FK_EM710.all",
                        r"D:\falkor\FK005B\0006_20120917_184101_FK_EM710.all",
                        r"D:\falkor\FK005B\0007_20120917_190227_FK_EM710.all",
                        r"D:\falkor\FK005B\0008_20120917_193227_FK_EM710.all",
                        r"D:\falkor\FK005B\0009_20120917_200227_FK_EM710.all",
                        r"D:\falkor\FK005B\0010_20120917_203227_FK_EM710.all",
                        r"D:\falkor\FK005B\0011_20120917_210227_FK_EM710.all",
                        r"D:\falkor\FK005B\0012_20120917_213228_FK_EM710.all",
                        r"D:\falkor\FK005B\0013_20120917_220228_FK_EM710.all",
                        r"D:\falkor\FK005B\0014_20120917_223228_FK_EM710.all"])