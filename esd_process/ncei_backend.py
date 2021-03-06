import os
import sqlite3
import logging

from esd_process import scrape_variables


class BaseBackend:
    """
    Base class for backends, must be inherited to use
    """
    def __init__(self):
        self.output_folder = None

        # these attributes are populated during scrape and saved to the backend (database)
        self.downloaded_success_count = 0
        self.downloaded_error_count = 0
        self.ignored_count = 0
        self.ship_name = ''
        self.survey_name = ''
        self.survey_url = ''
        self.raw_data_path = ''
        self.processed_data_path = ''
        self.grid_path = ''

        self._backend_logger = logging.getLogger(scrape_variables.logger_name + '_backend')
        self._backend_logger.setLevel(scrape_variables.logger_level)

    def _configure_backend(self):
        raise NotImplementedError('_configure_backend must be implemented for this backend to operate')

    def _create_backend(self):
        raise NotImplementedError('_create_backend must be implemented for this backend to operate')

    def _add_survey(self):
        raise NotImplementedError('_add_survey must be implemented for this backend to operate')

    def _check_for_survey(self, shipname: str, surveyname: str):
        raise NotImplementedError('_check_for_survey must be implemented for this backend to operate')

    def _check_for_grid(self, shipname: str, surveyname: str):
        raise NotImplementedError('_check_for_grid must be implemented for this backend to operate')

    def _remove_survey(self, shipname: str, surveyname: str):
        raise NotImplementedError('_remove_survey must be implemented for this backend to operate')

    def _close_backend(self):
        raise NotImplementedError('_close_backend must be implemented for this backend to operate')


class SqlBackend(BaseBackend):
    """
    python sqlite3 backend, will store metdata about surveys in the 'surveys' table in the self.database_file sqlite3 file.
    """
    def __init__(self):
        super().__init__()
        self.database_file = None
        self._cur = None
        self._conn = None

    def _configure_backend(self):
        """
        Creates the database_file if it does not exist.  Will also run _create_backend to generate a blank table
        if that table does not exist.
        """
        self.database_file = os.path.join(self.output_folder, 'survey_database.sqlite3')
        needs_create = False
        if not os.path.exists(self.database_file):
            needs_create = True
        self._conn = sqlite3.connect(self.database_file)
        self._cur = self._conn.cursor()
        if needs_create:
            self._create_backend()

    def _create_backend(self):
        """
        Generate a new sqlite3 database for the project
        """
        self._backend_logger.log(logging.INFO, f'Generating new table "surveys" for scrape data...')
        # create the single table that we need to store survey metadata
        self._cur.execute('''CREATE TABLE surveys 
                             (ship_name text, survey text, downloaded_success int, downloaded_error int, 
                             ignored int, raw_data_path text, processed_data_path text, grid_path text)''')
        self._conn.commit()

    def _add_survey(self):
        """
        Add a new entry for this survey to the database
        """
        if self.ship_name and self.survey_name:
            if not self._check_for_survey(self.ship_name, self.survey_name):
                self._backend_logger.log(logging.INFO, f'Adding new data for {self.ship_name}/{self.survey_name} to sqlite database')
                self._cur.execute(f'INSERT INTO surveys VALUES ("{self.ship_name.lower()}","{self.survey_name.lower()}",'
                                  f'{self.downloaded_success_count},{self.downloaded_error_count},{self.ignored_count},'
                                  f'"{self.raw_data_path}","{self.processed_data_path}","{self.grid_path}")')
                self._conn.commit()
        # reset data to defaults to get ready for next survey
        self.ship_name = ''
        self.survey_name = ''
        self.downloaded_success_count = 0
        self.downloaded_error_count = 0
        self.ignored_count = 0
        self.raw_data_path = ''
        self.processed_data_path = ''
        self.grid_path = ''

    def _check_for_survey(self, shipname: str, surveyname: str):
        """
        Check to see if this survey exists in the database
        """
        data = self._cur.execute(f'SELECT * FROM surveys WHERE ship_name="{shipname.lower()}" and survey="{surveyname.lower()}"')
        if len(data.fetchall()) > 0:
            return True
        else:
            return False

    def _check_for_grid(self, shipname: str, surveyname: str):
        """
        Check to see if this survey has a grid path in the database (lets you know if you have successfully created a
        grid with this survey)
        """
        data = self._cur.execute(f'SELECT * FROM surveys WHERE ship_name="{shipname.lower()}" and survey="{surveyname.lower()}" and grid_path != ""')
        if len(data.fetchall()) > 0:
            return True
        else:
            return False

    def _remove_survey(self, shipname: str, surveyname: str):
        """
        Remove the entry for this survey from the database
        """
        self._cur.execute(f'DELETE FROM surveys WHERE shipname="{shipname.lower()}" and survey="{surveyname.lower()}"')
        self._conn.commit()

    def _close_backend(self):
        """
        Close the database connection
        """
        self._conn.close()
