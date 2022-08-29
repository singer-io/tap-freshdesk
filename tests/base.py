import os
import unittest
import dateutil.parser
import datetime
from datetime import datetime as dt
from datetime import timedelta
import time

from tap_tester import menagerie, runner, connections, LOGGER

class FreshdeskBaseTest(unittest.TestCase):

    REPLICATION_KEYS = "valid-replication-keys"
    PRIMARY_KEYS = "table-key-properties"
    FOREIGN_KEYS = "table-foreign-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    INCREMENTAL = "INCREMENTAL"
    FULL = "FULL_TABLE"

    start_date = ""
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z" # %H:%M:%SZ
    BOOKMARK_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

    OBEYS_START_DATE = "obey-start-date"
    
    #######################################
    #  Tap Configurable Metadata Methods  #
    #######################################

    def setUp(self):
        missing_envs = [x for x in [
            'TAP_FRESHDESK_API_KEY',
            'TAP_FRESHDESK_SUBDOMAIN',
        ] if os.getenv(x) is None]
        if missing_envs:
            raise Exception("Missing environment variables: {}".format(missing_envs))

    @staticmethod
    def get_type():
        return "platform.freshdesk"

    @staticmethod
    def tap_name():
        return "tap-freshdesk"

    def get_properties(self, original: bool = True):
        """
        Maintain states for start_date and end_date
        :param original: set to false to change the start_date or end_date
        """
        return_value = {
            'start_date' : '2019-01-04T00:00:00Z'
        }
        if original:
            return return_value

        # Reassign start and end dates
        return_value["start_date"] = self.start_date
        return return_value

    def get_credentials(self):
        return {
            'api_key': os.getenv('TAP_FRESHDESK_API_KEY'),
            'domain': os.getenv('TAP_FRESHDESK_SUBDOMAIN'),
        }

    def required_environment_variables(self):
        return set(['TAP_FRESHDESK_API_KEY',
                    'TAP_FRESHDESK_SUBDOMAIN'])

    def expected_metadata(self):
        """The expected streams and metadata about the streams"""
        return  {
            "agents": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"updated_at"},
                self.OBEYS_START_DATE: True
            },
            "companies": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"updated_at"},
                self.OBEYS_START_DATE: True
            },
            "conversations": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"updated_at"},
                self.OBEYS_START_DATE: True
            },
            "groups": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"updated_at"},
                self.OBEYS_START_DATE: True
            },
            "roles": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"updated_at"},
                self.OBEYS_START_DATE: True
            },
            "satisfaction_ratings": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"updated_at"},
                self.OBEYS_START_DATE: True
            },
            "tickets": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"updated_at"},
                self.OBEYS_START_DATE: True
            },
            "time_entries": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"updated_at"},
                self.OBEYS_START_DATE: True
            },
            "contacts": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"updated_at"},
                self.OBEYS_START_DATE: True
            },
        }

    #############################
    #  Common Metadata Methods  #
    #############################

    def expected_primary_keys(self):
        """
        Return a dictionary with key of table name
        and value as a set of primary key fields
        """
        return {table: properties.get(self.PRIMARY_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_automatic_fields(self):
        """
        Return a dictionary with key of table name 
        and value as the primary keys and replication keys
        """
        pks = self.expected_primary_keys()
        rks = self.expected_replication_keys()

        return {stream: rks.get(stream, set()) | pks.get(stream, set())
                for stream in self.expected_streams()}

    def expected_replication_method(self):
        """
        Return a dictionary with key of table name 
        and value of replication method
        """
        return {table: properties.get(self.REPLICATION_METHOD, None)
                for table, properties
                in self.expected_metadata().items()}

    def expected_streams(self):
        """A set of expected stream names"""
        return set(self.expected_metadata().keys())

    def expected_replication_keys(self):
        """
        Return a dictionary with key of table name
        and value as a set of replication key fields
        """
        return {table: properties.get(self.REPLICATION_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    ##########################
    #  Common Test Actions   #
    ##########################

    def run_and_verify_check_mode(self, conn_id):
        """
        Run the tap in check mode and verify it succeeds.
        This should be ran prior to field selection and initial sync.

        Return the connection id and found catalogs from menagerie.
        """
        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['stream_name'], found_catalogs))
        LOGGER.info(found_catalog_names)
        self.assertSetEqual(self.expected_streams(), found_catalog_names, msg="discovered schemas do not match")
        LOGGER.info("discovered schemas are OK")

        return found_catalogs

    def run_and_verify_sync(self, conn_id):
        """
        Run a sync job and make sure it exited properly.
        Return a dictionary with keys of streams synced
        and values of records synced for each stream
        """
        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify actual rows were synced
        sync_record_count = runner.examine_target_output_file(self,
                                                              conn_id,
                                                              self.expected_streams(),
                                                              self.expected_primary_keys())
        total_row_count = sum(sync_record_count.values())
        self.assertGreater(total_row_count, 0,
                           msg="failed to replicate any data: {}".format(sync_record_count))
        LOGGER.info("total replicated row count: {}".format(total_row_count))

        return sync_record_count

    def perform_and_verify_table_and_field_selection(self,
                                                     conn_id,
                                                     test_catalogs,
                                                     select_all_fields=True):
        """
        Perform table and field selection based off of the streams to select
        set and field selection parameters.
        Verify this results in the expected streams selected and all or no
        fields selected for those streams.
        """

        # Select all available fields or select no fields from all testable streams
        self.select_all_streams_and_fields(
            conn_id=conn_id, catalogs=test_catalogs, select_all_fields=select_all_fields
        )

        catalogs = menagerie.get_catalogs(conn_id)

        # Ensure our selection affects the catalog
        expected_selected = [tc.get('stream_name') for tc in test_catalogs]
        for cat in catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat['stream_id'])

            # Verify all testable streams are selected
            selected = catalog_entry.get('annotated-schema').get('selected')
            LOGGER.info("Validating selection on {}: {}".format(cat['stream_name'], selected))
            if cat['stream_name'] not in expected_selected:
                self.assertFalse(selected, msg="Stream selected, but not testable.")
                continue # Skip remaining assertions if we aren't selecting this stream
            self.assertTrue(selected, msg="Stream not selected.")

            if select_all_fields:
                # Verify all fields within each selected stream are selected
                for field, field_props in catalog_entry.get('annotated-schema').get('properties').items():
                    field_selected = field_props.get('selected')
                    LOGGER.info("\tValidating selection on {}.{}: {}".format(
                        cat['stream_name'], field, field_selected))
                    self.assertTrue(field_selected, msg="Field not selected.")
            else:
                # Verify only automatic fields are selected
                expected_automatic_fields = self.expected_automatic_fields().get(cat['stream_name'])
                selected_fields = self.get_selected_fields_from_metadata(catalog_entry['metadata'])
                self.assertEqual(expected_automatic_fields, selected_fields)

    @staticmethod
    def get_selected_fields_from_metadata(metadata):
        selected_fields = set()
        for field in metadata:
            is_field_metadata = len(field['breadcrumb']) > 1
            inclusion_automatic_or_selected = (
                field['metadata']['selected'] is True or \
                field['metadata']['inclusion'] == 'automatic'
            )
            if is_field_metadata and inclusion_automatic_or_selected:
                selected_fields.add(field['breadcrumb'][1])
        return selected_fields

    @staticmethod
    def select_all_streams_and_fields(conn_id, catalogs, select_all_fields: bool = True):
        """Select all streams and all fields within streams"""
        for catalog in catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])

            non_selected_properties = []
            if not select_all_fields:
                # Get a list of all properties so that none are selected
                non_selected_properties = schema.get('annotated-schema', {}).get(
                    'properties', {}).keys()

            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema, [], non_selected_properties)

    ################################
    #  Tap Specific Test Actions   #
    ################################

    def dt_to_ts(self, dtime, format):
        """Convert datetime with a format to timestamp"""
        date_stripped = int(time.mktime(dt.strptime(dtime, format).timetuple()))
        return date_stripped

    def calculated_states_by_stream(self, current_state):
        """
        Look at the bookmarks from a previous sync and set a new bookmark
        value based off timedelta expectations. This ensures the subsequent sync will replicate
        at least 1 record but, fewer records than the previous sync.

        Sufficient test data is required for this test to cover a given stream.
        An incremental replication stream must have at least two records with
        replication keys that differ by some time span.

        If the test data is changed in the future this may break expectations for this test.
        """
        timedelta_by_stream = {stream: [0, 12, 0]  # {stream_name: [days, hours, minutes], ...}
                               for stream in current_state['bookmarks'].keys()}
        
        stream_to_calculated_state = {stream: "" for stream in current_state['bookmarks'].keys()}
        for stream, state in current_state['bookmarks'].items():
            state_key, state_value = next(iter(state.keys())), next(iter(state.values()))
            state_as_datetime = dateutil.parser.parse(state_value)

            days, hours, minutes = timedelta_by_stream[stream]
            calculated_state_as_datetime = state_as_datetime - timedelta(days=days, hours=hours, minutes=minutes)

            state_format = self.BOOKMARK_FORMAT
            calculated_state_formatted = datetime.datetime.strftime(calculated_state_as_datetime, state_format)
            stream_to_calculated_state[stream] = {state_key: calculated_state_formatted}

        return stream_to_calculated_state