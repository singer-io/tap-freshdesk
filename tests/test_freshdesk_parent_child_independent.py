from tap_tester import connections, runner

from base import FreshdeskBaseTest

class ParentChildIndependentTest(FreshdeskBaseTest):
    """
        Test case to verify that tap is working fine if only first-level child streams are selected
    """  
  
    def name(self):
        return "tap_tester_freshdesk_parent_child_test"

    def test_run(self):
        """
            Testing that tap is working fine if only child streams are selected
            • Verify that if only child streams are selected then only child streams are replicated.
        """

        # To collect "time_entries", "satisfaction_ratings"(child streams of "tickets") pro account is needed. 
        # Skipping them for now.
        child_streams = {'conversations'}
        
        # Instantiate connection
        conn_id = connections.ensure_connection(self)

        # Run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Table and field selection
        catalog_entries = [catalog for catalog in found_catalogs
                           if catalog.get('tap_stream_id') in child_streams]
        # Table and field selection
        self.perform_and_verify_table_and_field_selection(conn_id, catalog_entries)

        # Run initial sync
        self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(child_streams, synced_stream_names)