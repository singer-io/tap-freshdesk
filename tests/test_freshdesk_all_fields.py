import os

from tap_tester import runner, connections, menagerie

from base import FreshdeskBaseTest

# As we are not able to generate following fields by Freshdesk UI, so removed it form expectation list.
KNOWN_MISSING_FIELDS = {
    'tickets': {
        'facebook_id',
        'description',
        'description_text',
        'twitter_id',
        'name',
        'phone',
        'email'
    },
    'groups': {
        'auto_ticket_assign',
        'agent_ids'
    },
    'agents': {
        'group_ids',
        'role_ids'
    },
    'contacts': {
        'view_all_tickets',
        'other_companies',
        'other_emails',
        'tags',
        'avatar'
    }
}

class TestFreshdeskAllFields(FreshdeskBaseTest):
    """Test that with all fields selected for a stream automatic and available fields are  replicated"""

    @staticmethod
    def name():
        return "tap_tester_freshdesk_all_fields"

    def test_run(self):
        """
        • Verify no unexpected streams were replicated
        • Verify that more than just the automatic fields are replicated for each stream. 
        • Verify all fields for each stream are replicated
        """
        
        # To collect "time_entries", "satisfaction_ratings" pro account is needed. Skipping them for now.
        expected_streams = self.expected_streams() - {"time_entries", "satisfaction_ratings"}

        # Instantiate connection
        conn_id = connections.ensure_connection(self)

        # Run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Table and field selection
        test_catalogs_all_fields = [catalog for catalog in found_catalogs
                                    if catalog.get('stream_name') in expected_streams]
        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs_all_fields, select_all_fields=True,
        )

        # Grab metadata after performing table-and-field selection to set expectations
        stream_to_all_catalog_fields = dict() # used for asserting all fields are replicated
        for catalog in test_catalogs_all_fields:
            stream_id, stream_name = catalog['stream_id'], catalog['stream_name']
            catalog_entry = menagerie.get_annotated_schema(conn_id, stream_id)
            fields_from_field_level_md = [md_entry['breadcrumb'][1]
                                          for md_entry in catalog_entry['metadata']
                                          if md_entry['breadcrumb'] != []]
            stream_to_all_catalog_fields[stream_name] = set(fields_from_field_level_md)

        # Run initial sync
        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(expected_streams, synced_stream_names)

        for stream in expected_streams:
            with self.subTest(stream=stream):
                # Expected values
                expected_automatic_fields = self.expected_automatic_fields().get(stream)

                # Get all expected keys
                expected_all_keys = stream_to_all_catalog_fields[stream]

                messages = synced_records.get(stream)
                # Collect actual values
                actual_all_keys = set()
                for message in messages['messages']:
                    if message['action'] == 'upsert':
                        actual_all_keys.update(message['data'].keys())
                    
                expected_all_keys = expected_all_keys - KNOWN_MISSING_FIELDS.get(stream, set())

                # Verify all fields for a stream were replicated
                self.assertGreater(len(expected_all_keys), len(expected_automatic_fields))
                self.assertTrue(expected_automatic_fields.issubset(expected_all_keys), msg=f'{expected_automatic_fields-expected_all_keys} is not in "expected_all_keys"')
                self.assertSetEqual(expected_all_keys, actual_all_keys)