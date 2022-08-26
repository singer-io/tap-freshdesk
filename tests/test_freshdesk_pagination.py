from math import ceil
from tap_tester import menagerie, connections, runner
import re

from base import FreshdeskBaseTest

class PaginationTest(FreshdeskBaseTest):

    def name(self):
        return "tap_tester_freshdesk_pagination_test"

    def test_name(self):
        print("Pagination Test for tap-freshdesk")

    def test_run(self):

        # Page size for pagination supported streams
        page_size = 100

        # Instantiate connection
        conn_id = connections.ensure_connection(self)

        # Add supported streams 1 by 1
        expected_streams = self.expected_streams() - {"time_entries", "satisfaction_ratings"}
        
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Table and field selection
        test_catalogs = [catalog for catalog in found_catalogs
                         if catalog.get('stream_name') in expected_streams]

        self.perform_and_verify_table_and_field_selection(conn_id, test_catalogs)

        sync_record_count = self.run_and_verify_sync(conn_id)
        sync_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(sync_records.keys())
        self.assertSetEqual(expected_streams, synced_stream_names)

        # Test by stream
        for stream in expected_streams:
            with self.subTest(stream=stream):
                # Not able to generate more data as roles stream requires pro account.
                # So, updating page_sie according to data available.
                if stream == "roles" or stream == "ticket_fields":
                    page_size = 2
                else:
                    page_size = 100
                # Expected values
                expected_primary_keys = self.expected_primary_keys()[stream]
                
                # Collect information for assertions from syncs 1 & 2 base on expected values
                record_count_sync = sync_record_count.get(stream, 0)
                primary_keys_list = [tuple(message.get('data').get(expected_pk)
                                     for expected_pk in expected_primary_keys)
                                     for message in sync_records.get(stream).get('messages')
                                     if message.get('action') == 'upsert']

                # Verify that for each stream you can get multiple pages of data
                self.assertGreater(record_count_sync, page_size,
                                   msg="The number of records is not over the stream max limit")
                
                # Chunk the replicated records (just primary keys) into expected pages
                pages = []
                page_count = ceil(len(primary_keys_list) / page_size)
                for page_index in range(page_count):
                    page_start = page_index * page_size
                    page_end = (page_index + 1) * page_size
                    pages.append(set(primary_keys_list[page_start:page_end]))

                # Verify by primary keys that data is unique for each page
                for current_index, current_page in enumerate(pages):
                    with self.subTest(current_page_primary_keys=current_page):

                        for other_index, other_page in enumerate(pages):
                            if current_index == other_index:
                                continue  # don't compare the page to itself

                            self.assertTrue(
                                current_page.isdisjoint(other_page), msg=f'other_page_primary_keys={other_page}'
                            )