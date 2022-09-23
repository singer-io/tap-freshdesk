from sre_parse import State
from tap_tester import menagerie, connections, runner

from base import FreshdeskBaseTest


class FreshdeskBookmarks(FreshdeskBaseTest):
    """Test to verify bookmark logic for parent-child sync."""
  
    @staticmethod
    def name():
        return "tap_tester_freshdesk_parent_child_sync"

    def test_run(self):
        minimum_bookmark = "2022-08-01T10:07:03.000000Z"
        maximum_bookmark = "2022-08-17T10:07:03.000000Z"
        new_state = {
            "bookmarks": {
                "tickets": {
                    "updated_at": maximum_bookmark
                },
                "tickets_deleted": {
                    "updated_at": maximum_bookmark
                },
                "tickets_spam": {
                    "updated_at": maximum_bookmark
                },
                "conversations": {
                    "updated_at": minimum_bookmark
                }
            }
        }
        stream_to_test = {'conversations'}
        self.run_test(new_state, stream_to_test,
                      minimum_bookmark, maximum_bookmark)

        new_state = {
            "bookmarks": {
                "tickets": {
                    "updated_at": minimum_bookmark
                },
                "tickets_deleted": {
                    "updated_at": minimum_bookmark
                },
                "tickets_spam": {
                    "updated_at": minimum_bookmark
                },
                "conversations": {
                    "updated_at": maximum_bookmark
                }
            }
        }
        stream_to_test = {'tickets'}
        self.run_test(new_state, stream_to_test,
                      minimum_bookmark, maximum_bookmark)

    def run_test(self, new_state, stream_to_test, minimum_bookmark, maximum_bookmark):
        """
        Test case to verify the working of parent-child streams
        Prerequisite:
            - Set child bookmark is earlier than parent bookmark
            - Set Parent bookmark is earlier than child bookmark
            
        • Verify that minimum bookmark is used for selected parent-child stream.
        • Verify that records between the bookmark values are replicated.
        """

        # To collect "time_entries", "satisfaction_ratings"(child streams of "tickets") pro account is needed.
        # Skipping them for now.
        expected_streams = {'tickets', 'conversations'}

        expected_replication_keys = self.expected_replication_keys()

        conn_id = connections.ensure_connection(self)

        menagerie.set_state(conn_id, new_state)

        # Run in check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Table and field selection
        catalog_entries = [catalog for catalog in found_catalogs
                           if catalog.get('tap_stream_id') in expected_streams]

        self.perform_and_verify_table_and_field_selection(
            conn_id, catalog_entries)

        # Run a sync job using orchestrator
        sync_record_count = self.run_and_verify_sync(conn_id)
        sync_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # Collect information for assertions
                sync_messages = [record.get('data') for record in
                                 sync_records.get(stream, {'messages': []}).get('messages')
                                 if record.get('action') == 'upsert']

                replication_key = list(expected_replication_keys[stream])[0]
                sync_start_date_ts = self.dt_to_ts(new_state.get("bookmarks", {stream: None}).get(
                    stream).get(replication_key), self.BOOKMARK_FORMAT)

                for record in sync_messages:
                    # Verify that the minimum bookmark is used for selected parent-child stream.
                    replication_key_value = self.dt_to_ts(
                        record.get(replication_key), self.BOOKMARK_FORMAT)
                    
                    # Verify that the records replicated for the selected streams are greater than or
                    # equal to given bookmark.
                    self.assertGreaterEqual(
                        replication_key_value, sync_start_date_ts,
                        msg="Sync records do not respect the provided bookmark."
                    )

                # Verify that atleast 1 record is getting replicated for streams to test.
                self.assertGreater(sync_record_count.get(stream, 0), 0)

                if stream in stream_to_test:
                    minimum_bookmark_value_ts = self.dt_to_ts(minimum_bookmark, self.BOOKMARK_FORMAT)
                    maximum_bookmark_value_ts = self.dt_to_ts(maximum_bookmark, self.BOOKMARK_FORMAT)
                    records_between_dates = []
                    for record in sync_messages:
                        replication_key_value = self.dt_to_ts(record.get(replication_key), self.BOOKMARK_FORMAT)

                        if minimum_bookmark_value_ts <= replication_key_value <= maximum_bookmark_value_ts:
                            records_between_dates.append(record)

                    # Verify that records between the bookmark values are replicated for streams in streams to test.
                    self.assertIsNotNone(records_between_dates)
