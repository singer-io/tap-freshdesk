from tap_tester import connections, runner, menagerie
from base import FreshdeskBaseTest


class TestFreshdeskInterruptedSyncRemoveStream(FreshdeskBaseTest):
    """Test tap's ability to recover from an interrupted sync"""

    @staticmethod
    def name():
        return "tt_freshdesk_interrupted_sync_remove_stream_test"

    def get_properties(self):
        """
        Maintain states for start_date and end_date
        """
        return {
            'start_date' : '2022-07-19T00:00:00Z'
        }

    def test_run(self):

        # Test for removing any stream from state
        self.run_interrupted_sync("groups")

        # Test for removing currently syncing stream from state
        self.run_interrupted_sync("roles")

    def run_interrupted_sync(self, removed_stream):
        """
        Testing that if a sync job is interrupted and state is saved with `currently_syncing`(stream),
        the next sync job kicks off and the tap picks back up on that `currently_syncing` stream.
        - Verify behavior is consistent when a stream is removed from the selected list between initial and resuming sync.
        """
        streams_to_test = {"roles", "agents", "groups", "companies"}
        conn_id = connections.ensure_connection(self)
        expected_replication_methods = self.expected_replication_method()
        expected_replication_keys = self.expected_replication_keys()

        start_date = self.dt_to_ts(self.get_properties().get("start_date"), self.START_DATE_FORMAT)

        # Run a discovery job
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Partition catalogs for use in table/field selection
        test_catalogs = [catalog for catalog in found_catalogs
                           if catalog.get('stream_name') in streams_to_test]
        self.perform_and_verify_table_and_field_selection(conn_id, test_catalogs, select_all_fields=True)

        # Run a sync
        self.run_and_verify_sync(conn_id)

        # Acquire records from target output
        full_sync_records = runner.get_records_from_target_output()
        full_sync_state = menagerie.get_state(conn_id)

        # Create new connection for another sync
        conn_id_2 = connections.ensure_connection(self)

        # Add a stream between syncs
        streams_to_test = streams_to_test - {removed_stream}
        found_catalogs = self.run_and_verify_check_mode(conn_id_2)

        test_catalogs = [catalog for catalog in found_catalogs
                           if catalog.get('stream_name') in streams_to_test]

        # Add new stream to selected list
        self.perform_and_verify_table_and_field_selection(conn_id_2, test_catalogs, select_all_fields=True)

        # Set state in with a currently syncing stream

        interrupted_state = {
            "currently_syncing": "roles",
            "bookmarks": {
                "agents": {
                    "updated_at": "2022-08-25T04:35:47.000000Z"
                },
                "companies": {
                    "updated_at": "2022-08-22T13:58:07.000000Z"
                },
                "groups": {
                    "updated_at": "2022-08-18T05:13:56.000000Z"
                },
                "roles": {
                    "updated_at": "2022-07-19T11:49:58.000000Z"
                }
            }
        }

        menagerie.set_state(conn_id_2, interrupted_state)

        # Run another sync
        self.run_and_verify_sync(conn_id_2)

        # Acquire records from target output
        interrupted_sync_records = runner.get_records_from_target_output()
        final_state = menagerie.get_state(conn_id_2)
        currently_syncing = final_state.get('currently_syncing')

        # Checking resuming sync resulted in a successfully saved state
        with self.subTest():

            # Verify sync is not interrupted by checking currently_syncing in the state for sync
            self.assertIsNone(currently_syncing)

            # Verify bookmarks are saved
            self.assertIsNotNone(final_state.get('bookmarks'))


            full_sync_bookmark = full_sync_state["bookmarks"]
            final_bookmark = final_state["bookmarks"]
            interrupted_repo_bookmark = interrupted_state["bookmarks"]

            for stream in list(streams_to_test) + [removed_stream]:
                with self.subTest(stream=stream):

                    # Expected values
                    expected_replication_method = expected_replication_methods[stream]
                    expected_primary_keys = list(self.expected_primary_keys()[stream])

                    # Gather results
                    full_records = [message['data'] for message in
                                    full_sync_records.get(stream, {}).get('messages', []) ]
                    full_record_count = len(full_records)

                    if stream != removed_stream:
                        interrupted_records = [message['data'] for message in
                                            interrupted_sync_records.get(stream, {}).get('messages', [])]
                        interrupted_record_count = len(interrupted_records)
                    else:
                        self.assertNotIn(stream, interrupted_sync_records.keys())

                    if expected_replication_method == self.INCREMENTAL:
                        expected_replication_key = next(iter(expected_replication_keys[stream]))
                        full_sync_stream_bookmark = self.dt_to_ts(full_sync_bookmark.get(stream, {}).get("updated_at"), self.BOOKMARK_FORMAT)

                        if stream in interrupted_repo_bookmark.keys():
                            interrupted_bookmark = self.dt_to_ts(interrupted_repo_bookmark[stream]["updated_at"], self.BOOKMARK_FORMAT)
                            final_sync_stream_bookmark = self.dt_to_ts(final_bookmark.get(stream, {}).get("updated_at"), self.BOOKMARK_FORMAT)

                            if stream != removed_stream:

                                # Verify state ends with the same value for common streams after both full and interrupted syncs
                                self.assertEqual(full_sync_stream_bookmark, final_sync_stream_bookmark)

                                # Verify resuming sync only replicates records with replication key values greater or equal to
                                # the interrupted_state for streams that were completed, replicated during the interrupted sync.
                                for record in interrupted_records:
                                    with self.subTest(record_primary_key=record[expected_primary_keys[0]]):
                                        rec_time = self.dt_to_ts(record[expected_replication_key], self.RECORD_REPLICATION_KEY_FORMAT)
                                        self.assertGreaterEqual(rec_time, interrupted_bookmark)

                                        # Verify all interrupted recs are in full recs
                                        self.assertIn(record, full_records,  msg='Incremental table record in interrupted sync not found in full sync')

                                # Record count for all streams of interrupted sync match expectations
                                full_records_after_interrupted_bookmark = 0
                                for record in full_records:
                                    rec_time = self.dt_to_ts(record[expected_replication_key], self.RECORD_REPLICATION_KEY_FORMAT)
                                    self.assertGreater(rec_time, start_date, msg=f"{expected_replication_key} {stream} {record}")

                                    if (rec_time >= interrupted_bookmark):
                                        full_records_after_interrupted_bookmark += 1

                                self.assertGreaterEqual(full_records_after_interrupted_bookmark, interrupted_record_count, \
                                                    msg="Expected max {} records in each sync".format(full_records_after_interrupted_bookmark))
                            else:
                                # Verify the bookmark has not advanced for the removed stream
                                self.assertEqual(final_sync_stream_bookmark, interrupted_bookmark)
                        else:
                            # verify we collected records that have the same replication value as a bookmark for streams that are already synced
                            self.assertGreater(interrupted_record_count, 0)

                    else:
                        # Verify full table streams do not save bookmarked values after a successful sync
                        self.assertNotIn(stream, full_sync_bookmark.keys())
                        self.assertNotIn(stream, final_bookmark.keys())

                        # Verify first and second sync have the same records
                        self.assertEqual(full_record_count, interrupted_record_count)
                        for rec in interrupted_records:
                            self.assertIn(rec, full_records, msg='Full table record in interrupted sync not found in full sync')

                    # Verify at least 1 record was replicated for each stream
                    if stream != removed_stream:
                        self.assertGreater(interrupted_record_count, 0)

                        print(f"{stream} resumed sync records replicated: {interrupted_record_count}")