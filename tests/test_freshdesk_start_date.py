from tap_tester import connections, runner, LOGGER

from base import FreshdeskBaseTest


class FreshdeskStartDateTest(FreshdeskBaseTest):
    """Test that the start_date configuration is respected"""

    start_date_1 = ""
    start_date_2 = ""

    @staticmethod
    def name():
        return "tap_tester_freshdesk_start_date_test"

    def test_run(self):
        """   
        • Verify that a sync with a later start date has at least one record synced
          and less records than the 1st sync with a previous start date
        • Verify that each stream has less records than the earlier start date sync
        • Verify all data from later start data has bookmark values >= start_date
        • Verify that the minimum bookmark sent to the target for the later start_date sync
          is greater than or equal to the start date
        • Verify by primary key values, that all records in the 1st sync are included in the 2nd sync.
        """

        self.start_date_1 = self.get_properties().get('start_date')
        self.start_date_2 = "2022-07-19T00:00:00Z"

        self.start_date = self.start_date_1

        start_date_1_epoch = self.dt_to_ts(self.start_date_1, self.START_DATE_FORMAT)
        start_date_2_epoch = self.dt_to_ts(self.start_date_2, self.START_DATE_FORMAT)

        expected_streams = self.expected_streams()

        ##########################################################################
        ### First Sync
        ##########################################################################

        # Instantiate connection
        conn_id_1 = connections.ensure_connection(self)

        # Run check mode
        found_catalogs_1 = self.run_and_verify_check_mode(conn_id_1)
        
        # Table and field selection
        test_catalogs_1_all_fields = [catalog for catalog in found_catalogs_1
                                      if catalog.get('stream_name') in expected_streams]
        self.perform_and_verify_table_and_field_selection(conn_id_1, test_catalogs_1_all_fields, select_all_fields=True)

        # Run initial sync
        record_count_by_stream_1 = self.run_and_verify_sync(conn_id_1)
        synced_records_1 = runner.get_records_from_target_output()

        ##########################################################################
        ### Update START DATE Between Syncs
        ##########################################################################

        LOGGER.info("REPLICATION START DATE CHANGE: {} ===>>> {} ".format(self.start_date, self.start_date_2))
        self.start_date = self.start_date_2

        ##########################################################################
        ### Second Sync
        ##########################################################################

        # Create a new connection with the new start_date
        conn_id_2 = connections.ensure_connection(self, original_properties=False)

        # Run check mode
        found_catalogs_2 = self.run_and_verify_check_mode(conn_id_2)

        # Table and field selection
        test_catalogs_2_all_fields = [catalog for catalog in found_catalogs_2
                                      if catalog.get('stream_name') in expected_streams]
        self.perform_and_verify_table_and_field_selection(conn_id_2, test_catalogs_2_all_fields, select_all_fields=True)

        # Run sync
        record_count_by_stream_2 = self.run_and_verify_sync(conn_id_2)
        synced_records_2 = runner.get_records_from_target_output()

        # Verify that sync 2 has at least one record synced and less records than sync 1
        self.assertGreater(sum(record_count_by_stream_2.values()), 0)
        self.assertGreater(sum(record_count_by_stream_1.values()), sum(record_count_by_stream_2.values()))

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # Expected values
                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_replication_keys = self.expected_replication_keys()[stream]
                expected_metadata = self.expected_metadata()[stream]

                # Collect information for assertions from syncs 1 & 2 base on expected values
                record_count_sync_1 = record_count_by_stream_1.get(stream, 0)
                record_count_sync_2 = record_count_by_stream_2.get(stream, 0)
                primary_keys_list_1 = [tuple(message.get('data', {}).get(expected_pk) for expected_pk in expected_primary_keys)
                                       for message in synced_records_1.get(stream, {'messages': []}).get('messages')
                                       if message.get('action') == 'upsert']
                primary_keys_list_2 = [tuple(message.get('data', {}).get(expected_pk) for expected_pk in expected_primary_keys)
                                       for message in synced_records_2.get(stream, {'messages': []}).get('messages')
                                       if message.get('action') == 'upsert']

                primary_keys_sync_1 = set(primary_keys_list_1)
                primary_keys_sync_2 = set(primary_keys_list_2)

                # Verify that sync 2 has at least one record synced
                self.assertGreater(record_count_sync_2, 0)

                if expected_metadata.get(self.OBEYS_START_DATE):
                    
                    # Expected bookmark key is one element in set so directly access it
                    bookmark_keys_list_1 = [message.get('data').get(next(iter(expected_replication_keys))) for message in synced_records_1.get(stream).get('messages')
                                            if message.get('action') == 'upsert']
                    bookmark_keys_list_2 = [message.get('data').get(next(iter(expected_replication_keys))) for message in synced_records_2.get(stream).get('messages')
                                            if message.get('action') == 'upsert']

                    bookmark_key_sync_1 = set(bookmark_keys_list_1)
                    bookmark_key_sync_2 = set(bookmark_keys_list_2)

                    # Verify bookmark key values are greater than or equal to start date of sync 1
                    for bookmark_key_value in bookmark_key_sync_1:
                        self.assertGreaterEqual(
                            self.dt_to_ts(bookmark_key_value, self.BOOKMARK_FORMAT), start_date_1_epoch,
                            msg="Report pertains to a date prior to our start date.\n" +
                            "Sync start_date: {}\n".format(self.start_date_1) +
                                "Record date: {} ".format(bookmark_key_value)
                        )

                    # Verify bookmark key values are greater than or equal to start date of sync 2
                    for bookmark_key_value in bookmark_key_sync_2:
                        self.assertGreaterEqual(
                            self.dt_to_ts(bookmark_key_value, self.BOOKMARK_FORMAT), start_date_2_epoch,
                            msg="Report pertains to a date prior to our start date.\n" +
                            "Sync start_date: {}\n".format(self.start_date_2) +
                                "Record date: {} ".format(bookmark_key_value)
                        )

                    # Verify the number of records replicated in sync 1 is greater than the number
                    # of records replicated in sync 2 for stream
                    self.assertGreater(record_count_sync_1, record_count_sync_2)

                    # Verify the records replicated in sync 2 were also replicated in sync 1
                    self.assertTrue(primary_keys_sync_2.issubset(primary_keys_sync_1))

                # Currently all streams obey start date.  Leaving this in incase one of the two remaining
                # streams are implemented in the future and do not obey start date
                # else:
                #     print("Stream {} does NOT obey start_date".format(stream))                  
                #     # Verify that the 2nd sync with a later start date replicates the same number of
                #     # records as the 1st sync.
                #     self.assertEqual(record_count_sync_2, record_count_sync_1)

                #     # Verify by primary key the same records are replicated in the 1st and 2nd syncs
                #     self.assertSetEqual(primary_keys_sync_1, primary_keys_sync_2)