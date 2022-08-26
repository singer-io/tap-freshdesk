import tap_tester.connections as connections
import tap_tester.runner as runner
import tap_tester.menagerie as menagerie
import dateutil.parser
from datetime import timedelta
from datetime import datetime as dt

from base import FreshdeskBaseTest

class BookmarkTest(FreshdeskBaseTest):
    """Test tap sets a separate bookmark for PUBLISHED and SCHEDULED tweets and 
    respects it for the next sync of a tweets stream"""
    
    def name(self):
        return "tap_tester_freshdesk_bookmark_test"

    def test_run(self):
        """
        Verify that for each stream you can do a sync that records bookmarks.
        That the bookmark is the maximum value sent to the target for the replication key.
        That a second sync respects the bookmark
            All data of the second sync is >= the bookmark from the first sync
            The number of records in the 2nd sync is less than the first (This assumes that
                new data added to the stream is done at a rate slow enough that you haven't
                doubled the amount of data from the start date to the first sync between
                the first sync and second sync run in this test)
        PREREQUISITE
        For EACH stream that is incrementally replicated, there are multiple rows of data with
            different values for the replication key
        """
        
        streams_to_test = {'tickets', 'contacts'}
        expected_replication_keys = self.expected_replication_keys()

        ##########################################################################
        # First Sync
        ##########################################################################
        conn_id = connections.ensure_connection(self)

        # Run in check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Table and field selection
        catalog_entries = [catalog for catalog in found_catalogs
                           if catalog.get('tap_stream_id') in streams_to_test]

        self.perform_and_verify_table_and_field_selection(
            conn_id, catalog_entries)

        # Run a first sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()
        first_sync_bookmarks = menagerie.get_state(conn_id)

        ##########################################################################
        # Update State Between Syncs
        ##########################################################################

        new_states = {'bookmarks': dict()}
        simulated_states = self.calculated_states_by_stream(first_sync_bookmarks)
        for stream, new_state in simulated_states.items():
            new_states['bookmarks'][stream] = new_state
        menagerie.set_state(conn_id, new_states)

        ##########################################################################
        # Second Sync
        ##########################################################################

        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_bookmarks = menagerie.get_state(conn_id)

        ##########################################################################
        # Test By Stream
        ##########################################################################

        for stream in streams_to_test:
            with self.subTest(stream=stream):

                replication_key = next(iter(expected_replication_keys[stream]))
                if stream == 'tickets':
                    filter_streams = ['deleted', 'spam']
                if stream == 'contacts':
                    filter_streams = ['deleted', 'blocked']

                # collect information for assertions from syncs 1 & 2 base on expected values
                second_sync_count = second_sync_record_count.get(stream, 0)
                first_sync_messages = [record.get('data') for record in
                                       first_sync_records.get(
                                           stream, {}).get('messages', [])
                                       if record.get('action') == 'upsert']
                second_sync_messages = [record.get('data') for record in
                                        second_sync_records.get(
                                            stream, {}).get('messages', [])
                                        if record.get('action') == 'upsert']

                # Get bookmark for tickets/contacts stream
                first_bookmark_value = first_sync_bookmarks.get('bookmarks', {}).get(stream, {}).get(replication_key)
                second_bookmark_value = second_sync_bookmarks.get('bookmarks', {}).get(stream, {}).get(replication_key)                        
                
                first_bookmark_value_ts = self.dt_to_ts(first_bookmark_value, self.BOOKMARK_FORMAT)
                second_bookmark_value_ts = self.dt_to_ts(second_bookmark_value, self.BOOKMARK_FORMAT)

                simulated_bookmark_value = self.dt_to_ts(new_states['bookmarks'][stream][replication_key], self.BOOKMARK_FORMAT)

                # Get bookmark for contacts_deleted/tickets_deleted
                first_1_bookmark_value = first_sync_bookmarks.get('bookmarks', {}).get(stream + '_' +filter_streams[0], {}).get(replication_key)
                second_1_bookmark_value = second_sync_bookmarks.get('bookmarks', {}).get(stream + '_' +filter_streams[0], {}).get(replication_key)
                
                first_1_bookmark_value_ts = self.dt_to_ts(first_bookmark_value, self.BOOKMARK_FORMAT)
                second_1_bookmark_value_ts = self.dt_to_ts(second_bookmark_value, self.BOOKMARK_FORMAT)
                
                simulated_1_bookmark_value = self.dt_to_ts(new_states['bookmarks'][stream + '_' +filter_streams[0]][replication_key], self.BOOKMARK_FORMAT)

                # Get bookmark for contacts_blocked/tickets_spam
                first_2_bookmark_value = first_sync_bookmarks.get('bookmarks', {}).get(stream + '_' +filter_streams[1], {}).get(replication_key)
                second_2_bookmark_value = second_sync_bookmarks.get('bookmarks', {}).get(stream + '_' +filter_streams[1], {}).get(replication_key)

                first_2_bookmark_value_ts = self.dt_to_ts(first_bookmark_value, self.BOOKMARK_FORMAT)
                second_2_bookmark_value_ts = self.dt_to_ts(second_bookmark_value, self.BOOKMARK_FORMAT)
                
                simulated_2_bookmark_value = self.dt_to_ts(new_states['bookmarks'][stream + '_' +filter_streams[1]][replication_key], self.BOOKMARK_FORMAT)
                
                # Verify the first sync sets bookmarks of the expected form
                self.assertIsNotNone(first_bookmark_value)
                self.assertIsNotNone(first_1_bookmark_value)
                self.assertIsNotNone(first_2_bookmark_value)
                
                # Verify the second sync sets bookmarks of the expected form
                self.assertIsNotNone(second_bookmark_value)
                self.assertIsNotNone(second_1_bookmark_value)
                self.assertIsNotNone(second_2_bookmark_value)

                for record in first_sync_messages:

                    # Verify the first sync bookmark value is the max replication key value for a given stream
                    replication_key_value = record.get(replication_key)
                    if record.get(filter_streams[0], None) in ["false", None] and record.get(filter_streams[1], None) in ["false", None]:
                        # Verify the first sync bookmark value is the max replication key value for a tickets/contacts stream
                        self.assertLessEqual(
                            replication_key_value, first_bookmark_value_ts,
                            msg=("First sync bookmark for {} was set incorrectly, a record with a greater replication-key value was synced.".format(stream))
                        )
                    elif record.get(filter_streams[0]) == "true":
                        # Verify the first sync bookmark value is the max replication key value for a contacts_deleted/tickets_deleted
                        self.assertLessEqual(
                            replication_key_value, first_1_bookmark_value_ts,
                            msg=("First sync bookmark for {}_{} was set incorrectly, a record with a greater replication-key value was synced.".format(stream, filter_streams[0]))
                        )
                    elif record.get(filter_streams[1]) == "true":
                        # Verify the first sync bookmark value is the max replication key value for a contacts_blocked/tickets_spam
                        self.assertLessEqual(
                            replication_key_value, first_2_bookmark_value_ts,
                            msg=("First sync bookmark for {}_{} was set incorrectly, a record with a greater replication-key value was synced.".format(stream, filter_streams[1]))
                        )
                
                for record in second_sync_messages:
                    
                    # Verify the first sync bookmark value is the max replication key value for a given stream
                    replication_key_value = record.get(replication_key)
                    if record.get(filter_streams[0], None) in ["false", None] and record.get(filter_streams[1], None) in ["false", None]:                        
                        # Verify the first sync bookmark value is the max replication key value for a tickets/contacts stream
                        self.assertGreaterEqual(replication_key_value, simulated_bookmark_value,
                                                msg=("Second sync records do not respect the previous bookmark for {}.".format(stream)))

                        self.assertLessEqual(
                            replication_key_value, second_bookmark_value_ts,
                            msg=("First sync bookmark for {} was set incorrectly, a record with a greater replication-key value was synced.".format(stream))
                        )
                    elif record.get(filter_streams[0]) == "true":
                        # Verify the first sync bookmark value is the max replication key value for a contacts_deleted/tickets_deleted
                        self.assertGreaterEqual(replication_key_value, simulated_1_bookmark_value,
                                                msg=("Second sync records do not respect the previous bookmark for {}_{}.".format(stream, filter_streams[0])))

                        self.assertLessEqual(
                            replication_key_value, second_1_bookmark_value_ts,
                            msg=("First sync bookmark for {}_{} was set incorrectly, a record with a greater replication-key value was synced.".format(stream, filter_streams[0]))
                        )
                    elif record.get(filter_streams[1]) == "true":
                        # Verify the first sync bookmark value is the max replication key value for a contacts_blocked/tickets_spam
                        self.assertGreaterEqual(replication_key_value, simulated_2_bookmark_value,
                                                msg=("Second sync records do not respect the previous bookmark for {}_{}.".format(stream, filter_streams[0])))

                        self.assertLessEqual(
                            replication_key_value, second_2_bookmark_value_ts,
                            msg=("First sync bookmark for {}_{} was set incorrectly, a record with a greater replication-key value was synced.".format(stream, filter_streams[1]))
                        )

                # Verify at least 1 record was replicated in the second sync
                self.assertGreater(
                    second_sync_count, 0, msg="We are not fully testing bookmarking for {}".format(stream))