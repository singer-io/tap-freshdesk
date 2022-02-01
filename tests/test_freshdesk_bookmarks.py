"""Test tap check mode and metadata/annotated-schema."""
import re
import os
import pytz
import time
import dateutil.parser

from datetime import timedelta
from datetime import datetime

from tap_tester import menagerie, connections, runner

from base import FreshdeskBaseTest


class FreshdeskBookmarks(FreshdeskBaseTest):
    """Test incremental replication via bookmarks (without CRUD)."""

    start_date = ""
    
    @staticmethod
    def name():
        return "tt_freshdesk_bookmarks"

    def get_properties(self, original: bool = True):
        return_value = {
            #'start_date':  dt.today() - timedelta(days=5*365),
            'start_date':  '2016-02-09T00:00:00Z',
            #'end_date'  :  "2021-02-09T18:17:30.000000Z",
            #'start_date_with_fmt': dt.strftime(start_date, self.START_DATE_FORMAT),
        }

        return return_value
        
    @staticmethod
    def convert_state_to_utc(date_str):
        """
        Convert a saved bookmark value of the form '2020-08-25T13:17:36-07:00' to
        a string formatted utc datetime,
        in order to compare aginast json formatted datetime values
        """
        date_object = dateutil.parser.parse(date_str)
        date_object_utc = date_object.astimezone(tz=pytz.UTC)
        return datetime.strftime(date_object_utc, "%Y-%m-%dT%H:%M:%SZ")

    def calculated_states_by_stream(self, current_state):
        """
        Look at the bookmarks from a previous sync and set a new bookmark
        value based off timedelta expectations. This ensures the subsequent sync will replicate
        at least 1 record but, fewer records than the previous sync.

        Sufficient test data is required for this test to cover a given stream.
        An incrmeental replication stream must have at least two records with
        replication keys that differ by more than the lookback window.

        If the test data is changed in the future this will break expectations for this test.

        The following streams barely make the cut:

        campaigns "2021-02-09T18:17:30.000000Z"
                  "2021-02-09T16:24:58.000000Z"

        adsets    "2021-02-09T18:17:41.000000Z"
                  "2021-02-09T17:10:09.000000Z"

        leads     '2021-04-07T20:09:39+0000',
                  '2021-04-07T20:08:27+0000',
        """
        timedelta_by_stream = {stream: [2,0,0]  # {stream_name: [days, hours, minutes], ...}
                               for stream in self.expected_streams()}
        # Works with static start date to go back to 2017 Feb 8 22:xx:xx and pick up 5 of 6 records
        # including 2 of 3 conversations assocated with those 5 ticket records
        timedelta_by_stream['tickets'] = [698, 17, 15]
        #timedelta_by_stream['conversations'] = [1, 0 , 0]  # Child stream of tickets, no bookmarks

        # BUG https://jira.talendforge.org/browse/TDL-17559.  Redefining state to be closer to
        # expected format so the underlying code wont have to change as much after the JIRA fix
        current_state = {'bookmarks': current_state}
        del current_state['bookmarks']['tickets_deleted']  # Delete unexpected streams
        del current_state['bookmarks']['tickets_spam']
        #print("current_state: {}".format(current_state))
        #print("timedelta_by_stream['tickets']: {}".format(timedelta_by_stream['tickets']))

        # Expected format more like this
        # state = {
        #     'bookmarks': {
        #         stream_1: {'replicatione_key': replication_key_value},
        #         stream_2: {'replicatione_key': replication_key_value},
        #     },
        #     'other-keys': value,
        # }

        # Keep existing format for this method so it will work after bug fix
        stream_to_calculated_state = {stream: "" for stream in current_state['bookmarks'].keys()}
        for stream, state_value in current_state['bookmarks'].items():
            state_as_datetime = dateutil.parser.parse(state_value)

            days, hours, minutes = timedelta_by_stream[stream]
            calculated_state_as_datetime = state_as_datetime - timedelta(days=days, hours=hours, minutes=minutes)

            state_format = self.BOOKMARK_FORMAT
            calculated_state_formatted = datetime.strftime(calculated_state_as_datetime, state_format)

            stream_to_calculated_state[stream] = calculated_state_formatted

        return stream_to_calculated_state

    # function for verifying the date format
    def is_expected_date_format(self, date):
        try:
            # parse date
            datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError:
            # return False if date is in not expected format
            return False
        # return True in case of no error
        return True

    def test_run(self):
        expected_streams =  self.expected_streams()
        
        # Testing against ads insights objects
        self.start_date = self.get_properties()['start_date']
        #self.end_date = self.get_properties()['end_date']
        self.bookmarks_test(expected_streams)

    def bookmarks_test(self, expected_streams):
        """A Parametrized Bookmarks Test"""
        expected_replication_keys = self.expected_replication_keys()
        expected_replication_methods = self.expected_replication_method()

        ##########################################################################
        ### First Sync
        ##########################################################################

        conn_id = connections.ensure_connection(self, original_properties=False)

        # Run in check mode
        check_job_name = self.run_and_verify_check_mode(conn_id)

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()
        first_sync_bookmarks = menagerie.get_state(conn_id)
        
        ##########################################################################
        ### Update State Between Syncs
        ##########################################################################

        #new_states = {'bookmarks': dict()}
        new_states = {}
        simulated_states = self.calculated_states_by_stream(first_sync_bookmarks)
        for stream, new_state in simulated_states.items():
            #new_states['bookmarks'][stream] = new_state  # Save expected format
            new_states[stream] = new_state                # Send expected format
        menagerie.set_state(conn_id, new_states)

        # Common break point between syncs
        #import ipdb; ipdb.set_trace()
        #1+1
        
        ##########################################################################
        ### Second Sync
        ##########################################################################

        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_bookmarks = menagerie.get_state(conn_id)

        ##########################################################################
        ### Test By Stream
        ##########################################################################

        #for stream in expected_streams:
        test_streams = {'tickets'}
        for stream in test_streams:  # Add supported streams 1 by 1
            with self.subTest(stream=stream):

                # expected values
                expected_replication_method = expected_replication_methods[stream]

                # collect information for assertions from syncs 1 & 2 base on expected values
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)
                first_sync_messages = [record.get('data') for record in
                                       first_sync_records.get(stream).get('messages')
                                       if record.get('action') == 'upsert']
                second_sync_messages = [record.get('data') for record in
                                        second_sync_records.get(stream).get('messages')
                                        if record.get('action') == 'upsert']
                #first_bookmark_key_value = first_sync_bookmarks.get('bookmarks', {stream: None}).get(stream)
                first_bookmark_key_value = first_sync_bookmarks.get(stream)
                #second_bookmark_key_value = second_sync_bookmarks.get('bookmarks', {stream: None}).get(stream)
                second_bookmark_key_value = second_sync_bookmarks.get(stream)

                if expected_replication_method == self.INCREMENTAL:

                    # collect information specific to incremental streams from syncs 1 & 2
                    replication_key = next(iter(expected_replication_keys[stream]))
                    #first_bookmark_value = first_bookmark_key_value.get(replication_key)
                    first_bookmark_value = first_bookmark_key_value
                    #second_bookmark_value = second_bookmark_key_value.get(replication_key)
                    second_bookmark_value = second_bookmark_key_value
                    first_bookmark_value_utc = self.convert_state_to_utc(first_bookmark_value)
                    second_bookmark_value_utc = self.convert_state_to_utc(second_bookmark_value)
                    #simulated_bookmark_value = new_states['bookmarks'][stream][replication_key]
                    simulated_bookmark_value = new_states[stream]
                    # BHT removed lookback logic specific to facebooks insights
                    simulated_bookmark_minus_lookback =  simulated_bookmark_value

                    # Verify the first sync sets a bookmark of the expected form
                    self.assertIsNotNone(first_bookmark_key_value)
                    #self.assertIsNotNone(first_bookmark_key_value.get(replication_key))

                    # Verify the second sync sets a bookmark of the expected form
                    self.assertIsNotNone(second_bookmark_key_value)
                    #self.assertIsNotNone(second_bookmark_key_value.get(replication_key))

                    # Verify the second sync bookmark is Equal to the first sync bookmark
                    self.assertEqual(second_bookmark_value, first_bookmark_value) # assumes no changes to data during test

                    # Verify the number of records in the 2nd sync is less then the first
                    self.assertLess(second_sync_count, first_sync_count)


                # No such tables for freshdesk as of Jan 31 2022
                elif expected_replication_method == self.FULL_TABLE:

                    # Verify the syncs do not set a bookmark for full table streams
                    self.assertIsNone(first_bookmark_key_value)
                    self.assertIsNone(second_bookmark_key_value)

                    # Verify the number of records in the second sync is the same as the first
                    self.assertEqual(second_sync_count, first_sync_count)

                else:

                    raise NotImplementedError(
                        "INVALID EXPECTATIONS\t\tSTREAM: {} REPLICATION_METHOD: {}".format(stream, expected_replication_method)
                    )


                # Verify at least 1 record was replicated in the second sync
                self.assertGreater(second_sync_count, 0, msg="We are not fully testing bookmarking for {}".format(stream))
