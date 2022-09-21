import unittest
from tap_freshdesk.sync import update_currently_syncing, get_ordered_stream_list


class TestGetOrderedStreamList(unittest.TestCase):
    """
    Test `get_ordered_stream_list` function to get ordered list od streams
    """

    streams_to_sync = ["agents", "companies",  "tickets",
                       "conversations", "groups", "satisfaction_ratings", "time_entries"]

    def test_currently_syncing_not_in_list(self):
        """Test if currently syncing is not available in `streams_to_sync` list, function returns sorted streams_to_sync list."""
        expected_list = ["agents", "companies", "conversations",
                         "groups", "satisfaction_ratings", "tickets", "time_entries"]
        final_list = get_ordered_stream_list("roles", self.streams_to_sync)

        # Verify with expected ordered list of streams
        self.assertEqual(final_list, expected_list)

    def test_for_interrupted_sync(self):
        """Test when the sync was interrupted, the function returns ordered list of streams starting with 'currently_syncing' stream."""
        expected_list = ["groups", "satisfaction_ratings", "tickets",
                         "time_entries", "agents", "companies", "conversations"]
        final_list = get_ordered_stream_list("groups", self.streams_to_sync)

        # Verify with expected ordered list of streams
        self.assertEqual(final_list, expected_list)

    def test_for_completed_sync(self):
        """Test when sync was not interrupted, the function returns sorted streams_to_sync list."""
        expected_list = ["agents", "companies", "conversations",
                         "groups", "satisfaction_ratings", "tickets", "time_entries"]
        final_list = get_ordered_stream_list(None, self.streams_to_sync)

        # Verify with expected ordered list of streams
        self.assertEqual(final_list, expected_list)


class TestUpdateCurrentlySyncing(unittest.TestCase):

    """
    Test `update_currently_syncing` function of sync.
    """

    def test_update_syncing_stream(self):
        """Test for adding currently syncing stream in state."""
        state = {"currently_syncing": "groups"}
        update_currently_syncing(state, "groups")

        # Verify with expected state
        self.assertEqual(state, {"currently_syncing": "groups"})

    def test_flush_currently_syncing(self):
        """Test for removing currently syncing stream from state."""
        state = {"currently_syncing": "agents"}
        update_currently_syncing(state, None)

        # Verify with expected state
        self.assertEqual(state, {})
