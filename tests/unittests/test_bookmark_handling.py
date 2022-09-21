import unittest
from parameterized import parameterized
from tap_freshdesk.streams import get_min_bookmark, get_schema, write_bookmark

START_DATE = '2022-09-00T00:00:00.000000Z'


class TestGetMinBookmark(unittest.TestCase):
    """
    Test `get_min_bookmark` method of the stream class
    """

    start_date = "2020-04-01T00:00:00Z"
    state = {
        "bookmarks": {
            "tickets": {"updated_at": "2022-03-29T00:00:00Z"},
            "conversations": {"updated_at": "2022-03-01T00:00:00Z"},
            "satisfaction_ratings": {"updated_at": "2022-03-14T00:00:00Z"},
            "time_entries": {"updated_at": "2022-04-01T00:00:00Z"},
        }
    }

    @parameterized.expand([
        # ["test_name", "selected_streams", "state", "expected_bookmark"]
        ['test_parent_only_with_state', ['tickets'], {'bookmarks': {'tickets': {'updated_at': '2022-08-30T00:00:00.000000Z'}}}, '2022-08-30T00:00:00.000000Z'],
        ['test_child_only_with_state', ['conversations'], {'bookmarks': {'conversations': {'updated_at': '2022-08-30T00:00:00.000000Z'}}}, '2022-08-30T00:00:00.000000Z'],
        ['test_parent_only_without_state', ['tickets'], {}, START_DATE],
        ['test_child_only_without_state', ['tickets'], {}, START_DATE],
        ['test_min_parent_bookmark_single_child', ['tickets', 'conversations'],
         {'bookmarks': {'tickets': {'updated_at': '2022-07-30T00:00:00.000000Z'}, 'conversations': {'updated_at': '2022-08-30T00:00:00.000000Z'}}}, '2022-07-30T00:00:00.000000Z'],
        ['test_min_child_bookmark_single_child', ['tickets', 'conversations'],
         {'bookmarks': {'tickets': {'updated_at': '2022-08-30T00:00:00.000000Z'}, 'conversations': {'updated_at': '2022-07-30T00:00:00.000000Z'}}}, '2022-07-30T00:00:00.000000Z'],
        ['test_min_child_bookmark_multiple_child', ['tickets', 'conversations', 'time_entries'],
         {'bookmarks': {'tickets': {'updated_at': '2022-09-30T00:00:00.000000Z'}, 'conversations': {'updated_at': '2022-09-30T00:00:00.000000Z'}}}, START_DATE],
        ['test_multiple_child_only_bookmark', ['tickets', 'conversations', 'time_entries'],
         {'bookmarks': {'time_entries': {'updated_at': '2022-09-30T00:00:00.000000Z'}, 'conversations': {'updated_at': '2022-09-30T00:00:00.000000Z'}}}, START_DATE],
        ['test_multiple_child_bookmark', ['tickets', 'conversations', 'time_entries'],
         {'bookmarks': {'time_entries': {'updated_at': '2022-06-30T00:00:00.000000Z'}, 'tickets': {'updated_at': '2022-08-30T00:00:00.000000Z'}, 'conversations': {'updated_at': '2022-11-30T00:00:00.000000Z'}}}, '2022-06-30T00:00:00.000000Z']

    ])
    def test_min_bookmark(self, test_name, selected_streams, state, expected_bookmark):
        """
        Test that `get_min_bookmark` method returns the minimum bookmark from the parent and its corresponding child bookmarks. 
        """
        current_time = '2022-09-30T00:00:00.000000Z'
        actual_bookmark = get_min_bookmark('tickets', selected_streams, current_time, START_DATE, state, 'updated_at')
        self.assertEqual(actual_bookmark, expected_bookmark)


class TestGetSchema(unittest.TestCase):
    """
    Test `get_schema` method of the stream class.
    """

    def test_get_schema(self):
        """Verify function returns expected schema"""
        catalog = [
            {"tap_stream_id": "roles"},
            {"tap_stream_id": "agents"},
            {"tap_stream_id": "time_entries"},
        ]
        expected_schema = {"tap_stream_id": "agents"}

        # Verify returned schema is same as expected schema
        self.assertEqual(get_schema(catalog, "agents"), expected_schema)


class TestWriteBookmark(unittest.TestCase):
    """
    Test the `write_bookmark` method of the stream class
    """

    @parameterized.expand([
        # ["test_name", "stream", "expected_state"]
        ["stream_not_selected", "agents", {"bookmarks": {}}],
        ["stream_not_selected", "groups", {"bookmarks": {"groups": {"updated_at": "BOOKMARK_VALUE"}}}],
    ])
    def test_write_bookmark(self, test_name, stream, expected_state):
        """
        Test that bookmark is written only if the stream is selected
        """
        state = {"bookmarks": {}}
        write_bookmark(stream, ["roles", "groups"], "BOOKMARK_VALUE", state)

        # Verify that the final state is as expected
        self.assertEqual(state, expected_state)
