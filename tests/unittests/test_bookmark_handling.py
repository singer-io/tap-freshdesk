import unittest
from parameterized import parameterized
from tap_freshdesk.streams import get_min_bookmark, get_schema, write_bookmark


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
        ["with_child_selected", "tickets", ["tickets", "satisfaction_ratings"], "updated_at", "2022-03-14T00:00:00Z"],
        ["only_children_selected", "tickets", ["satisfaction_ratings","conversations", "time_entries"], "updated_at", "2022-03-01T00:00:00Z"],
        ["only_parent_selected", "tickets", ["tickets"], "updated_at", "2022-03-29T00:00:00Z"],
    ])
    def test_min_bookmark(self, name, stream_name, stream_to_sync, bookmark_key, expected_bookmark):
        """
        Test that `get_min_bookmark` method returns the minimum bookmark from the parent and its corresponding child bookmarks. 
        """
        min_bookmark = get_min_bookmark(stream_name, stream_to_sync, self.start_date, self.state, bookmark_key)

        # Verify returned bookmark is expected
        self.assertEqual(min_bookmark, expected_bookmark)


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

        # Verify returned schema is same as exected schema 
        self.assertEqual(get_schema(catalog, "agents"), expected_schema)


class TestWriteBookmark(unittest.TestCase):
    """
    Test the `write_bookmark` method of the stream class
    """

    @parameterized.expand([
        ["stream_not_selected", "agents", False, {"bookmarks": {}}],
        ["stream_not_selected", "groups", True, {"bookmarks": {"groups": {"updated_at": "BOOKMARK_VALUE"}}}],
    ])
    def test_write_bookmark(self, name, stream, is_called, expected_state):
        """
        Test that bookmark is written only if the stream is selected
        """
        state = {"bookmarks": {}}
        write_bookmark(stream, ["roles", "groups"], "BOOKMARK_VALUE", state)
        
        # Verify that the final state is as expected
        self.assertEqual(state, expected_state)
