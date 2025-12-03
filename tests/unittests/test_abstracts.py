import unittest
from unittest.mock import patch, MagicMock
from tap_freshdesk.streams.abstracts import ParentBaseStream, ChildBaseStream, IncrementalStream


class ConcreteParentBaseStream(ParentBaseStream):
    """Concrete subclass for testing ParentBaseStream"""
    @property
    def key_properties(self):
        return ["id"]

    @property
    def replication_keys(self):
        return ["updated_at"]

    @property
    def replication_method(self):
        return "INCREMENTAL"

    @property
    def tap_stream_id(self):
        return "tickets"
    
    @property
    def forced_replication_method(self):
        return "INCREMENTAL"


class ConcreteChildBaseStream(ChildBaseStream):
    """Concrete subclass for testing ChildBaseStream"""
    @property
    def key_properties(self):
        return ["id"]

    @property
    def replication_keys(self):
        return ["updated_at"]

    @property
    def replication_method(self):
        return "INCREMENTAL"

    @property
    def tap_stream_id(self):
        return "conversations"
    
    @property
    def forced_replication_method(self):
        return "INCREMENTAL"
    
    def __init__(self, client=None, catalog=None):
        super().__init__(client, catalog)
        self.parent = "tickets"
        self.path = "tickets/{}/conversations"


class ConcreteIncrementalStream(IncrementalStream):
    """Concrete subclass for testing IncrementalStream"""
    @property
    def key_properties(self):
        return ["id"]

    @property
    def replication_keys(self):
        return ["updated_at"]

    @property
    def replication_method(self):
        return "INCREMENTAL"

    @property
    def tap_stream_id(self):
        return "test_stream"
    
    @property
    def forced_replication_method(self):
        return "INCREMENTAL"


class TestParentBaseStream(unittest.TestCase):
    """Test cases for ParentBaseStream.get_bookmark"""
    
    @patch("tap_freshdesk.streams.abstracts.metadata.to_map")
    def setUp(self, mock_to_map):
        mock_catalog = MagicMock()
        mock_catalog.schema.to_dict.return_value = {"key": "value"}
        mock_catalog.metadata = "mock_metadata"
        mock_to_map.return_value = {"metadata_key": "metadata_value"}

        self.stream = ConcreteParentBaseStream(catalog=mock_catalog)
        self.stream.child_to_sync = []

    @patch("tap_freshdesk.streams.abstracts.BaseStream.is_selected", return_value=True)
    @patch("tap_freshdesk.streams.abstracts.IncrementalStream.get_bookmark", return_value=100)
    def test_get_bookmark_parent_only_with_selected(self, mock_get_bookmark, _mock_is_selected):
        state = {}
        result = self.stream.get_bookmark(state, "parent_stream")
        mock_get_bookmark.assert_called_once_with(state, "parent_stream")
        self.assertEqual(result, 100)

    @patch("tap_freshdesk.streams.abstracts.BaseStream.is_selected", return_value=False)
    @patch("tap_freshdesk.streams.abstracts.IncrementalStream.get_bookmark", return_value=100)
    def test_get_bookmark_parent_only_but_not_selected(self, _mock_get_bookmark, _mock_is_selected):
        state = {}
        result = self.stream.get_bookmark(state, "parent_stream")
        self.assertEqual(result, None)

    @patch("tap_freshdesk.streams.abstracts.BaseStream.is_selected", return_value=True)
    @patch("tap_freshdesk.streams.abstracts.IncrementalStream.get_bookmark", side_effect=[100, 50, 75])
    def test_get_bookmark_with_children(self, mock_get_bookmark, _mock_is_selected):
        child1 = MagicMock()
        child1.tap_stream_id = "child_stream_1"
        child2 = MagicMock()
        child2.tap_stream_id = "child_stream_2"
        self.stream.child_to_sync = [child1, child2]

        state = {}
        result = self.stream.get_bookmark(state, "parent_stream")

        self.assertEqual(mock_get_bookmark.call_count, 3)
        self.assertEqual(result, 50)

    @patch("tap_freshdesk.streams.abstracts.BaseStream.is_selected", return_value=True)
    @patch("tap_freshdesk.streams.abstracts.IncrementalStream.get_bookmark", return_value=None)
    def test_get_bookmark_parent_returns_none(self, mock_get_bookmark, _mock_is_selected):
        state = {}
        result = self.stream.get_bookmark(state, "parent_stream")
        mock_get_bookmark.assert_called_once_with(state, "parent_stream")
        self.assertEqual(result, None)

    @patch("tap_freshdesk.streams.abstracts.BaseStream.is_selected", return_value=True)
    @patch("tap_freshdesk.streams.abstracts.IncrementalStream.get_bookmark", side_effect=[100, None, 75])
    def test_get_bookmark_with_none_child_bookmarks(self, mock_get_bookmark, _mock_is_selected):
        """Test that None child bookmarks cause TypeError in current implementation"""
        child1 = MagicMock()
        child1.tap_stream_id = "child_stream_1"
        child2 = MagicMock()
        child2.tap_stream_id = "child_stream_2"
        self.stream.child_to_sync = [child1, child2]

        state = {}
        # Current implementation has a bug with None handling in min() comparison
        with self.assertRaises(TypeError):
            self.stream.get_bookmark(state, "parent_stream")

    @patch("tap_freshdesk.streams.abstracts.BaseStream.is_selected", return_value=True)
    @patch("tap_freshdesk.streams.abstracts.IncrementalStream.get_bookmark", side_effect=[100, 50, 75])
    def test_get_bookmark_with_valid_child_bookmarks_only(self, mock_get_bookmark, _mock_is_selected):
        """Test with all valid (non-None) bookmarks"""
        child1 = MagicMock()
        child1.tap_stream_id = "child_stream_1"
        child2 = MagicMock()
        child2.tap_stream_id = "child_stream_2"
        self.stream.child_to_sync = [child1, child2]

        state = {}
        result = self.stream.get_bookmark(state, "parent_stream")

        self.assertEqual(mock_get_bookmark.call_count, 3)
        self.assertEqual(result, 50)

    @patch("tap_freshdesk.streams.abstracts.BaseStream.is_selected", return_value=True)
    @patch("tap_freshdesk.streams.abstracts.IncrementalStream.get_bookmark", side_effect=[100, 100, 100])
    def test_get_bookmark_all_same_values(self, mock_get_bookmark, _mock_is_selected):
        child1 = MagicMock()
        child1.tap_stream_id = "child_stream_1"
        child2 = MagicMock()
        child2.tap_stream_id = "child_stream_2"
        self.stream.child_to_sync = [child1, child2]

        state = {}
        result = self.stream.get_bookmark(state, "parent_stream")

        self.assertEqual(mock_get_bookmark.call_count, 3)
        self.assertEqual(result, 100)


class TestParentBaseStreamWriteBookmark(unittest.TestCase):
    """Test cases for ParentBaseStream.write_bookmark"""
    
    @patch("tap_freshdesk.streams.abstracts.metadata.to_map")
    def setUp(self, mock_to_map):
        mock_catalog = MagicMock()
        mock_catalog.schema.to_dict.return_value = {"key": "value"}
        mock_catalog.metadata = "mock_metadata"
        mock_to_map.return_value = {"metadata_key": "metadata_value"}

        self.stream = ConcreteParentBaseStream(catalog=mock_catalog)
        self.stream.child_to_sync = []

    @patch("tap_freshdesk.streams.abstracts.BaseStream.is_selected", return_value=True)
    @patch("tap_freshdesk.streams.abstracts.IncrementalStream.write_bookmark")
    def test_write_bookmark_parent_selected(self, mock_write_bookmark, _mock_is_selected):
        state = {"bookmarks": {}}
        result = self.stream.write_bookmark(state, "tickets", value="2024-01-01")
        mock_write_bookmark.assert_called_once_with(state, "tickets", value="2024-01-01")
        self.assertEqual(result, state)

    @patch("tap_freshdesk.streams.abstracts.BaseStream.is_selected", return_value=True)
    @patch("tap_freshdesk.streams.abstracts.IncrementalStream.write_bookmark")
    def test_write_bookmark_with_child_streams(self, mock_write_bookmark, _mock_is_selected):
        child = MagicMock()
        child.tap_stream_id = "conversations"
        child.write_child_bookmark_with_parent = MagicMock(return_value={"bookmarks": {}})
        child.get_bookmark = MagicMock(return_value="2024-01-01")
        self.stream.child_to_sync = [child]
        
        state = {"bookmarks": {}}
        self.stream.write_bookmark(state, "tickets", value="2024-02-01")
        
        mock_write_bookmark.assert_called_once()
        child.write_child_bookmark_with_parent.assert_called_once_with(
            state, "", "2024-01-01", "2024-02-01"
        )

    @patch("tap_freshdesk.streams.abstracts.BaseStream.is_selected", return_value=False)
    @patch("tap_freshdesk.streams.abstracts.IncrementalStream.write_bookmark")
    def test_write_bookmark_parent_not_selected(self, mock_write_bookmark, _mock_is_selected):
        state = {"bookmarks": {}}
        result = self.stream.write_bookmark(state, "tickets", value="2024-01-01")
        mock_write_bookmark.assert_not_called()
        self.assertEqual(result, state)

    @patch("tap_freshdesk.streams.abstracts.BaseStream.is_selected", return_value=True)
    @patch("tap_freshdesk.streams.abstracts.IncrementalStream.write_bookmark")
    def test_write_bookmark_with_multiple_children(self, mock_write_bookmark, _mock_is_selected):
        child1 = MagicMock()
        child1.tap_stream_id = "conversations"
        child1.write_child_bookmark_with_parent = MagicMock(return_value={"bookmarks": {}})
        child1.get_bookmark = MagicMock(return_value="2024-01-15")
        
        child2 = MagicMock()
        child2.tap_stream_id = "notes"
        child2.write_child_bookmark_with_parent = MagicMock(return_value={"bookmarks": {}})
        child2.get_bookmark = MagicMock(return_value="2024-01-20")
        
        self.stream.child_to_sync = [child1, child2]
        
        state = {"bookmarks": {}}
        self.stream.write_bookmark(state, "tickets", value="2024-02-01")
        
        mock_write_bookmark.assert_called_once()
        child1.write_child_bookmark_with_parent.assert_called_once()
        child2.write_child_bookmark_with_parent.assert_called_once()

    @patch("tap_freshdesk.streams.abstracts.BaseStream.is_selected", return_value=True)
    @patch("tap_freshdesk.streams.abstracts.IncrementalStream.write_bookmark")
    def test_write_bookmark_child_returns_none_bookmark(self, mock_write_bookmark, _mock_is_selected):
        child = MagicMock()
        child.tap_stream_id = "conversations"
        child.write_child_bookmark_with_parent = MagicMock(return_value={"bookmarks": {}})
        child.get_bookmark = MagicMock(return_value=None)
        self.stream.child_to_sync = [child]
        
        state = {"bookmarks": {}}
        self.stream.write_bookmark(state, "tickets", value="2024-02-01")
        
        mock_write_bookmark.assert_called_once()
        child.write_child_bookmark_with_parent.assert_called_once_with(
            state, "", None, "2024-02-01"
        )


class TestChildBaseStream(unittest.TestCase):
    """Test cases for ChildBaseStream"""
    
    @patch("tap_freshdesk.streams.abstracts.metadata.to_map")
    def setUp(self, mock_to_map):
        mock_catalog = MagicMock()
        mock_catalog.schema.to_dict.return_value = {"key": "value"}
        mock_catalog.metadata = "mock_metadata"
        mock_to_map.return_value = {"metadata_key": "metadata_value"}

        mock_client = MagicMock()
        mock_client.base_url = "https://domain.freshdesk.com/api/v2"
        
        self.stream = ConcreteChildBaseStream(catalog=mock_catalog, client=mock_client)

    def test_get_url_endpoint(self):
        parent_obj = {"id": 123}
        result = self.stream.get_url_endpoint(parent_obj)
        expected = "https://domain.freshdesk.com/api/v2/tickets/123/conversations"
        self.assertEqual(result, expected)

    def test_get_url_endpoint_with_string_id(self):
        parent_obj = {"id": "abc123"}
        result = self.stream.get_url_endpoint(parent_obj)
        expected = "https://domain.freshdesk.com/api/v2/tickets/abc123/conversations"
        self.assertEqual(result, expected)

    def test_get_parent_bookmark_for_category_from_child_state(self):
        state = {
            "bookmarks": {
                "conversations_spam": {
                    "updated_at": "2024-01-15",
                    "tickets_spam_updated_at": "2024-01-10"
                }
            }
        }
        
        result = self.stream.get_parent_bookmark_for_category(state, "_spam")
        self.assertEqual(result, "2024-01-10")

    def test_get_parent_bookmark_for_category_missing_state(self):
        state = {"bookmarks": {}}
        result = self.stream.get_parent_bookmark_for_category(state, "_spam")
        self.assertEqual(result, None)

    def test_get_parent_bookmark_for_category_missing_parent_key(self):
        state = {
            "bookmarks": {
                "conversations_spam": {
                    "updated_at": "2024-01-15"
                }
            }
        }
        result = self.stream.get_parent_bookmark_for_category(state, "_spam")
        self.assertEqual(result, None)

    def test_get_parent_bookmark_for_category_empty_category(self):
        state = {
            "bookmarks": {
                "conversations": {
                    "updated_at": "2024-01-15",
                    "tickets_updated_at": "2024-01-10"
                }
            }
        }
        result = self.stream.get_parent_bookmark_for_category(state, "")
        self.assertEqual(result, "2024-01-10")

    def test_write_child_bookmark_with_parent(self):
        state = {"bookmarks": {}}
        
        result = self.stream.write_child_bookmark_with_parent(
            state, "_spam", "2024-01-15", "2024-01-10"
        )
        
        expected = {
            "bookmarks": {
                "conversations_spam": {
                    "updated_at": "2024-01-15",
                    "tickets_spam_updated_at": "2024-01-10"
                }
            }
        }
        self.assertEqual(result, expected)

    def test_write_child_bookmark_with_parent_empty_category(self):
        state = {"bookmarks": {}}
        
        result = self.stream.write_child_bookmark_with_parent(
            state, "", "2024-01-15", "2024-01-10"
        )
        
        expected = {
            "bookmarks": {
                "conversations": {
                    "updated_at": "2024-01-15",
                    "tickets_updated_at": "2024-01-10"
                }
            }
        }
        self.assertEqual(result, expected)

    def test_write_child_bookmark_overwrites_existing(self):
        state = {
            "bookmarks": {
                "conversations_spam": {
                    "updated_at": "2024-01-01",
                    "tickets_spam_updated_at": "2024-01-01"
                }
            }
        }
        
        result = self.stream.write_child_bookmark_with_parent(
            state, "_spam", "2024-01-15", "2024-01-10"
        )
        
        expected = {
            "bookmarks": {
                "conversations_spam": {
                    "updated_at": "2024-01-15",
                    "tickets_spam_updated_at": "2024-01-10"
                }
            }
        }
        self.assertEqual(result, expected)


class TestIncrementalStream(unittest.TestCase):
    """Test cases for IncrementalStream"""
    
    @patch("tap_freshdesk.streams.abstracts.metadata.to_map")
    def setUp(self, mock_to_map):
        mock_catalog = MagicMock()
        mock_catalog.schema.to_dict.return_value = {"key": "value"}
        mock_catalog.metadata = "mock_metadata"
        mock_to_map.return_value = {"metadata_key": "metadata_value"}

        mock_client = MagicMock()
        mock_client.config = {"start_date": "2023-01-01"}
        
        self.stream = ConcreteIncrementalStream(catalog=mock_catalog, client=mock_client)

    @patch("tap_freshdesk.streams.abstracts.get_bookmark")
    def test_get_bookmark(self, mock_get_bookmark):
        mock_get_bookmark.return_value = "2024-01-01"
        state = {"bookmarks": {}}
        
        result = self.stream.get_bookmark(state, "test_stream")
        
        mock_get_bookmark.assert_called_once_with(
            state, "test_stream", "updated_at", "2023-01-01"
        )
        self.assertEqual(result, "2024-01-01")

    @patch("tap_freshdesk.streams.abstracts.write_bookmark")
    @patch("tap_freshdesk.streams.abstracts.get_bookmark")
    def test_write_bookmark_takes_max_value(self, mock_get_bookmark, mock_write_bookmark):
        mock_get_bookmark.return_value = "2024-02-01"
        mock_write_bookmark.return_value = {"bookmarks": {}}
        
        state = {"bookmarks": {}}
        self.stream.write_bookmark(state, "test_stream", value="2024-01-01")
        
        # Should write the max value (2024-02-01)
        mock_write_bookmark.assert_called_once_with(state, "test_stream", "updated_at", "2024-02-01")

    @patch("tap_freshdesk.streams.abstracts.write_bookmark")
    @patch("tap_freshdesk.streams.abstracts.get_bookmark")
    def test_write_bookmark_equal_values(self, mock_get_bookmark, mock_write_bookmark):
        mock_get_bookmark.return_value = "2024-01-01"
        mock_write_bookmark.return_value = {"bookmarks": {}}
        
        state = {"bookmarks": {}}
        self.stream.write_bookmark(state, "test_stream", value="2024-01-01")
        
        mock_write_bookmark.assert_called_once_with(state, "test_stream", "updated_at", "2024-01-01")

    @patch("tap_freshdesk.streams.abstracts.write_bookmark")
    @patch("tap_freshdesk.streams.abstracts.get_bookmark")
    def test_write_bookmark_with_none_existing(self, mock_get_bookmark, mock_write_bookmark):
        """Test that None existing bookmark causes TypeError in max() comparison"""
        mock_get_bookmark.return_value = None
        mock_write_bookmark.return_value = {"bookmarks": {}}
        
        state = {"bookmarks": {}}
        # Current implementation has a bug with None handling in max() comparison
        with self.assertRaises(TypeError):
            self.stream.write_bookmark(state, "test_stream", value="2024-01-01")

    @patch("tap_freshdesk.streams.abstracts.write_bookmark")
    @patch("tap_freshdesk.streams.abstracts.get_bookmark")
    def test_write_bookmark_with_none_new_value(self, mock_get_bookmark, mock_write_bookmark):
        """Test that None new value causes TypeError in max() comparison"""
        mock_get_bookmark.return_value = "2024-01-01"
        mock_write_bookmark.return_value = {"bookmarks": {}}
        
        state = {"bookmarks": {}}
        # Current implementation has a bug with None handling in max() comparison
        with self.assertRaises(TypeError):
            self.stream.write_bookmark(state, "test_stream", value=None)

    @patch("tap_freshdesk.streams.abstracts.write_bookmark")
    @patch("tap_freshdesk.streams.abstracts.get_bookmark")
    def test_write_bookmark_with_valid_values(self, mock_get_bookmark, mock_write_bookmark):
        """Test write_bookmark with both values being valid (non-None)"""
        mock_get_bookmark.return_value = "2024-01-01"
        mock_write_bookmark.return_value = {"bookmarks": {}}
        
        state = {"bookmarks": {}}
        self.stream.write_bookmark(state, "test_stream", value="2024-03-01")
        
        # Should write the max value
        mock_write_bookmark.assert_called_once_with(state, "test_stream", "updated_at", "2024-03-01")

    @patch("tap_freshdesk.streams.abstracts.get_bookmark")
    def test_get_bookmark_returns_start_date(self, mock_get_bookmark):
        """Test that get_bookmark returns the start_date when no bookmark exists"""
        mock_get_bookmark.return_value = "2023-01-01"
        state = {"bookmarks": {}}
        
        result = self.stream.get_bookmark(state, "test_stream")
        
        self.assertEqual(result, "2023-01-01")
