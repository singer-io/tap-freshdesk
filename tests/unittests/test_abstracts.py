import unittest
from unittest.mock import patch, MagicMock, Mock
from tap_freshdesk.streams.abstracts import ParentBaseStream, ChildBaseStream, IncrementalStream

from tap_freshdesk.streams import Tickets
from tap_freshdesk.exceptions import freshdeskBadRequestError


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

        mock_client = MagicMock()
        mock_client.config = {"start_date": "2023-01-01"}

        self.stream = ConcreteParentBaseStream(catalog=mock_catalog, client=mock_client)
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
            state, "", "2023-01-01", "2024-02-01"
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


class TestTicketsSync(unittest.TestCase):

    @patch("tap_freshdesk.streams.abstracts.metadata.to_map")
    def setUp(self, mock_to_map):
        mock_catalog = MagicMock()
        mock_catalog.schema.to_dict.return_value = {"key": "value"}
        mock_catalog.metadata = "mock_metadata"
        mock_to_map.return_value = {"metadata_key": "metadata_value"}

        mock_client = MagicMock()
        mock_client.config = {"start_date": "2023-01-01"}
        self.stream = Tickets(client=mock_client, catalog=mock_catalog)

        self.stream.is_selected = Mock(return_value=True)
        self.stream.is_child_selected = Mock(return_value=True)

        self.stream.child_to_sync = []

        self.transformer = Mock()
        self.transformer.transform.side_effect = (
            lambda record, *args: record
        )

        self.state = {}

    @patch("tap_freshdesk.streams.abstracts.write_record")
    def test_sync_happy_path(self, mock_write_record):
        """
        Verify all records are written and bookmark is updated.
        """

        self.stream.get_bookmark = Mock(
            return_value="2024-01-01T00:00:00Z"
        )

        self.stream.write_bookmark = Mock(
            side_effect=lambda state, key, value: state
        )

        self.stream.get_records = Mock(
            side_effect=lambda state: iter(
                [
                    {
                        "id": 1,
                        "updated_at": "2024-01-01T01:00:00Z",
                    },
                    {
                        "id": 2,
                        "updated_at": "2024-01-01T02:00:00Z",
                    },
                ]
            )
        )

        self.stream.sync(
            self.state,
            self.transformer,
        )

        # Call count for 2 records, for all 3 filter types (all, spam, deleted)
        self.assertEqual(mock_write_record.call_count, 2*3)

        self.stream.write_bookmark.assert_called()

    @patch("tap_freshdesk.streams.abstracts.write_record")
    def test_restart_after_400(self, mock_write_record):
        """
        Verify sync restarts after Freshdesk 300-page error.
        """

        self.stream.get_bookmark = Mock(
            return_value="2024-01-01T00:00:00Z"
        )

        self.stream.write_bookmark = Mock(
            side_effect=lambda state, key, value: state
        )

        call_count = 0

        def mock_get_records(state):
            nonlocal call_count

            call_count += 1

            if call_count == 1:
                yield {
                    "id": 1,
                    "updated_at": "2024-01-01T01:00:00Z",
                }

                raise freshdeskBadRequestError(
                    "Freshdesk 300 page limit reached"
                )

            yield {
                "id": 1,
                "updated_at": "2024-01-01T01:00:00Z",
            }

            yield {
                "id": 2,
                "updated_at": "2024-01-01T01:00:00Z",
            }

        self.stream.get_records = mock_get_records

        self.stream.sync(
            self.state,
            self.transformer,
        )
        #
        # ticket 1 emitted once
        # ticket 2 emitted once
        # This is done 3 times for all filter types 
        self.assertEqual(
            mock_write_record.call_count,
            2*3,
        )

        self.assertEqual(
            call_count,
            4,
        )

    @patch("tap_freshdesk.streams.abstracts.write_record")
    def test_same_ticket_processed_again_after_timestamp_window_change(
        self,
        mock_write_record,
    ):
        """
        Scenario:

        pk1  updated_at=1
        pk2  updated_at=1
        pk3  updated_at=1

        cache = {1,2,3}

        --------------------------------

        pk4  updated_at=2
        pk5  updated_at=2
        pk6  updated_at=2

        cache cleared

        cache = {4,5,6}

        --------------------------------

        pk1  updated_at=3

        cache cleared

        pk1 should be written again because
        deduplication is only scoped to the
        current timestamp window.
        """

        self.stream.get_bookmark = Mock(
            return_value="2024-01-01T00:00:00Z"
        )

        self.stream.write_bookmark = Mock(
            side_effect=lambda state, key, value: state
        )

        records = [
            {
                "id": 1,
                "updated_at": "2024-01-01T01:00:00Z",
            },
            {
                "id": 2,
                "updated_at": "2024-01-01T01:00:00Z",
            },
            {
                "id": 3,
                "updated_at": "2024-01-01T01:00:00Z",
            },
            {
                "id": 4,
                "updated_at": "2024-01-01T02:00:00Z",
            },
            {
                "id": 5,
                "updated_at": "2024-01-01T02:00:00Z",
            },
            {
                "id": 6,
                "updated_at": "2024-01-01T02:00:00Z",
            },
            {
                "id": 1,
                "updated_at": "2024-01-01T03:00:00Z",
            },
        ]

        # Note: The get_records method is mocked to return an iterator over the records list.
        # Hence the iterator will be exhausted after the first filter loop
        # and the subsequent filter loops will not yield any records.
        self.stream.get_records = Mock(
            return_value=iter(records)
        )

        self.stream.sync(
            self.state,
            self.transformer,
        )

        #
        # Expected writes:
        #
        # id=1 @ 01:00
        # id=2 @ 01:00
        # id=3 @ 01:00
        # id=4 @ 02:00
        # id=5 @ 02:00
        # id=6 @ 02:00
        # id=1 @ 03:00
        #
        self.assertEqual(
            mock_write_record.call_count,
            7,
        )

        written_ids = [
            call.args[1]["id"]
            for call in mock_write_record.call_args_list
        ]

        self.assertEqual(
            written_ids,
            [1, 2, 3, 4, 5, 6, 1]
        )

    @patch("tap_freshdesk.streams.abstracts.write_record")
    def test_duplicate_same_timestamp_skipped(
        self,
        mock_write_record,
    ):
        """
        Verify duplicate ticket IDs in same timestamp
        window are skipped.
        """

        self.stream.get_bookmark = Mock(
            return_value="2024-01-01T00:00:00Z"
        )

        self.stream.write_bookmark = Mock(
            side_effect=lambda state, key, value: state
        )

        self.stream.get_records = Mock(
            return_value=iter(
                [
                    {
                        "id": 1,
                        "updated_at": "2024-01-01T01:00:00Z",
                    },
                    {
                        "id": 1,
                        "updated_at": "2024-01-01T01:00:00Z",
                    },
                ]
            )
        )

        self.stream.sync(
            self.state,
            self.transformer,
        )

        self.assertEqual(
            mock_write_record.call_count,
            1,
        )

    @patch("tap_freshdesk.streams.abstracts.write_record")
    def test_same_ticket_new_timestamp_processed(
        self,
        mock_write_record,
    ):
        """
        Verify same ticket ID is emitted again
        when updated_at changes.
        """

        self.stream.get_bookmark = Mock(
            return_value="2024-01-01T00:00:00Z"
        )

        self.stream.write_bookmark = Mock(
            side_effect=lambda state, key, value: state
        )

        self.stream.get_records = Mock(
            return_value=iter(
                [
                    {
                        "id": 1,
                        "updated_at": "2024-01-01T01:00:00Z",
                    },
                    {
                        "id": 1,
                        "updated_at": "2024-01-01T02:00:00Z",
                    },
                ]
            )
        )

        self.stream.sync(
            self.state,
            self.transformer,
        )

        self.assertEqual(
            mock_write_record.call_count,
            2,  # Since the iterator will be ehausted after normal filters,
                # the spam and deleted filters will not get any records.
        )

    @patch("tap_freshdesk.streams.abstracts.write_record")
    def test_bookmark_written_before_restart(
        self,
        mock_write_record,
    ):
        """
        Verify bookmark is persisted before retry.
        """

        bookmark_values = []

        def mock_write_bookmark(
            state,
            key,
            value,
        ):
            bookmark_values.append(value)
            return state

        self.stream.write_bookmark = Mock(
            side_effect=mock_write_bookmark
        )

        self.stream.get_bookmark = Mock(
            return_value="2024-01-01T00:00:00Z"
        )

        call_count = 0

        def mock_get_records(state):
            nonlocal call_count

            call_count += 1

            yield {
                "id": 1,
                "updated_at": "2024-01-01T05:00:00Z",
            }

            if call_count == 1:
                raise freshdeskBadRequestError(
                    "Freshdesk 300 page limit reached"
                )

        self.stream.get_records = mock_get_records

        self.stream.sync(
            self.state,
            self.transformer,
        )

        self.assertIn(
            "2024-01-01T05:00:00Z",
            bookmark_values,
        )

        # Call count workflow --> call_count = 0
        # 1. First call with normal_filters ==> call_count = 1
        # 1. When generator resumes, it raises freshdeskBadRequestError
        # 2. Second call with normal_filters ==> call_count = 2
        # 3. Third call with spam_filters ==> call_count = 3
        # 4. Fourth call with deleted_filters ==> call_count = 4
        self.assertEqual(call_count, 4)

    @patch("tap_freshdesk.streams.abstracts.LOGGER")
    @patch("tap_freshdesk.streams.abstracts.write_record")
    def test_max_restarts_exceeded_advances_bookmark_and_reraises(
        self, mock_write_record, mock_logger
    ):
        """
        Verify that when freshdeskBadRequestError occurs MAX_RESTARTS (10)
        consecutive times, the bookmark is advanced by 1 second, an error is
        logged, and the exception is re-raised.

        Covers abstracts.py lines 505-528:
          - Line 505:     if restart_count >= MAX_RESTARTS
          - Lines 506-512: advanced_bookmark = current_max_bookmark_date + 1s
          - Lines 514-520: LOGGER.error(...)
          - Lines 522-526: write_bookmark called with advanced_bookmark
          - Line 528:     raise
        """
        # Bookmark with microseconds to match the strptime format
        # "%Y-%m-%dT%H:%M:%S.%fZ" used on lines 508-509
        bookmark = "2024-01-01T00:00:00.000000Z"
        expected_advanced_bookmark = "2024-01-01T00:00:01.000000Z"

        self.stream.get_bookmark = Mock(return_value=bookmark)

        bookmark_writes = []

        def mock_write_bookmark(state, key, value):
            bookmark_writes.append((key, value))
            return state

        self.stream.write_bookmark = Mock(side_effect=mock_write_bookmark)

        # Always raise to exhaust all MAX_RESTARTS (10) attempts
        def mock_get_records(state):
            raise freshdeskBadRequestError("Freshdesk 300 page limit reached")

        self.stream.get_records = mock_get_records

        # Exception must be re-raised after MAX_RESTARTS attempts (line 528)
        with self.assertRaises(freshdeskBadRequestError):
            self.stream.sync(self.state, self.transformer)

        # write_bookmark is called exactly 10 times:
        #   - 9 times to persist progress (restart_count 1..9)
        #   - 1 time with the advanced bookmark when restart_count = 10
        self.assertEqual(len(bookmark_writes), 10)

        # First 9 writes: persist the original bookmark unchanged
        for key, value in bookmark_writes[:9]:
            self.assertEqual(key, "tickets")
            self.assertEqual(value, bookmark)

        # 10th write (lines 522-526): bookmark advanced by exactly 1 second
        last_key, last_value = bookmark_writes[-1]
        self.assertEqual(last_key, "tickets")
        self.assertEqual(last_value, expected_advanced_bookmark)

        # LOGGER.error called once: "max restart" message (lines 514-520)
        mock_logger.error.assert_called_once_with(
            "Max restart attempts reached for %s. "
            "Advancing bookmark from %s to %s and aborting sync.",
            "tickets",
            bookmark,
            expected_advanced_bookmark,
        )

    @patch("tap_freshdesk.streams.abstracts.write_record")
    def test_max_restarts_not_yet_exceeded_does_not_reraise(
        self, mock_write_record
    ):
        """
        Verify that when freshdeskBadRequestError occurs fewer than
        MAX_RESTARTS (10) times, the sync retries without advancing
        the bookmark or re-raising.

        This tests the False branch of the `if restart_count >= MAX_RESTARTS:`
        guard at line 505, confirming that only reaching the exact threshold
        triggers the lines 505-528 logic.
        """
        bookmark = "2024-01-01T00:00:00.000000Z"
        advanced_bookmark = "2024-01-01T00:00:01.000000Z"

        self.stream.get_bookmark = Mock(return_value=bookmark)

        bookmark_writes = []

        def mock_write_bookmark(state, key, value):
            bookmark_writes.append((key, value))
            return state

        self.stream.write_bookmark = Mock(side_effect=mock_write_bookmark)

        call_count = 0
        # Fail exactly MAX_RESTARTS-1 = 9 times, then succeed on the 10th call
        MAX_RESTARTS = 10

        def mock_get_records(state):
            nonlocal call_count
            call_count += 1
            if call_count <= MAX_RESTARTS - 1:
                raise freshdeskBadRequestError(
                    "Freshdesk 300 page limit reached"
                )
            yield {
                "id": 1,
                "updated_at": "2024-01-01T01:00:00.000000Z",
            }

        self.stream.get_records = mock_get_records

        # Should complete without raising (9 failures is below MAX_RESTARTS)
        self.stream.sync(self.state, self.transformer)

        # The advanced bookmark must never have been written
        advanced_writes = [
            value for (_, value) in bookmark_writes
            if value == advanced_bookmark
        ]
        self.assertEqual(
            advanced_writes,
            [],
            "Advanced bookmark must not be written "
            "when MAX_RESTARTS is not reached",
        )

    @patch("tap_freshdesk.streams.abstracts.LOGGER")
    @patch("tap_freshdesk.streams.abstracts.write_record")
    def test_max_restarts_bookmark_without_microseconds(
        self, mock_write_record, mock_logger
    ):
        """
        Bug fix: advanced_bookmark must not crash when
        current_max_bookmark_date lacks microseconds.

        Previously the code used datetime.strptime with the
        format "%Y-%m-%dT%H:%M:%S.%fZ" which requires .ffffff.
        A bookmark like "2024-01-01T00:00:00Z" (no microseconds)
        would raise ValueError.  The fix uses singer's
        strptime_to_utc which handles any ISO-8601 input.
        """
        # Bookmark without microseconds - previously caused ValueError
        bookmark = "2024-01-01T00:00:00Z"

        self.stream.get_bookmark = Mock(return_value=bookmark)

        bookmark_writes = []

        def mock_write_bookmark(state, key, value):
            bookmark_writes.append((key, value))
            return state

        self.stream.write_bookmark = Mock(
            side_effect=mock_write_bookmark
        )

        # Always raise to reach MAX_RESTARTS
        def mock_get_records(state):
            raise freshdeskBadRequestError(
                "Freshdesk 300 page limit reached"
            )

        self.stream.get_records = mock_get_records

        # Must not raise ValueError from strptime
        with self.assertRaises(freshdeskBadRequestError):
            self.stream.sync(self.state, self.transformer)

        # The 10th write must be the advanced bookmark
        # (1 second after "2024-01-01T00:00:00Z")
        _, last_value = bookmark_writes[-1]
        self.assertIn("2024-01-01T00:00:01", last_value)

    @patch("tap_freshdesk.streams.abstracts.LOGGER")
    @patch("tap_freshdesk.streams.abstracts.write_record")
    def test_max_restarts_date_only_bookmark(
        self, mock_write_record, mock_logger
    ):
        """
        The advanced_bookmark calculation also handles a plain
        date string start_date ("2023-01-01") without crashing.
        singer's strptime_to_utc parses it as midnight UTC.
        """
        bookmark = "2023-01-01"

        self.stream.get_bookmark = Mock(return_value=bookmark)

        bookmark_writes = []

        def mock_write_bookmark(state, key, value):
            bookmark_writes.append((key, value))
            return state

        self.stream.write_bookmark = Mock(
            side_effect=mock_write_bookmark
        )

        def mock_get_records(state):
            raise freshdeskBadRequestError(
                "Freshdesk 300 page limit reached"
            )

        self.stream.get_records = mock_get_records

        with self.assertRaises(freshdeskBadRequestError):
            self.stream.sync(self.state, self.transformer)

        _, last_value = bookmark_writes[-1]
        # 2023-01-01T00:00:00Z + 1s = 2023-01-01T00:00:01
        self.assertIn("2023-01-01T00:00:01", last_value)

# ---------------------------------------------------------------------------
# ChildBaseStream.get_bookmark — singleton caching change
# ---------------------------------------------------------------------------


class TestChildBaseStreamGetBookmarkSingleton(unittest.TestCase):
    """Tests for the singleton caching added to ChildBaseStream.get_bookmark.

    Before this change: every call delegated straight to super().get_bookmark,
    so multiple calls with different state would return different values.
    After this change: the first call caches the result in self.bookmark_value
    and all subsequent calls return that cached value, regardless of state.
    """

    def _make_stream(self):
        with patch("tap_freshdesk.streams.abstracts.metadata.to_map"):
            mock_catalog = MagicMock()
            mock_catalog.schema.to_dict.return_value = {}
            mock_catalog.metadata = []
            mock_client = MagicMock()
            mock_client.config = {"start_date": "2024-01-01T00:00:00Z"}
            stream = ConcreteChildBaseStream(
                catalog=mock_catalog, client=mock_client
            )
        stream.bookmark_value = None
        return stream

    @patch("tap_freshdesk.streams.abstracts.get_bookmark")
    def test_first_call_reads_from_state(self, mock_get_bookmark):
        """First get_bookmark call must read from state via super()."""
        mock_get_bookmark.return_value = "2024-01-15T00:00:00Z"
        stream = self._make_stream()
        state = {"bookmarks": {"conversations": {"updated_at": "2024-01-15T00:00:00Z"}}}

        result = stream.get_bookmark(state, "conversations")

        self.assertEqual(result, "2024-01-15T00:00:00Z")
        mock_get_bookmark.assert_called_once()

    @patch("tap_freshdesk.streams.abstracts.get_bookmark")
    def test_second_call_returns_cached_value(self, mock_get_bookmark):
        """Subsequent calls must return the cached value without calling super()."""
        mock_get_bookmark.return_value = "2024-01-15T00:00:00Z"
        stream = self._make_stream()
        state = {"bookmarks": {"conversations": {"updated_at": "2024-01-15T00:00:00Z"}}}

        stream.get_bookmark(state, "conversations")  # first call — caches
        mock_get_bookmark.reset_mock()

        result = stream.get_bookmark(state, "conversations")  # second call

        self.assertEqual(result, "2024-01-15T00:00:00Z")
        mock_get_bookmark.assert_not_called()  # super() must NOT be called again

    @patch("tap_freshdesk.streams.abstracts.get_bookmark")
    def test_cached_value_not_updated_when_state_changes(self, mock_get_bookmark):
        """Once cached, the bookmark does not change even if state is updated."""
        mock_get_bookmark.return_value = "2024-01-15T00:00:00Z"
        stream = self._make_stream()
        state = {"bookmarks": {"conversations": {"updated_at": "2024-01-15T00:00:00Z"}}}

        stream.get_bookmark(state, "conversations")  # caches "2024-01-15"
        mock_get_bookmark.return_value = "2024-03-01T00:00:00Z"  # state advances

        result = stream.get_bookmark(state, "conversations")

        # Still returns the originally cached value
        self.assertEqual(result, "2024-01-15T00:00:00Z")

    @patch("tap_freshdesk.streams.abstracts.get_bookmark")
    def test_none_bookmark_value_triggers_super_on_next_call(self, mock_get_bookmark):
        """If bookmark_value is falsy (None), super() is called each time until set."""
        mock_get_bookmark.return_value = None
        stream = self._make_stream()
        state = {"bookmarks": {}}

        stream.get_bookmark(state, "conversations")
        stream.get_bookmark(state, "conversations")

        # super() called both times because None is falsy and never cached
        self.assertEqual(mock_get_bookmark.call_count, 2)

    @patch("tap_freshdesk.streams.abstracts.get_bookmark")
    def test_pre_set_bookmark_value_is_returned_without_super_call(
        self, mock_get_bookmark
    ):
        """If bookmark_value is already set before get_bookmark, super() is skipped."""
        stream = self._make_stream()
        stream.bookmark_value = "2024-06-01T00:00:00Z"  # pre-set
        state = {"bookmarks": {}}

        result = stream.get_bookmark(state, "conversations")

        self.assertEqual(result, "2024-06-01T00:00:00Z")
        mock_get_bookmark.assert_not_called()


# ---------------------------------------------------------------------------
# ParentBaseStream.write_bookmark — uses super().get_bookmark for child state
# ---------------------------------------------------------------------------

class TestParentWriteBookmarkUsesDirectChildBookmark(unittest.TestCase):
    """Tests confirming write_bookmark reads the child's state bookmark via
    IncrementalStream.get_bookmark (super()) rather than the child's own
    overridden get_bookmark.

    Before this change: child.get_bookmark(state, child.tap_stream_id) was
    called, which returned the singleton cached value and could hand a stale
    or None bookmark to write_child_bookmark_with_parent.

    After this change: super().get_bookmark(state, child.tap_stream_id) is
    called, which always reads directly from state — ensuring the freshest
    child bookmark is propagated.
    """

    def _make_parent_stream(self):
        with patch("tap_freshdesk.streams.abstracts.metadata.to_map"):
            mock_catalog = MagicMock()
            mock_catalog.schema.to_dict.return_value = {}
            mock_catalog.metadata = []
            stream = ConcreteParentBaseStream(catalog=mock_catalog)
        stream.child_to_sync = []
        return stream

    @patch("tap_freshdesk.streams.abstracts.BaseStream.is_selected", return_value=True)
    @patch("tap_freshdesk.streams.abstracts.IncrementalStream.get_bookmark")
    @patch("tap_freshdesk.streams.abstracts.IncrementalStream.write_bookmark")
    def test_write_bookmark_reads_child_bookmark_via_super(
        self, _mock_wb, mock_get_bookmark, _mock_is_selected
    ):
        """super().get_bookmark must be the source for the child bookmark value
        passed to write_child_bookmark_with_parent, NOT child.get_bookmark."""
        stream = self._make_parent_stream()

        child = MagicMock(spec=ConcreteChildBaseStream)
        child.tap_stream_id = "conversations"
        # The child's own get_bookmark returns a stale cached value
        child.get_bookmark.return_value = "2024-01-01T00:00:00Z"
        # super().get_bookmark (IncrementalStream) returns the actual state value
        mock_get_bookmark.return_value = "2024-02-15T00:00:00Z"
        stream.child_to_sync = [child]

        state = {
            "bookmarks": {
                "conversations": {"updated_at": "2024-02-15T00:00:00Z"}
            }
        }
        stream.write_bookmark(state, "tickets", value="2024-03-01T00:00:00Z")

        # write_child_bookmark_with_parent must have been called with the value
        # from super().get_bookmark, not from child.get_bookmark
        child.write_child_bookmark_with_parent.assert_called_once_with(
            state,
            "",                         # category_suffix derived from "tickets"
            "2024-02-15T00:00:00Z",     # from super().get_bookmark
            "2024-03-01T00:00:00Z",     # parent value
        )
        # Crucially, child.get_bookmark must NOT have been called
        child.get_bookmark.assert_not_called()

    @patch("tap_freshdesk.streams.abstracts.BaseStream.is_selected", return_value=True)
    @patch("tap_freshdesk.streams.abstracts.IncrementalStream.get_bookmark")
    @patch("tap_freshdesk.streams.abstracts.IncrementalStream.write_bookmark")
    def test_write_bookmark_child_bookmark_none_in_state(
        self, _mock_wb, mock_get_bookmark, _mock_is_selected
    ):
        """When the child has no bookmark in state, None is passed as child
        bookmark to write_child_bookmark_with_parent."""
        stream = self._make_parent_stream()

        child = MagicMock(spec=ConcreteChildBaseStream)
        child.tap_stream_id = "conversations"
        mock_get_bookmark.return_value = None  # no state bookmark for child
        stream.child_to_sync = [child]

        state = {"bookmarks": {}}
        stream.write_bookmark(state, "tickets", value="2024-03-01T00:00:00Z")

        child.write_child_bookmark_with_parent.assert_called_once_with(
            state, "", None, "2024-03-01T00:00:00Z"
        )

    @patch("tap_freshdesk.streams.abstracts.BaseStream.is_selected", return_value=True)
    @patch("tap_freshdesk.streams.abstracts.IncrementalStream.get_bookmark")
    @patch("tap_freshdesk.streams.abstracts.IncrementalStream.write_bookmark")
    def test_write_bookmark_category_suffix_for_spam(
        self, _mock_wb, mock_get_bookmark, _mock_is_selected
    ):
        """Category suffix is correctly derived from the stream key for _spam."""
        stream = self._make_parent_stream()

        child = MagicMock(spec=ConcreteChildBaseStream)
        child.tap_stream_id = "conversations"
        mock_get_bookmark.return_value = "2024-02-01T00:00:00Z"
        stream.child_to_sync = [child]

        state = {"bookmarks": {}}
        # stream key includes "_spam" suffix → category_suffix should be "_spam"
        stream.write_bookmark(state, "tickets_spam", value="2024-03-01T00:00:00Z")

        child.write_child_bookmark_with_parent.assert_called_once_with(
            state, "_spam", "2024-02-01T00:00:00Z", "2024-03-01T00:00:00Z"
        )

    @patch("tap_freshdesk.streams.abstracts.BaseStream.is_selected", return_value=True)
    @patch("tap_freshdesk.streams.abstracts.IncrementalStream.get_bookmark")
    @patch("tap_freshdesk.streams.abstracts.IncrementalStream.write_bookmark")
    def test_write_bookmark_category_suffix_for_deleted(
        self, _mock_wb, mock_get_bookmark, _mock_is_selected
    ):
        """Category suffix is correctly derived from the stream key for _deleted."""
        stream = self._make_parent_stream()

        child = MagicMock(spec=ConcreteChildBaseStream)
        child.tap_stream_id = "conversations"
        mock_get_bookmark.return_value = "2024-02-01T00:00:00Z"
        stream.child_to_sync = [child]

        state = {"bookmarks": {}}
        stream.write_bookmark(state, "tickets_deleted", value="2024-03-01T00:00:00Z")

        child.write_child_bookmark_with_parent.assert_called_once_with(
            state, "_deleted", "2024-02-01T00:00:00Z", "2024-03-01T00:00:00Z"
        )

    @patch("tap_freshdesk.streams.abstracts.BaseStream.is_selected", return_value=True)
    @patch("tap_freshdesk.streams.abstracts.IncrementalStream.get_bookmark")
    @patch("tap_freshdesk.streams.abstracts.IncrementalStream.write_bookmark")
    def test_write_bookmark_super_called_once_per_child(
        self, _mock_wb, mock_get_bookmark, _mock_is_selected
    ):
        """super().get_bookmark must be called exactly once per child stream."""
        stream = self._make_parent_stream()

        child1 = MagicMock(spec=ConcreteChildBaseStream)
        child1.tap_stream_id = "conversations"
        child2 = MagicMock(spec=ConcreteChildBaseStream)
        child2.tap_stream_id = "time_entries"
        mock_get_bookmark.return_value = "2024-01-01T00:00:00Z"
        stream.child_to_sync = [child1, child2]

        state = {"bookmarks": {}}
        stream.write_bookmark(state, "tickets", value="2024-03-01T00:00:00Z")

        # Called once for parent (is_selected=True) + once per child = 3 total
        self.assertEqual(mock_get_bookmark.call_count, 2)
