import unittest
from unittest import mock
from parameterized import parameterized
from tap_freshdesk.streams import Agents, Tickets


class TestSyncObj(unittest.TestCase):
    """
    Test `sync_obj` method of stream.
    """

    start_date = "2019-06-01T00:00:00Z"
    only_parent_response = [
        [{"id": i, "updated_at": f"2020-0{i}-01T00:00:00Z"} for i in [1, 5, 2]],    # Tickets Response
        [{"id": "33", "updated_at": f"2020-03-01T00:00:00Z"}],                   # Deleted tickets Response
        [{"id": "55", "updated_at": f"2020-04-01T00:00:00Z"}],                   # Spam tickets Response
    ]
    written_states_1 = {
        "tickets": "2020-05-01T00:00:00Z",
        "tickets_deleted": "2020-03-01T00:00:00Z",
        "tickets_spam": "2020-04-01T00:00:00Z",
    }
    with_child_response = [
        [{"id": i, "updated_at": f"2020-0{i}-01T00:00:00Z"} for i in [1, 5, 2]],    # Tickets Response
        [{"id": i, "updated_at": f"2020-0{i}-01T00:00:00Z"} for i in [2, 4]],    # conversations Response
        [{"id": "33", "updated_at": "2020-03-01T00:00:00Z"}],                   # conversations Response
        [{"id": "55", "updated_at": "2020-04-01T00:00:00Z"}],                   # conversations Response
        [], []   # Deleted/Spam tickets response
    ]
    written_states_2 = {
        "conversations": "2020-04-01T00:00:00Z",
    }
    written_states_3 = {
        "tickets": "2020-05-01T00:00:00Z",
        "conversations": "2020-04-01T00:00:00Z",
    }
    expected_state_1 = {
        "conversations": {"updated_at": "2020-04-01T00:00:00Z"},
        "tickets": {"updated_at": "2020-03-15T00:00:00Z"},
        "tickets_deleted": {"updated_at": "2020-05-01T00:00:00Z"},
        "tickets_spam": {"updated_at": "2020-04-01T00:00:00Z"}
    }
    expected_state_2 = {'conversations': {'updated_at': '2020-04-01T00:00:00Z'},
                        'tickets': {'updated_at': '2019-06-01T00:00:00Z'},
                        'tickets_deleted': {'updated_at': '2020-05-01T00:00:00Z'},
                        'tickets_spam': {'updated_at': '2020-04-01T00:00:00Z'}}
    expected_state_3 = {
        **expected_state_1,
        "tickets": {"updated_at": "2020-03-16T00:00:00Z"},
    }

    @parameterized.expand([
        # ["test_name", "selected_streams", "streams_to_sync", "responses", "written_records", "written_states"]
        ["parent_selected", ["tickets"], ["tickets"],
         only_parent_response, 5, written_states_1],
        ["child_selected", ["conversations"], ["tickets", "conversations"],
         with_child_response, 4, written_states_2],
        ["parent_child_both_selected", ["tickets", "conversations"], ["tickets", "conversations"],
         with_child_response, 7, written_states_3],
    ])
    @mock.patch("singer.write_record")
    @mock.patch("singer.write_bookmark")
    def test_stream_sync_obj(
            self, test_name, selected_streams, streams_to_sync, responses, written_records, written_states,
            mock_write_bookmark, mock_write_record):
        """
        Test that stream is writing records and bookmarks only if selected.
        """
        stream = Tickets()
        state = {}
        client = mock.Mock()
        client.base_url = ""
        client.page_size = 100
        client.request.side_effect = responses
        catalog = [
            {"schema": {}, "tap_stream_id": "tickets", "metadata": []},
            {"schema": {}, "tap_stream_id": "conversations", "metadata": []}
        ]

        stream.sync_obj(state, self.start_date, client, catalog, selected_streams, streams_to_sync)

        # Verify expected records are written
        self.assertEqual(mock_write_record.call_count, written_records)

        # Verify max bookmark is updated for all selected streams
        for stream, bookmark in written_states.items():
            mock_write_bookmark.assert_any_call({}, stream, "updated_at", bookmark)

    @parameterized.expand([
        # ["test_name", "state", "expected_state", "written_records"]
        ["without_state", dict(), expected_state_1, 13],
        ["with_parent_state", {"bookmarks": {"tickets": {"updated_at": "2020-03-16T00:00:00Z"}}}, expected_state_2, 10],
        ["with_child_state", {"bookmarks": {"conversations": {
            "updated_at": "2020-03-23T00:00:00Z"}}}, expected_state_1, 8],
        ["with_both_state", {"bookmarks": {"tickets": {"updated_at": "2020-03-16T00:00:00Z"},
                                           "conversations": {"updated_at": "2020-03-23T00:00:00Z"}}}, expected_state_3, 5],
    ])
    @mock.patch("singer.write_record")
    def test_parent_child_both_selected(self, test_name, state, expected_state, written_records, mock_write_record):
        """
        Test parent and child streams both selected in given conditions:
            - Without a state, all the records will be written.
            - With only parent bookmark parent records with replication value < bookmark will not be written.
            - With only a child bookmark child records with replication value < bookmark will not be written.
            - With both parent and child bookmarks, both follow bookmarks.
        """
        stream = Tickets()
        client = mock.Mock()
        client.base_url = ""
        client.page_size = 100
        client.request.side_effect = [
            [{"id": i, "updated_at": f"2020-03-{i}T00:00:00Z"} for i in [11, 15, 12]],    # Tickets Response
            [{"id": 10+i, "updated_at": f"2020-03-{i}T00:00:00Z"} for i in [13, 24]],    # conversations Response
            [{"id": 13, "updated_at": "2020-03-01T00:00:00Z", "last_edited_at": "2020-03-02T00:00:00Z"}],                 # conversations Response
            [{"id": 95, "updated_at": "2020-04-01T00:00:00Z"}],                 # conversations Response
            [{"id": 73, "updated_at": "2020-05-01T00:00:00Z"}],                 # Deleted tickets response
            [{"id": 30+i, "updated_at": f"2020-03-{i}T00:00:00Z"}for i in [22, 10]],  # conversations response
            [{"id": 43, "updated_at": "2020-04-01T00:00:00Z"}],                 # Spam tickets response
            [{"id": 50+i, "updated_at": f"2020-03-{i}T00:00:00Z"}for i in [12, 25]],  # conversations response
        ]
        catalog = [
            {"schema": {}, "tap_stream_id": "tickets", "metadata": []},
            {"schema": {}, "tap_stream_id": "conversations", "metadata": []}
        ]

        stream.sync_obj(state=state,
                        start_date=self.start_date,
                        client=client,
                        catalog=catalog,
                        selected_streams=["tickets", "conversations"],
                        streams_to_sync=["tickets", "conversations"])
        self.assertEqual(mock_write_record.call_count, written_records)
        self.assertDictEqual(state, {"bookmarks": expected_state})


class TestSyncTransformDict(unittest.TestCase):
    """
    Test `transform_dict` method of stream class.
    """

    stream = Agents()
    expected_list_1 = [{"name": "Agency", "value": "Justice League"},
                       {"name": "Department", "value": "Superhero"}]
    expected_list_2 = [{"key": "Agency", "data": "Justice League"},
                       {"key": "Department", "data": "Superhero"}]
    expected_list_3 = [{"name": "Agency", "value": "justice league"},
                       {"name": "Department", "value": "superhero"}]

    @ parameterized.expand([
        # ["test_name", "dictionary", "expected_list", "kwargs"]
        ["coverting_dict_to_list", {"Agency": "Justice League", "Department": "Superhero"},
         expected_list_1, {}],
        ["With_custom_keys", {"Agency": "Justice League", "Department": "Superhero"},
         expected_list_2, {"key_key": "key", "value_key": "data"}],
        ["With_string_value", {"Agency": "Justice League", "Department": "Superhero"},
         expected_list_3, {"force_str": True}],
    ])
    def test_transform(self, test_name, dictionary, expected_list, kwargs):
        """
        Test that the dictionary is transformed as per given conditions:
            - Value is a lowercase string when force_str: True
            - Key-Values can be customized by passing in args
        """
        returned_list = self.stream.transform_dict(dictionary, **kwargs)

        # Verify returned list is expected
        self.assertEqual(returned_list, expected_list)
