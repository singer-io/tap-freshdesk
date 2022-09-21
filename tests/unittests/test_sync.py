import unittest
from unittest import mock
from parameterized import parameterized
from tap_freshdesk.sync import (write_schemas, get_selected_streams,
                                get_stream_to_sync, sync)


def get_stream_catalog(stream_name, is_selected=False):
    """Return catalog for stream"""
    return {
        "schema": {},
        "tap_stream_id": stream_name,
        "metadata": [
            {
                "breadcrumb": [],
                "metadata":{"selected": is_selected}
            }
        ],
        "key_properties": []
    }


def get_catalog(parent=False, child=False):
    """Return complete catalog"""

    return {
        "streams": [
            get_stream_catalog("agents"),
            get_stream_catalog("companies", parent),
            get_stream_catalog("conversations", child),
            get_stream_catalog("tickets", parent),
            get_stream_catalog("time_entries", child),
            get_stream_catalog("groups", parent),
        ]
    }


class TestSyncFunctions(unittest.TestCase):
    """
    Test `sync` function.
    """

    # NOTE: For `tickets` stream `sync_obj` is called 3 times
    @parameterized.expand([
        # ["test_name", "mock_catalog", "selected_streams", "synced_streams"]
        ["only_parent_selected", get_catalog(parent=True),
         ["companies", "tickets", "groups"], 5],
        ["only_child_selected", get_catalog(child=True),
         ["conversations", "time_entries"], 3],
        ["both_selected", get_catalog(parent=True, child=True),
         ["companies", "tickets", "groups", "conversations", "time_entries"], 5],
        ["No_streams_selected", get_catalog(),
         [], 0],
    ])
    @mock.patch("singer.write_state")
    @mock.patch("singer.write_schema")
    @mock.patch("tap_freshdesk.streams.Stream.sync_obj")
    def test_sync(self, test_name, mock_catalog, selected_streams, synced_streams,
                  mock_sync_endpoint, mock_write_schemas, mock_write_state):
        """
        Test sync function that for child streams selected parent sync_obj is called and,
        schema is written only for selected streams.
        """
        client = mock.Mock()
        sync(client, {'start_date': ""}, {}, mock_catalog)

        # Verify write schema is called for selected streams
        self.assertEqual(mock_write_schemas.call_count, len(selected_streams))
        for stream in selected_streams:
            mock_write_schemas.assert_any_call(stream, mock.ANY, mock.ANY)

        # Verify sync object was called for syncing parent streams
        self.assertEqual(mock_sync_endpoint.call_count, synced_streams)


class TestUtilsFunction(unittest.TestCase):
    """
    Test functions used in `sync`.
    """

    mock_catalog = {"streams": [
        get_stream_catalog("tickets"),
        get_stream_catalog("time_entries"),
        get_stream_catalog("conversations")
    ]}

    @parameterized.expand([
        # [test_name, selected_streams, mock_write_schema]
        ["parents_selected", ["tickets"]],
        ["child_selected", ["time_entries"]],
        ["parent_and_child_selected", ["tickets", "conversations"]],
    ])
    @mock.patch("singer.write_schema")
    def test_write_schema(self, test_name, selected_streams, mock_write_schema):
        """
        Test that `write_schemas` function writes schema of only selected streams.
        """
        write_schemas("tickets", self.mock_catalog, selected_streams)
        for stream in selected_streams:
            # Verify write_schema function is called.
            mock_write_schema.assert_any_call(stream, mock.ANY, mock.ANY)

    @parameterized.expand([
        # ["test_name", "selected_streams", "expected_streams"]
        ['test_parent_selected', ["tickets"], ["tickets"]],
        ['test_child_selected', ["conversations", "satisfaction_ratings"], ["tickets"]],
        ['test_both_selected', ["conversations", "roles", "tickets"], ["roles", "tickets"]]
    ])
    def test_get_sync_streams(self, test_name, selected_streams, expected_streams):
        """
        Test that if an only child is selected in the catalog,
        then `get_stream_to_sync` returns the parent streams if selected stream is child.
        """
        sync_streams = get_stream_to_sync(selected_streams)

        # Verify that the expected list of streams is returned
        self.assertEqual(sync_streams, expected_streams)

    def test_streams_selection(self):
        """
        Test that  `get_selected_streams` returns the list of selected streams.
        """
        catalog = {"streams": [
            get_stream_catalog("tickets", is_selected=True),
            get_stream_catalog("roles", is_selected=True),
            get_stream_catalog("contacts"),
            get_stream_catalog("groups", is_selected=True),
            get_stream_catalog("agents"),
        ]}
        expected_streams = ["tickets", "roles", "groups"]
        selected_streams = get_selected_streams(catalog)

        # Verify expected list is returned
        self.assertEqual(selected_streams, expected_streams)
