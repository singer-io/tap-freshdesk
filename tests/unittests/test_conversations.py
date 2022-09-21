import unittest
from unittest import mock
from parameterized import parameterized
from tap_freshdesk.streams import Tickets


class TestConversationSync(unittest.TestCase):
    """
    Test `sync_obj` method of conversation stream.
    If `last_edited_at` field is not null then `updated_at` will be overwritten by `last_edited_at`.
    """
    start_date = "2019-06-01T00:00:00Z"

    responses_1 = [
        [{"id": "33", "updated_at": "2020-03-01T00:00:00Z"}],   # Tickets Response
        [{"id": "44", "updated_at": "2020-04-01T00:00:00Z",
          "last_edited_at": "2020-05-01T00:00:00Z"}],           # conversations Response
        [], []
    ]

    responses_2 = [
        [{"id": "33", "updated_at": "2020-03-01T00:00:00Z"}],   # Tickets Response
        [{"id": "44", "updated_at": "2020-04-01T00:00:00Z",
          "last_edited_at": None}],                             # conversations Response
        [], []
    ]

    @parameterized.expand([
        # ["test_name", "responses", "expected_updated_at"]
        ["with last_edited_at value", responses_1, "2020-05-01T00:00:00Z"],
        ["with null last_edited_at", responses_2, "2020-04-01T00:00:00Z"],
    ])
    @mock.patch("singer.write_record")
    @mock.patch("singer.write_bookmark")
    def test_stream(self, test_name, responses, expected_updated_at, mock_write_bookmark, mock_write_record):
        """
        Test that the stream is writing the expected record.
        """
        stream = Tickets()
        state = {}
        client = mock.Mock()
        client.page_size = 100
        client.base_url = ""
        client.request.side_effect = responses

        # Record with expected `updated_at` value
        expected_record = {**responses[1][0], "updated_at": expected_updated_at}
        catalog = [
            {"schema": {}, "tap_stream_id": "tickets", "metadata": []},
            {"schema": {}, "tap_stream_id": "conversations", "metadata": []}
        ]

        stream.sync_obj(state, self.start_date, client, catalog, ["conversations"], ["tickets"])

        # Verify that the expected record is written
        mock_write_record.assert_called_with(mock.ANY, expected_record, time_extracted=mock.ANY)
