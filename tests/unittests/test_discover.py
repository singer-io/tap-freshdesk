"""Unit tests for tap_freshdesk discover module and check_stream_access helper."""
import unittest
from unittest.mock import MagicMock, patch

from tap_freshdesk.exceptions import freshdeskUnauthorizedError, freshdeskForbiddenError, freshdeskNoAccessibleStreamsError
from tap_freshdesk.discover import check_stream_access, discover
from tap_freshdesk.streams import STREAMS


# ---------------------------------------------------------------------------
# check_stream_access
# ---------------------------------------------------------------------------

class TestCheckStreamAccess(unittest.TestCase):
    """Tests for the merged check_stream_access function in tap_freshdesk.discover."""

    def _make_client(self, side_effect=None):
        client = MagicMock()
        client.base_url = "https://example.freshdesk.com/api/v2"
        if side_effect:
            client.get.side_effect = side_effect
        return client

    def test_child_stream_always_accessible(self):
        """Child streams (path contains '{}') are skipped and returned as True."""
        for stream_name, stream_cls in STREAMS.items():
            if '{}' in stream_cls.path:
                client = self._make_client()
                result = check_stream_access(client, stream_cls)
                self.assertTrue(result, msg=f"Child stream '{stream_name}' should always be True")
                client.get.assert_not_called()

    def test_top_level_stream_returns_true_when_accessible(self):
        client = self._make_client()
        result = check_stream_access(client, STREAMS["tickets"])
        self.assertTrue(result)
        client.get.assert_called_once()

    def test_top_level_stream_returns_false_on_401(self):
        client = self._make_client(side_effect=freshdeskUnauthorizedError("401"))
        result = check_stream_access(client, STREAMS["tickets"])
        self.assertFalse(result)

    def test_top_level_stream_returns_false_on_403(self):
        client = self._make_client(side_effect=freshdeskForbiddenError("403"))
        result = check_stream_access(client, STREAMS["agents"])
        self.assertFalse(result)

    def test_top_level_stream_reraises_other_errors(self):
        client = self._make_client(side_effect=ConnectionError("timeout"))
        with self.assertRaises(ConnectionError):
            check_stream_access(client, STREAMS["contacts"])

    def test_probe_url_is_built_from_base_url_and_path(self):
        """Verifies the endpoint passed to client.get is base_url + path."""
        client = self._make_client()
        stream_cls = STREAMS["tickets"]
        check_stream_access(client, stream_cls)
        call_kwargs = client.get.call_args
        called_endpoint = call_kwargs.kwargs.get("endpoint") or call_kwargs[1].get("endpoint")
        self.assertIn(stream_cls.path, called_endpoint)


# ---------------------------------------------------------------------------
# discover()
# ---------------------------------------------------------------------------

class TestDiscover(unittest.TestCase):
    """Tests for the discover() function in tap_freshdesk.discover."""

    def _minimal_schema_pair(self, stream_names):
        schemas = {n: {"type": "object", "properties": {}} for n in stream_names}
        meta = {n: [{"metadata": {"table-key-properties": ["id"]}, "breadcrumb": []}]
                for n in stream_names}
        return schemas, meta

    @patch("tap_freshdesk.discover.get_schemas")
    @patch("tap_freshdesk.discover.check_stream_access")
    def test_all_accessible_streams_in_catalog(self, mock_check, mock_get_schemas):
        """All streams accessible → all appear in the catalog."""
        stream_names = list(STREAMS.keys())
        mock_get_schemas.return_value = self._minimal_schema_pair(stream_names)
        mock_check.return_value = True

        client = MagicMock()
        catalog = discover(client)
        returned = {s.tap_stream_id for s in catalog.streams}
        self.assertEqual(returned, set(stream_names))

    @patch("tap_freshdesk.discover.get_schemas")
    @patch("tap_freshdesk.discover.check_stream_access")
    def test_inaccessible_stream_excluded(self, mock_check, mock_get_schemas):
        """A stream returning False from access check is excluded from the catalog."""
        stream_names = list(STREAMS.keys())
        blocked = "agents"
        mock_get_schemas.return_value = self._minimal_schema_pair(stream_names)
        mock_check.side_effect = lambda client, cls: cls is not STREAMS[blocked]

        client = MagicMock()
        catalog = discover(client)
        returned = {s.tap_stream_id for s in catalog.streams}
        self.assertNotIn(blocked, returned)
        self.assertEqual(returned, set(stream_names) - {blocked})

    @patch("tap_freshdesk.discover.get_schemas")
    @patch("tap_freshdesk.discover.check_stream_access")
    def test_all_inaccessible_raises_exception(self, mock_check, mock_get_schemas):
        """When all streams are inaccessible, discover() raises freshdeskNoAccessibleStreamsError."""
        mock_get_schemas.return_value = self._minimal_schema_pair(list(STREAMS.keys()))
        mock_check.return_value = False

        client = MagicMock()
        with self.assertRaises(freshdeskNoAccessibleStreamsError) as ctx:
            discover(client)
        self.assertIn("The credentials do not have read access to any of the supported streams", str(ctx.exception))

    @patch("tap_freshdesk.discover.get_schemas")
    @patch("tap_freshdesk.discover.check_stream_access")
    def test_check_called_for_every_stream(self, mock_check, mock_get_schemas):
        """check_stream_access is called exactly once per stream."""
        stream_names = list(STREAMS.keys())
        mock_get_schemas.return_value = self._minimal_schema_pair(stream_names)
        mock_check.return_value = True

        client = MagicMock()
        discover(client)
        self.assertEqual(mock_check.call_count, len(stream_names))


if __name__ == "__main__":
    unittest.main()
