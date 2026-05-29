"""Unit tests for tap_freshdesk discover module and check_stream_access helper."""
import unittest
from unittest.mock import MagicMock, patch

from tap_freshdesk.exceptions import freshdeskUnauthorizedError, freshdeskForbiddenError, freshdeskNoAccessibleStreamsError
from tap_freshdesk.discover import check_stream_access, _check_stream_access, discover
from tap_freshdesk.streams import STREAMS


# ---------------------------------------------------------------------------
# check_stream_access (shared helper)
# ---------------------------------------------------------------------------

class TestCheckStreamAccess(unittest.TestCase):
    """Tests for the shared check_stream_access helper in tap_freshdesk.discover."""

    def test_returns_true_when_probe_succeeds(self):
        result = check_stream_access(
            "tickets",
            probe_fn=lambda: None,
            auth_error_types=(freshdeskUnauthorizedError, freshdeskForbiddenError),
        )
        self.assertTrue(result)

    def test_returns_false_on_unauthorized_error(self):
        def _raise():
            raise freshdeskUnauthorizedError("401 Unauthorized")

        result = check_stream_access(
            "tickets",
            probe_fn=_raise,
            auth_error_types=(freshdeskUnauthorizedError, freshdeskForbiddenError),
        )
        self.assertFalse(result)

    def test_returns_false_on_forbidden_error(self):
        def _raise():
            raise freshdeskForbiddenError("403 Forbidden")

        result = check_stream_access(
            "tickets",
            probe_fn=_raise,
            auth_error_types=(freshdeskUnauthorizedError, freshdeskForbiddenError),
        )
        self.assertFalse(result)

    def test_re_raises_non_auth_error_when_fallback_false(self):
        def _raise():
            raise RuntimeError("500 Server Error")

        with self.assertRaises(RuntimeError):
            check_stream_access(
                "tickets",
                probe_fn=_raise,
                auth_error_types=(freshdeskUnauthorizedError, freshdeskForbiddenError),
                fallback_accessible=False,
            )

    def test_returns_true_on_non_auth_error_when_fallback_true(self):
        def _raise():
            raise RuntimeError("400 Bad Request")

        result = check_stream_access(
            "tickets",
            probe_fn=_raise,
            auth_error_types=(freshdeskUnauthorizedError, freshdeskForbiddenError),
            fallback_accessible=True,
        )
        self.assertTrue(result)


# ---------------------------------------------------------------------------
# _check_stream_access (tap-specific wrapper)
# ---------------------------------------------------------------------------

class TestFreshdeskCheckStreamAccess(unittest.TestCase):
    """Tests for the tap-freshdesk _check_stream_access wrapper in discover.py."""

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
                result = _check_stream_access(client, stream_name, stream_cls)
                self.assertTrue(result, msg=f"Child stream '{stream_name}' should always be True")
                client.get.assert_not_called()

    def test_top_level_stream_returns_true_when_accessible(self):
        client = self._make_client()
        result = _check_stream_access(client, "tickets", STREAMS["tickets"])
        self.assertTrue(result)
        client.get.assert_called_once()

    def test_top_level_stream_returns_false_on_401(self):
        client = self._make_client(side_effect=freshdeskUnauthorizedError("401"))
        result = _check_stream_access(client, "tickets", STREAMS["tickets"])
        self.assertFalse(result)

    def test_top_level_stream_returns_false_on_403(self):
        client = self._make_client(side_effect=freshdeskForbiddenError("403"))
        result = _check_stream_access(client, "agents", STREAMS["agents"])
        self.assertFalse(result)

    def test_top_level_stream_reraises_other_errors(self):
        client = self._make_client(side_effect=ConnectionError("timeout"))
        with self.assertRaises(ConnectionError):
            _check_stream_access(client, "contacts", STREAMS["contacts"])

    def test_probe_url_is_built_from_base_url_and_path(self):
        """Verifies the endpoint passed to client.get is base_url + path."""
        client = self._make_client()
        stream_cls = STREAMS["tickets"]
        _check_stream_access(client, "tickets", stream_cls)
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
    @patch("tap_freshdesk.discover._check_stream_access")
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
    @patch("tap_freshdesk.discover._check_stream_access")
    def test_inaccessible_stream_excluded(self, mock_check, mock_get_schemas):
        """A stream returning False from access check is excluded from the catalog."""
        stream_names = list(STREAMS.keys())
        blocked = "agents"
        mock_get_schemas.return_value = self._minimal_schema_pair(stream_names)
        mock_check.side_effect = lambda client, name, cls: name != blocked

        client = MagicMock()
        catalog = discover(client)
        returned = {s.tap_stream_id for s in catalog.streams}
        self.assertNotIn(blocked, returned)
        self.assertEqual(returned, set(stream_names) - {blocked})

    @patch("tap_freshdesk.discover.get_schemas")
    @patch("tap_freshdesk.discover._check_stream_access")
    def test_all_inaccessible_raises_exception(self, mock_check, mock_get_schemas):
        """When all streams are inaccessible, discover() raises freshdeskNoAccessibleStreamsError."""
        mock_get_schemas.return_value = self._minimal_schema_pair(list(STREAMS.keys()))
        mock_check.return_value = False

        client = MagicMock()
        with self.assertRaises(freshdeskNoAccessibleStreamsError) as ctx:
            discover(client)
        self.assertIn("No stream endpoints are accessible", str(ctx.exception))

    @patch("tap_freshdesk.discover.get_schemas")
    @patch("tap_freshdesk.discover._check_stream_access")
    def test_check_called_for_every_stream(self, mock_check, mock_get_schemas):
        """_check_stream_access is called exactly once per stream."""
        stream_names = list(STREAMS.keys())
        mock_get_schemas.return_value = self._minimal_schema_pair(stream_names)
        mock_check.return_value = True

        client = MagicMock()
        discover(client)
        self.assertEqual(mock_check.call_count, len(stream_names))


if __name__ == "__main__":
    unittest.main()
