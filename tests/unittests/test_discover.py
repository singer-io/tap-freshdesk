"""Unit tests for discovery access checks and catalog pruning."""
import unittest
from unittest.mock import MagicMock, patch

from tap_freshdesk.discover import (
    _apply_access_checks,
    _prune_inaccessible_children,
    check_stream_access,
    discover,
)
from tap_freshdesk.exceptions import freshdeskNotFoundError
from tap_freshdesk.exceptions import freshdeskNoAccessibleStreamsError
from tap_freshdesk.exceptions import freshdeskForbiddenError
from tap_freshdesk.exceptions import freshdeskUnauthorizedError
from tap_freshdesk.streams import STREAMS


class TestCheckStreamAccess(unittest.TestCase):
    """Tests for stream-level access probing wrapper."""

    def test_wrapper_uses_stream_check_access(self):
        client = MagicMock()

        class FakeStream:
            tap_stream_id = "fake_stream"  # required by check_stream_access logging

            def __init__(self, client=None, catalog=None):
                self.client = client

            def check_access(self):
                return True

        self.assertTrue(check_stream_access(client, FakeStream))

    def test_top_level_stream_is_probed(self):
        """A parent-less stream probes its own endpoint directly."""
        client = MagicMock()
        client.base_url = "https://example.freshdesk.com/api/v2"
        client.get.return_value = [{"id": 1}]

        result = check_stream_access(client, STREAMS["agents"])

        self.assertTrue(result)
        client.get.assert_called_once()
        endpoint = client.get.call_args.kwargs["endpoint"]
        self.assertEqual(endpoint, f"{client.base_url}/agents")

    def test_child_stream_is_probed_with_dummy_id(self):
        """Child streams substitute dummy id=1 so only ONE request is made."""
        client = MagicMock()
        client.base_url = "https://example.freshdesk.com/api/v2"
        client.get.return_value = [{"id": 1, "updated_at": "2024-01-01T00:00:00Z"}]

        result = check_stream_access(client, STREAMS["conversations"])

        self.assertTrue(result)
        client.get.assert_called_once()
        endpoint = client.get.call_args.kwargs["endpoint"]
        self.assertEqual(endpoint, f"{client.base_url}/tickets/1/conversations")

    def test_child_stream_probe_raises_for_generic_404(self):
        client = MagicMock()
        client.base_url = "https://example.freshdesk.com/api/v2"
        client.get.side_effect = freshdeskNotFoundError("404")

        with self.assertRaises(freshdeskNotFoundError):
            check_stream_access(client, STREAMS["conversations"])
        client.get.assert_called_once()

    def test_child_stream_probe_returns_false_for_account_not_found_404(self):
        client = MagicMock()
        client.base_url = "https://example.freshdesk.com/api/v2"
        client.get.side_effect = freshdeskNotFoundError(
            "HTTP-error-code: 404, Error: Account not found for the provided domain"
        )

        result = check_stream_access(client, STREAMS["conversations"])

        self.assertFalse(result)
        client.get.assert_called_once()

    def test_parent_stream_probe_returns_false_for_account_not_found_404(self):
        client = MagicMock()
        client.base_url = "https://example.freshdesk.com/api/v2"
        client.get.side_effect = freshdeskNotFoundError(
            "HTTP-error-code: 404, Error: Account not found for the provided domain"
        )

        result = check_stream_access(client, STREAMS["tickets"])

        self.assertFalse(result)
        client.get.assert_called_once()

    def test_parent_stream_probe_returns_false_for_401(self):
        client = MagicMock()
        client.base_url = "https://example.freshdesk.com/api/v2"
        client.get.side_effect = freshdeskUnauthorizedError("401")

        result = check_stream_access(client, STREAMS["tickets"])

        self.assertFalse(result)
        client.get.assert_called_once()

    def test_parent_stream_probe_returns_false_for_403(self):
        client = MagicMock()
        client.base_url = "https://example.freshdesk.com/api/v2"
        client.get.side_effect = freshdeskForbiddenError("403")

        result = check_stream_access(client, STREAMS["tickets"])

        self.assertFalse(result)
        client.get.assert_called_once()

    def test_child_stream_probe_returns_false_for_401(self):
        client = MagicMock()
        client.base_url = "https://example.freshdesk.com/api/v2"
        client.get.side_effect = [
            freshdeskUnauthorizedError("401"),
        ]

        result = check_stream_access(client, STREAMS["conversations"])

        self.assertFalse(result)
        self.assertEqual(client.get.call_count, 1)

    def test_child_stream_probe_returns_false_for_403(self):
        client = MagicMock()
        client.base_url = "https://example.freshdesk.com/api/v2"
        client.get.side_effect = [
            freshdeskForbiddenError("403"),
        ]

        result = check_stream_access(client, STREAMS["conversations"])

        self.assertFalse(result)
        self.assertEqual(client.get.call_count, 1)


class TestAccessChecks(unittest.TestCase):
    """Tests for in-place stream access filtering."""

    def _schemas_and_metadata(self, names):
        schemas = {name: {"type": "object", "properties": {}} for name in names}
        field_metadata = {
            name: [{"metadata": {"table-key-properties": ["id"]}, "breadcrumb": []}]
            for name in names
        }
        return schemas, field_metadata

    @patch("tap_freshdesk.discover.check_stream_access", return_value=True)
    def test_apply_access_checks_keeps_all_accessible_streams(self, _mock_check):
        names = ["tickets", "conversations", "agents"]
        schemas, field_metadata = self._schemas_and_metadata(names)

        _apply_access_checks(MagicMock(), schemas, field_metadata)

        self.assertEqual(set(schemas.keys()), set(names))
        self.assertEqual(set(field_metadata.keys()), set(names))

    @patch("tap_freshdesk.discover.check_stream_access")
    def test_apply_access_checks_removes_inaccessible_streams(self, mock_check):
        names = ["tickets", "agents"]
        schemas, field_metadata = self._schemas_and_metadata(names)

        def _side_effect(_client, stream_cls):
            return stream_cls is not STREAMS["agents"]

        mock_check.side_effect = _side_effect
        _apply_access_checks(MagicMock(), schemas, field_metadata)

        self.assertEqual(set(schemas.keys()), {"tickets"})
        self.assertEqual(set(field_metadata.keys()), {"tickets"})

    @patch("tap_freshdesk.discover.STREAMS")
    def test_prune_inaccessible_children_removes_nested_descendants(self, mock_streams):
        class Parent:
            parent = ""

        class Child:
            parent = "parent"

        class GrandChild:
            parent = "child"

        mock_streams.items.return_value = [
            ("parent", Parent),
            ("child", Child),
            ("grandchild", GrandChild),
        ]

        schemas = {"child": {}, "grandchild": {}}
        field_metadata = {"child": [], "grandchild": []}

        _prune_inaccessible_children(schemas, field_metadata)

        self.assertEqual(schemas, {})
        self.assertEqual(field_metadata, {})

    @patch("tap_freshdesk.discover.check_stream_access", return_value=False)
    def test_apply_access_checks_raises_when_no_stream_access(self, _mock_check):
        names = ["tickets", "conversations"]
        schemas, field_metadata = self._schemas_and_metadata(names)

        with self.assertRaises(freshdeskNoAccessibleStreamsError):
            _apply_access_checks(MagicMock(), schemas, field_metadata)


class TestDiscover(unittest.TestCase):
    """Tests for discover() with access-check orchestration."""

    def _minimal_schema_pair(self, stream_names):
        schemas = {name: {"type": "object", "properties": {}} for name in stream_names}
        metadata_map = {
            name: [{"metadata": {"table-key-properties": ["id"]}, "breadcrumb": []}]
            for name in stream_names
        }
        return schemas, metadata_map

    @patch("tap_freshdesk.discover._apply_access_checks")
    @patch("tap_freshdesk.discover.get_schemas")
    def test_discover_calls_access_checks_before_catalog_build(self, mock_get_schemas, mock_apply):
        stream_names = ["tickets", "conversations"]
        mock_get_schemas.return_value = self._minimal_schema_pair(stream_names)

        client = MagicMock()
        catalog = discover(client)

        self.assertEqual({entry.tap_stream_id for entry in catalog.streams}, set(stream_names))
        client.check_api_credentials.assert_called_once()
        mock_apply.assert_called_once()

    @patch("tap_freshdesk.discover._apply_access_checks")
    @patch("tap_freshdesk.discover.get_schemas")
    def test_discover_fails_fast_for_invalid_credentials(self, mock_get_schemas, mock_apply):
        client = MagicMock()
        client.check_api_credentials.side_effect = freshdeskUnauthorizedError("invalid credentials")

        with self.assertRaises(freshdeskUnauthorizedError):
            discover(client)

        mock_get_schemas.assert_not_called()
        mock_apply.assert_not_called()

    @patch("tap_freshdesk.discover.get_schemas")
    @patch("tap_freshdesk.discover.check_stream_access")
    def test_discover_excludes_child_when_parent_is_inaccessible(self, mock_check, mock_get_schemas):
        stream_names = ["tickets", "conversations", "agents"]
        mock_get_schemas.return_value = self._minimal_schema_pair(stream_names)

        def _side_effect(_client, stream_cls):
            if stream_cls is STREAMS["tickets"]:
                return False
            return True

        mock_check.side_effect = _side_effect

        catalog = discover(MagicMock())
        stream_ids = {entry.tap_stream_id for entry in catalog.streams}

        self.assertEqual(stream_ids, {"agents"})


if __name__ == "__main__":
    unittest.main()
