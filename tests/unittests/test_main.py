import unittest
from unittest import mock
from singer.catalog import Catalog
from tap_freshdesk import main
from tap_freshdesk.discover import discover


class MockArgs:
    """Mock args object class"""

    def __init__(self, config=None, catalog=None, state={}, discover=False) -> None:
        self.config = config
        self.catalog = catalog
        self.state = state
        self.discover = discover


@mock.patch("tap_freshdesk.FreshdeskClient")
@mock.patch("singer.utils.parse_args")
class TestDiscoverMode(unittest.TestCase):
    """
    Test main function for discover mode
    """

    mock_config = {"start_date": "", "access_token": ""}

    @mock.patch("tap_freshdesk._discover")
    def test_discover_with_config(self, mock_discover, mock_args, mock_verify_access):
        """Test `_discover` function is called for discover mode"""
        mock_discover.return_value = Catalog([])
        mock_args.return_value = MockArgs(
            discover=True, config=self.mock_config)
        main()

        # Verify that `discover` was called
        self.assertTrue(mock_discover.called)


@mock.patch("tap_freshdesk.FreshdeskClient.check_access_token")
@mock.patch("singer.utils.parse_args")
@mock.patch("tap_freshdesk._sync")
class TestSyncMode(unittest.TestCase):
    """
    Test main function for sync mode
    """

    mock_config = {"start_date": "", "access_token": ""}
    mock_catalog = {"streams": [{"stream": "teams", "schema": {}, "metadata": {}}]}

    @mock.patch("tap_freshdesk._discover")
    def test_sync_with_catalog(self, mock_discover, mock_sync, mock_args, mock_check_access_token):
        """Test sync mode with catalog given in args"""

        mock_args.return_value = MockArgs(config=self.mock_config,
                                          catalog=Catalog.from_dict(self.mock_catalog))
        main()

        # Verify `_sync` is called with expected arguments
        mock_sync.assert_called_with(mock.ANY, self.mock_config, {}, self.mock_catalog)

        # verify `_discover` function is not called
        self.assertFalse(mock_discover.called)

    @mock.patch("tap_freshdesk._discover")
    def test_sync_without_catalog(self, mock_discover, mock_sync, mock_args, mock_check_access_token):
        """Test sync mode without catalog given in args"""

        mock_discover.return_value = Catalog.from_dict(self.mock_catalog)
        mock_args.return_value = MockArgs(config=self.mock_config)
        main()

        # Verify `_sync` is called with expected arguments
        mock_sync.assert_called_with(mock.ANY, self.mock_config, {}, self.mock_catalog)

        # verify `_discover` function is  called
        self.assertTrue(mock_discover.called)

    def test_sync_with_state(self, mock_sync, mock_args, mock_check_access_token):
        """Test sync mode with state given in args"""
        mock_state = {"bookmarks": {"projec ts": ""}}
        mock_args.return_value = MockArgs(config=self.mock_config,
                                          catalog=Catalog.from_dict(self.mock_catalog),
                                          state=mock_state)
        main()

        # Verify `_sync` is called with expected arguments
        mock_sync.assert_called_with(mock.ANY, self.mock_config, mock_state, self.mock_catalog)


class TestDiscover(unittest.TestCase):
    """Test `discover` function."""

    def test_discover(self):
        return_catalog = discover()

        # Verify discover function returns `Catalog` type object.
        self.assertIsInstance(return_catalog, Catalog)

    @mock.patch("tap_freshdesk.discover.Schema")
    @mock.patch("tap_freshdesk.discover.LOGGER.error")
    def test_discover_error_handling(self, mock_logger, mock_schema):
        """Test discover function if exception arises."""
        mock_schema.from_dict.side_effect = Exception
        with self.assertRaises(Exception):
            discover()

        # Verify logger called 3 times when an exception arises.
        self.assertEqual(mock_logger.call_count, 3)
