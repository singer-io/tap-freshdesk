import unittest
from unittest import mock

from tap_freshdesk import client
from parameterized import parameterized


@mock.patch("tap_freshdesk.client.FreshdeskClient.request")
class TestAccessToken(unittest.TestCase):
    """
    Test case to verify the `check_access_token` method.
    """
    @parameterized.expand([
        ('free_account_with_invalid_token', client.FreshdeskAuthenticationError, 'The Authorization header is either missing or incorrect.'),
        ('free_account_with_invalid_domain', client.FreshdeskNotFoundError, 'The request contains invalid ID/Freshdesk domain in the URL or an invalid URL itself.')
    ])
    def test_invalid_domain_or_token(self, mock_request, name, error, err_message):
        """
        Verify that tap raise error for an invalid token or invalid domain name.
        """
        config = {}
        mock_request.side_effect = error(err_message)
        _client = client.FreshdeskClient(config)
        with self.assertRaises(error) as e:
            _client.check_access_token()
        
        # Verify that an error message is expected
        self.assertEqual(str(e.exception), err_message) 

    @mock.patch("tap_freshdesk.client.LOGGER.warning")
    def test_free_account(self, mock_logger, mock_request):
        """
        Verify that tap provide a warning message to upgrade the current free account to the pro account.
        Because `satisfaction_ratings` and `time_entries` streams are accessible only for the pro account.
        """
        config = {}
        mock_request.side_effect = client.FreshdeskAccessDeniedError
        _client = client.FreshdeskClient(config)

        _client.check_access_token()
        
        # Verify that `LOGGER.warning` is called for 1 time.
        self.assertEqual(mock_logger.call_count, 1)

        # Verify expected warning message.
        mock_logger.assert_called_with('The `Surveys` and the `Timesheets` features are not supported in your current plan. '\
            'So, Data collection cannot be initiated for satisfaction_ratings and time_entries streams. Please upgrade your '\
            'account to `Pro` plan to use it.')

    @mock.patch("tap_freshdesk.client.LOGGER.warning")
    def test_pro_account_plan(self, mock_logger, mock_request):
        """
        Verify that tap does not raise any error or provide any warning message for the pro account plan.
        """
        config = {}
        _client = client.FreshdeskClient(config)
        
        _client.check_access_token()

        # Verify that `LOGGER.warning` is not called.
        self.assertEqual(mock_logger.call_count, 0)
