import unittest
from unittest.mock import patch
from datetime import datetime
import requests
from tap_freshdesk.client import Client, freshdeskBackoffError
from tap_freshdesk.exceptions import freshdeskUnauthorizedError
from requests.exceptions import ConnectionError, Timeout, ChunkedEncodingError


class Mockresponse:
    """Mock response object class."""

    def __init__(self, status_code, json, raise_error, headers={}, text=None):
        self.status_code = status_code
        self.raise_error = raise_error
        self.text = json
        self.headers = headers
        self.content = "github"

    def raise_for_status(self):
        if not self.raise_error:
            return self.status_code

        raise requests.HTTPError("Sample message")

    def json(self):
        """Response JSON method."""
        return self.text


def get_response(status_code, json={}, raise_error=False):
    """Returns required mock response."""
    return Mockresponse(status_code, json, raise_error)


@patch("tap_freshdesk.client.Client._refresh_access_token")
@patch("tap_freshdesk.client.Client.check_active_account")
class TestMakeRequest(unittest.TestCase):
    @patch("requests.Session.request", return_value=get_response(200, {"result": []}))
    def test_successful_request(
        self, mocked_request, mock_refresh_token, mock_check_active_account
    ):
        """Test case for successful request."""
        url = "dummy_endpoint"
        params = {}
        headers = {"Authorization": "Bearer dummy_token"}

        # Mocking the Client behavior
        client_config = {"user_agent": "singer"}

        # Create a client instance
        with Client(client_config) as client:
            # Call the method
            result = client.get(url, params, headers)

            # Assertions
            mocked_request.assert_called_once_with(
                "GET", url, headers=headers, params=params, timeout=300
            )
            self.assertEqual(result, {"result": []})

    @patch("time.sleep")
    @patch("requests.Session.request", side_effect=ConnectionError)
    def test_connection_error(
        self, mocked_request, mock_sleep, mock_refresh_token, mock_check_active_account
    ):
        """Test case for ConnectionError."""
        url = "dummy_endpoint"
        params = {}
        headers = {"Authorization": "Bearer dummy_token"}

        # Mocking the Client behavior
        client_config = {"user_agent": "singer"}

        # Create a client instance
        with self.assertRaises(ConnectionError):
            with Client(client_config) as client:
                # Call the method
                client.get(url, params, headers)

        # Ensure the request was retried up to the backoff limit
        self.assertEqual(mocked_request.call_count, 5)

    @patch("time.sleep")
    @patch("requests.Session.request", side_effect=Timeout)
    def test_timeout_error(
        self, mocked_request, mock_sleep, mock_refresh_token, mock_check_active_account
    ):
        """Test case for Timeout error."""
        url = "dummy_endpoint"
        params = {}
        headers = {"Authorization": "Bearer dummy_token"}

        # Mocking the Client behavior
        client_config = {"user_agent": "singer"}

        # Create a client instance
        with self.assertRaises(Timeout):
            with Client(client_config) as client:
                # Call the method
                client.get(url, params, headers)

        # Ensure the request was retried up to the backoff limit
        self.assertEqual(mocked_request.call_count, 5)

    @patch("time.sleep")
    @patch("requests.Session.request", side_effect=ChunkedEncodingError)
    def test_chunked_encoding_error(
        self, mocked_request, mock_sleep, mock_refresh_token, mock_check_active_account
    ):
        """Test case for ChunkedEncodingError."""
        url = "dummy_endpoint"
        params = {}
        headers = {"Authorization": "Bearer dummy_token"}

        # Mocking the Client behavior
        client_config = {"user_agent": "singer"}

        # Create a client instance
        with self.assertRaises(ChunkedEncodingError):
            with Client(client_config) as client:
                # Call the method
                client.get(url, params, headers)

        # Ensure the request was retried up to the backoff limit
        self.assertEqual(mocked_request.call_count, 5)

    @patch("time.sleep")
    @patch("requests.Session.request")
    def test_freshdesk_rate_limit_error(
        self, mocked_request, mock_sleep, mock_refresh_token, mock_check_active_account
    ):
        """Test case for 429 Rate Limit error."""
        mocked_request.side_effect = [
            get_response(429, {}, True)
        ] * 5  # Simulate 5 retries for 429 error
        url = "dummy_endpoint"
        params = {}
        headers = {"Authorization": "Bearer dummy_token"}

        # Mocking the Client behavior
        client_config = {"user_agent": "singer"}

        # Create a client instance
        with self.assertRaises(freshdeskBackoffError):
            with Client(client_config) as client:
                # Call the method
                client.get(url, params, headers)

        # Ensure the request was retried up to the backoff limit
        self.assertEqual(mocked_request.call_count, 5)

    @patch("time.sleep")
    @patch("requests.Session.request")
    def test_401_error(
        self, mocked_request, mock_sleep, mock_refresh_token, mock_check_active_account
    ):
        """Test case for 401 Unauthorized error."""
        mocked_request.side_effect = [
            get_response(401, {}, True),
            get_response(401, {}, True),
        ]
        url = "dummy_endpoint"
        params = {}
        headers = {"Authorization": "Bearer dummy_token"}

        # Mocking the Client behavior
        client_config = {"user_agent": "singer"}

        # Create a client instance
        with self.assertRaises(freshdeskUnauthorizedError) as e:
            with Client(client_config) as client:
                # Call the method
                client.get(url, params, headers)

        self.assertEqual(mocked_request.call_count, 1)
        self.assertEqual(
            str(e.exception),
            "HTTP-error-code: 401, Error: The access token provided is expired, revoked, malformed or invalid for other reasons.",
        )


class TestClient(unittest.TestCase):
    @patch("tap_freshdesk.client.Client._refresh_access_token")
    @patch("tap_freshdesk.client.Client.get", return_value={"accounts": [{"id": 123}]})
    def test_check_active_account(self, mock_get, mock_refresh_token):
        """Test case for checking active account."""
        # Mocking the Client behavior
        client_config = {"user_agent": "singer"}

        # Create a client instance
        with Client(client_config) as client:
            # Assertions
            mock_get.assert_called_once_with(
                endpoint="https://id.getfreshdesk.com/api/v2/accounts"
            )
            self.assertEqual(client._account_id, "123")

    @patch("tap_freshdesk.client.Client._refresh_access_token")
    @patch("tap_freshdesk.client.Client.get", return_value={"accounts": []})
    def test_check_no_active_account(self, mock_get, mock_refresh_token):
        """Test case for no active account."""
        # Mocking the Client behavior
        client_config = {"user_agent": "singer"}

        with self.assertRaises(Exception) as e:
            # Create a client instance
            with Client(client_config):
                pass

        self.assertEqual(str(e.exception), "No Active freshdesk Account found")

    @patch("tap_freshdesk.client.Client._refresh_access_token")
    @patch("tap_freshdesk.client.datetime")
    def test_get_access_token_expired(self, mock_datetime, mock_refresh_token):
        """Test case for expired access token."""
        # Mocking the datetime to control the current time
        mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0)

        # Mocking the Client behavior
        client_config = {"user_agent": "singer"}

        client = Client(client_config)

        client._expires_at = datetime(2023, 1, 1, 11, 0, 0)

        # Call the method
        client.get_access_token()

        client._refresh_access_token.assert_called_once()
