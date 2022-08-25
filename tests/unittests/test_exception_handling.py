import unittest
from unittest import mock
import json
import requests
from parameterized import parameterized
from tap_freshdesk import client
from tap_freshdesk.client import raise_for_error, ERROR_CODE_EXCEPTION_MAPPING
from tap_freshdesk.streams import Tickets, TimeEntries

def get_response(status_code, json_resp={}, headers = None):
    """
    Returns mock response
    """
    response = requests.Response()
    response.status_code = status_code
    response._content = json.dumps(json_resp).encode()
    if headers:
        response.headers = headers
    return response


class TestExceptionHanfling(unittest.TestCase):
    """
    Test Error is thrown with the expected error message.
    """

    @parameterized.expand([
        [400, client.FreshdeskValidationError],
        [401, client.FreshdeskAuthenticationError],
        [403, client.FreshdeskAccessDeniedError],
        [404, client.FreshdeskNotFoundError],
        [405, client.FreshdeskMethodNotAllowedError],
        [406, client.FreshdeskUnsupportedAcceptHeaderError],
        [409, client.FreshdeskConflictingStateError],
        [415, client.FreshdeskUnsupportedContentError],
        [429, client.FreshdeskRateLimitError],
        [500, client.FreshdeskServerError],
        [503, client.Server5xxError],  # Unknown 5xx error
    ])
    def test_custom_error_message(self, error_code, error):
        """
        Test that error is thrown with the custom error message
        if no description is provided in response.
        """
        expected_message = "HTTP-error-code: {}, Error: {}".format(error_code, ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("message", "Unknown Error"))
        with self.assertRaises(error) as e:
            raise_for_error(get_response(error_code))

        # Verify that an error message is expected
        self.assertEqual(str(e.exception), expected_message)

    @parameterized.expand([
        [400, "Client or Validation Error", None, client.FreshdeskValidationError],
        [401, "Authentication Failure", "invalid_credentials", client.FreshdeskAuthenticationError],
        [403, "Access Denied", "access_denied", client.FreshdeskAccessDeniedError],
        [404, "Requested Resource not Found", None, client.FreshdeskNotFoundError],
        [405, "Method not allowed", None, client.FreshdeskMethodNotAllowedError],
        [406, "Unsupported Accept Header", None, client.FreshdeskUnsupportedAcceptHeaderError],
        [409, "AInconsistent/Conflicting State", "inconsistent_state", client.FreshdeskConflictingStateError],
        [415, "Unsupported Content-type", "invalid_json", client.FreshdeskUnsupportedContentError],
        [429, "Rate Limit Exceeded", None, client.FreshdeskRateLimitError],
        [500, "Unexpected Server Error", None, client.FreshdeskServerError],
        [503, "Service Unavailable", None, client.Server5xxError],    # Unknown 5xx error
    ])
    def test_error_response_message(self, status_code, message, code, error):
        """
        Test that error is thrown with description in the response.
        """

        error_code = status_code
        if code:
            error_code = f"{str(status_code)} {code}"
        expected_message = "HTTP-error-code: {}, Error: {}".format(error_code, message)
        with self.assertRaises(error) as e:
            raise_for_error(get_response(status_code, {"description": message, "code": code}))

        # Verify that an error message is expected
        self.assertEqual(str(e.exception), expected_message)

    def json_decoder_error(self):
        """Test for invalid json response, tap does not throw JSON decoder error."""
        mock_response = get_response(400, {"description": "Client or Validation Error", "code": None})
        mock_response._content = "ABC".encode()
        expected_message = "HTTP-error-code: {}, Error: {}".format(400, "Client or Validation Error")
        with self.assertRaises(client.FreshdeskValidationError) as e:
            raise_for_error(mock_response)

        # Verify that an error message is expected
        self.assertEqual(str(e.exception), expected_message)



class TestBackoffHandling(unittest.TestCase):
    """
    Test backoff handling for all 5xx, timeout and connection error.
    """

    @parameterized.expand([
        ["For error 500", lambda *x,**y: get_response(500), client.FreshdeskServerError],
        ["For 503 (unknown 5xx error)", lambda *x,**y:get_response(503), client.Server5xxError],   # Unknown 5xx error
        ["For Connection Error", requests.ConnectionError, requests.ConnectionError],
        ["For timeour Error", requests.Timeout, requests.Timeout],
    ])
    @mock.patch("requests.Session.send")
    @mock.patch("time.sleep")
    def test_backoff(self, name, mock_response, error, mock_sleep, mock_request):
        """
        Test that for 500, timeout and connection error `request` method will back off 5 times.
        """
        mock_request.side_effect = mock_response
        config = {"user_agent": "SAMPLE_AGENT", "api_key": "TEST_API_KEY"}
        _client = client.FreshdeskClient(config)
        with self.assertRaises(error) as e:
            _client.request("https://TEST_URL.com")

        # Verify that `request` method was called 5 times.
        self.assertEqual(mock_request.call_count, 5)


@mock.patch("requests.Session.send")
@mock.patch("tap_freshdesk.client.time.sleep")
class TestRateLimitHandling(unittest.TestCase):
    """
    Test rate-limit exception handling.
    """

    @parameterized.expand([
        ["30"],
        ["5"],
        ["50"],
    ])
    def test_rate_limit_exceeded(self, mock_sleep, mock_request, retry_seconds):
        """
        Test that when the rate limit is exceeded, the function is called again after `Retry-After` seconds.
        """
        mock_request.side_effect = [get_response(429, headers={"Retry-After": retry_seconds}), get_response(200)]
        config = {"user_agent": "SAMPLE_AGENT", "api_key": "TEST_API_KEY"}
        _client = client.FreshdeskClient(config)
        _client.request("https://TEST_URL.com")

        # Verify that `requests` method is called twice.
        self.assertEqual(mock_request.call_count, 2)

        # Verify that `time.sleep` was called for 'Retry-After' seconds from the header.
        mock_sleep.assert_any_call(int(retry_seconds))

    def test_rate_limit_not_exceeded(self, mock_sleep, mock_request):
        """
        Test that the function will not retry for the success response.
        """
        mock_request.side_effect = [get_response(200)]
        config = {"user_agent": "SAMPLE_AGENT", "api_key": "TEST_API_KEY"}
        _client = client.FreshdeskClient(config)
        _client.request("https://TEST_URL.com")

        # Verify that `requests` method is called once.
        self.assertEqual(mock_request.call_count, 1)
        mock_request.assert_called_with(mock.ANY, timeout=client.REQUEST_TIMEOUT)


class TestSkip404(unittest.TestCase):
    """
    Test handling of 404 for a specific child.
    """

    @mock.patch("tap_freshdesk.streams.LOGGER.warning")
    @mock.patch("tap_freshdesk.client.FreshdeskClient.request")
    def test_child_stream_skips(self, mock_request, mock_logger):
        """
        Test that on 404 error is skipped for `TimeEntries`.
        """
        stream = TimeEntries()
        _client = mock.Mock()
        _client.base_url = ""
        _client.request.side_effect = client.FreshdeskNotFoundError

        stream.parent_id = 10
        stream.sync_obj({}, "START_DATE", _client, {}, [], [])

        # Verify that error is not raised and the warning logger is called.
        mock_logger.assert_called_with("Could not retrieve time entries for ticket id 10. This may be caused by tickets marked as spam or deleted.")
