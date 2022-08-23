import unittest
from unittest import mock
import json
import requests
from parameterized import parameterized
from tap_freshdesk import client
from tap_freshdesk.client import raise_for_error, ERROR_CODE_EXCEPTION_MAPPING

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
        (400, client.FresdeskValidationError),
        (401, client.FresdeskAuthenticationError),
        (403, client.FresdeskAccessDeniedError),
        (404, client.FresdeskNotFoundError),
        (405, client.FresdeskMethodNotAllowedError),
        (406, client.FresdeskUnsupportedAcceptHeaderError),
        (409, client.FresdeskConflictingStateError),
        (415, client.FresdeskUnsupportedContentError),
        (429, client.FresdeskRateLimitError),
        (500, client.FresdeskServerError),
        (503, client.Server5xxError),  # Unknown 5xx error
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
        (400, "Client or Validation Error", client.FresdeskValidationError),
        (401, "Authentication Failure", client.FresdeskAuthenticationError),
        (403, "Access Denied", client.FresdeskAccessDeniedError),
        (404, "Requested Resource not Found", client.FresdeskNotFoundError),
        (405, "Method not allowed", client.FresdeskMethodNotAllowedError),
        (406, "Unsupported Accept Header", client.FresdeskUnsupportedAcceptHeaderError),
        (409, "AInconsistent/Conflicting State", client.FresdeskConflictingStateError),
        (415, "Unsupported Content-type", client.FresdeskUnsupportedContentError),
        (429, "Rate Limit Exceeded", client.FresdeskRateLimitError),
        (500, "Unexpected Server Error", client.FresdeskServerError),
    ])
    def test_error_response_message(self, error_code, message, error):
        """
        Test that error is thrown with description in the response.
        """
        expected_message = "HTTP-error-code: {}, Error: {}".format(error_code, message)
        with self.assertRaises(error) as e:
            raise_for_error(get_response(error_code, {"description": message}))

        # Verify that an error message is expected
        self.assertEqual(str(e.exception), expected_message)

    def json_decoder_error(self):
        """Test for invalid json response, tap does not throw JSON decoder error."""
        mock_response = get_response(400, {"description": "Client or Validation Error"})
        mock_response._content = "ABC".encode()
        expected_message = "HTTP-error-code: {}, Error: {}".format(400, "Client or Validation Error")
        with self.assertRaises(client.FresdeskValidationError) as e:
            raise_for_error(mock_response)

        # Verify that an error message is expected
        self.assertEqual(str(e.exception), expected_message)


@mock.patch("requests.Session.send")
@mock.patch("time.sleep")
class TestBackoffHandling(unittest.TestCase):
    """
    Test backoff handling for all 5xx, timeout and connection error.
    """

    @parameterized.expand([
        (lambda *x,**y:get_response(500), client.FresdeskServerError),
        (lambda *x,**y:get_response(503), client.Server5xxError),   # Unknown 5xx error
        (ConnectionError, ConnectionError),
        (TimeoutError, TimeoutError),
    ])
    def test_backoff(self, mock_sleep, mock_request, mock_response, error):
        """
        Test that for 500, timeout and connection error `request` method will backoff 5 times.
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
        ("30",),
        ("5",),
        ("50",),
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

    def test_rate_limite_not_exceeded(self, mock_sleep, mock_request):
        """
        Test that the function will not retry for the success response.
        """
        mock_request.side_effect = [get_response(200)]
        config = {"user_agent": "SAMPLE_AGENT", "api_key": "TEST_API_KEY"}
        _client = client.FreshdeskClient(config)
        _client.request("https://TEST_URL.com")

        # Verify that `requests` method is called once.
        self.assertEqual(mock_request.call_count, 1)
        mock_request.assert_called_with(mock.ANY, timeout=client.DEFAULT_TIMEOUT)
