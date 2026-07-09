import unittest
from unittest.mock import MagicMock, Mock, patch

import requests

from tap_freshdesk.client import Client, get_backoff_time, raise_for_error
from tap_freshdesk.exceptions import (
    freshdeskBackoffError,
    freshdeskBadRequestError,
    freshdeskError,
    freshdeskInternalServerError,
    freshdeskRateLimitError,
    freshdeskUnauthorizedError,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_response(status_code, json_data=None, headers=None):
    """Build a minimal mock requests.Response."""
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    resp.headers = headers or {}
    return resp


def _make_client():
    return Client(
        {
            "api_key": "test_key",
            "domain": "testdomain",
            "start_date": "2023-01-01",
        }
    )


# ---------------------------------------------------------------------------
# freshdeskRateLimitError
# ---------------------------------------------------------------------------


class TestFreshdeskRateLimitError(unittest.TestCase):
    """Tests for the updated freshdeskRateLimitError.__init__."""

    def test_retry_after_extracted_from_header(self):
        """Retry-After header value is stored as a float."""
        response = _make_response(429, headers={"Retry-After": "45"})
        err = freshdeskRateLimitError("rate limited", response)
        self.assertEqual(err.retry_after, 45.0)

    def test_retry_after_float_in_header(self):
        """Fractional Retry-After values are parsed correctly."""
        response = _make_response(429, headers={"Retry-After": "1.5"})
        err = freshdeskRateLimitError("rate limited", response)
        self.assertEqual(err.retry_after, 1.5)

    def test_retry_after_defaults_to_60_when_header_missing(self):
        """Missing Retry-After header defaults to 60.0 (float) seconds."""
        response = _make_response(429, headers={})
        err = freshdeskRateLimitError("rate limited", response)
        self.assertEqual(err.retry_after, 60.0)
        self.assertIsInstance(err.retry_after, float)

    def test_retry_after_defaults_to_60_on_invalid_value(self):
        """Non-numeric Retry-After header defaults to 60 seconds."""
        response = _make_response(
            429, headers={"Retry-After": "not-a-number"}
        )
        err = freshdeskRateLimitError("rate limited", response)
        self.assertEqual(err.retry_after, 60)

    def test_retry_after_is_none_when_no_response(self):
        """retry_after is None when no response object is given."""
        err = freshdeskRateLimitError("rate limited")
        self.assertIsNone(err.retry_after)

    def test_message_includes_retry_after_seconds(self):
        """Error message contains the Retry-After duration."""
        response = _make_response(429, headers={"Retry-After": "30"})
        err = freshdeskRateLimitError("Rate limited", response)
        self.assertIn("Retry after 30.0 seconds", str(err))

    def test_message_uses_default_text_when_no_custom_message(self):
        """Default message is used when none is provided."""
        response = _make_response(429, headers={"Retry-After": "30"})
        err = freshdeskRateLimitError(response=response)
        self.assertIn("FreshDesk Rate Limit Exceeded", str(err))

    def test_message_has_no_retry_after_suffix_when_no_response(self):
        """No 'Retry after' suffix when retry_after is None."""
        err = freshdeskRateLimitError("Rate limited")
        self.assertNotIn("Retry after", str(err))

    def test_is_subclass_of_backoff_error(self):
        """freshdeskRateLimitError must remain a freshdeskBackoffError."""
        self.assertTrue(issubclass(freshdeskRateLimitError, freshdeskBackoffError))


# ---------------------------------------------------------------------------
# get_backoff_time
# ---------------------------------------------------------------------------


class TestGetBackoffTime(unittest.TestCase):
    """Tests for the get_backoff_time() helper."""

    def test_returns_retry_after_from_rate_limit_exception(self):
        """Returns the retry_after value from a rate-limit exception."""
        response = _make_response(429, headers={"Retry-After": "45"})
        exception = freshdeskRateLimitError("rate limited", response)
        result = get_backoff_time({"exception": exception})
        self.assertEqual(result, 45.0)

    def test_returns_default_when_retry_after_is_none(self):
        """Returns 60 when the exception carries no retry_after."""
        exception = freshdeskRateLimitError("rate limited")
        # no response → retry_after is None
        result = get_backoff_time({"exception": exception})
        self.assertEqual(result, 60.0)

    def test_returns_default_for_non_rate_limit_exception(self):
        """Returns 60 when the exception is not a rate-limit error."""
        result = get_backoff_time({"exception": Exception("generic")})
        self.assertEqual(result, 60.0)

    def test_handles_exception_passed_directly(self):
        """Defensive branch: exception passed as the value instead of dict."""
        exception = freshdeskRateLimitError("rate limited")
        result = get_backoff_time(exception)
        self.assertEqual(result, 60.0)

    @patch("tap_freshdesk.client.LOGGER")
    def test_warning_is_logged(self, mock_logger):
        """A warning is always logged regardless of exception type."""
        exception = freshdeskRateLimitError("rate limited")
        get_backoff_time({"exception": exception})
        mock_logger.warning.assert_called_once()

    @patch("tap_freshdesk.client.LOGGER")
    def test_logged_wait_time_matches_retry_after(self, mock_logger):
        """The logged wait time matches the extracted retry_after."""
        response = _make_response(429, headers={"Retry-After": "77"})
        exception = freshdeskRateLimitError("rate limited", response)
        get_backoff_time({"exception": exception})
        logged_args = mock_logger.warning.call_args[0]
        # second positional arg is the wait time
        self.assertEqual(logged_args[1], 77.0)


# ---------------------------------------------------------------------------
# raise_for_error
# ---------------------------------------------------------------------------


class TestRaiseForError(unittest.TestCase):
    """Tests for raise_for_error() HTTP status → exception mapping."""

    def test_200_does_not_raise(self):
        """A 200 response must not raise any exception."""
        raise_for_error(_make_response(200))

    def test_400_raises_bad_request_error(self):
        with self.assertRaises(freshdeskBadRequestError):
            raise_for_error(_make_response(400))

    def test_401_raises_unauthorized_error(self):
        with self.assertRaises(freshdeskUnauthorizedError):
            raise_for_error(_make_response(401))

    def test_429_raises_rate_limit_error(self):
        with self.assertRaises(freshdeskRateLimitError):
            raise_for_error(_make_response(429))

    def test_500_raises_internal_server_error(self):
        with self.assertRaises(freshdeskInternalServerError):
            raise_for_error(_make_response(500))

    def test_unknown_status_code_raises_freshdesk_error(self):
        with self.assertRaises(freshdeskError):
            raise_for_error(_make_response(418))

    def test_error_message_taken_from_error_field(self):
        """When JSON has an 'error' key, it appears in the exception message."""
        response = _make_response(400, json_data={"error": "Bad input data"})
        with self.assertRaises(freshdeskBadRequestError) as ctx:
            raise_for_error(response)
        self.assertIn("Bad input data", str(ctx.exception))

    def test_error_message_taken_from_message_field(self):
        """When JSON has a 'message' key, it appears in the exception message."""
        response = _make_response(
            400, json_data={"message": "Validation failed"}
        )
        with self.assertRaises(freshdeskBadRequestError) as ctx:
            raise_for_error(response)
        self.assertIn("Validation failed", str(ctx.exception))

    def test_error_message_falls_back_to_mapping(self):
        """When JSON has neither key, the mapping's default message is used."""
        response = _make_response(400, json_data={})
        with self.assertRaises(freshdeskBadRequestError) as ctx:
            raise_for_error(response)
        self.assertIn("400", str(ctx.exception))

    def test_json_parse_failure_does_not_crash(self):
        """If response.json() raises, raise_for_error still maps the error."""
        response = _make_response(500)
        response.json.side_effect = ValueError("not json")
        with self.assertRaises(freshdeskInternalServerError):
            raise_for_error(response)


# ---------------------------------------------------------------------------
# Client retry behaviour
# ---------------------------------------------------------------------------


class TestClientRetryBehavior(unittest.TestCase):
    """
    Tests that verify the backoff/retry decorators on Client.__make_request.

    Layout of decorators (outermost first):
      @backoff.on_exception(expo,    NetworkErrors + freshdeskBackoffError)
      @backoff.on_exception(runtime, freshdeskRateLimitError)
      def __make_request(...)

    Execution order when a request fails:
      • freshdeskRateLimitError  → caught by inner (runtime) decorator first
      • freshdeskBackoffError    → bypasses inner, caught by outer (expo) decorator
      • Network errors           → bypasses inner, caught by outer (expo) decorator
    """

    def setUp(self):
        self.client = _make_client()

    # -- 429 rate-limit -------------------------------------------------------

    @patch("time.sleep", return_value=None)
    def test_rate_limit_error_is_retried(self, _mock_sleep):
        """
        A 429 response triggers the runtime-backoff retry.
        Second call succeeds → result is returned.
        """
        self.client._session.request = Mock(
            side_effect=[
                _make_response(429, headers={"Retry-After": "0"}),
                _make_response(200, json_data=[{"id": 1}]),
            ]
        )
        result = self.client.get("http://test.com", {}, {})
        self.assertEqual(self.client._session.request.call_count, 2)
        self.assertEqual(result, [{"id": 1}])

    @patch("time.sleep", return_value=None)
    def test_rate_limit_retry_after_header_governs_sleep(self, mock_sleep):
        """
        The Retry-After value from the response governs the sleep duration
        via get_backoff_time.
        """
        self.client._session.request = Mock(
            side_effect=[
                _make_response(429, headers={"Retry-After": "7"}),
                _make_response(200, json_data=[{"id": 2}]),
            ]
        )
        self.client.get("http://test.com", {}, {})
        # time.sleep must have been called with the Retry-After value
        sleep_calls = [call.args[0] for call in mock_sleep.call_args_list]
        self.assertIn(7.0, sleep_calls)

    # -- 5xx server errors (freshdeskBackoffError) ----------------------------

    @patch("time.sleep", return_value=None)
    def test_5xx_error_is_retried_with_expo_backoff(self, _mock_sleep):
        """
        Bug fix: freshdeskBackoffError (5xx) subclasses must be retried by
        the exponential-backoff decorator.

        Previously freshdeskBackoffError was removed from the outer decorator,
        causing 5xx errors to propagate immediately without retry.
        """
        self.client._session.request = Mock(
            side_effect=[
                _make_response(500),
                _make_response(200, json_data=[{"id": 3}]),
            ]
        )
        result = self.client.get("http://test.com", {}, {})
        self.assertEqual(self.client._session.request.call_count, 2)
        self.assertEqual(result, [{"id": 3}])

    @patch("time.sleep", return_value=None)
    def test_502_bad_gateway_is_retried(self, _mock_sleep):
        """502 Bad Gateway (a freshdeskBackoffError) must also be retried."""
        self.client._session.request = Mock(
            side_effect=[
                _make_response(502),
                _make_response(200, json_data=[{"id": 4}]),
            ]
        )
        result = self.client.get("http://test.com", {}, {})
        self.assertEqual(self.client._session.request.call_count, 2)
        self.assertEqual(result, [{"id": 4}])

    @patch("time.sleep", return_value=None)
    def test_503_service_unavailable_is_retried(self, _mock_sleep):
        """503 Service Unavailable must be retried."""
        self.client._session.request = Mock(
            side_effect=[
                _make_response(503),
                _make_response(200, json_data=[{"id": 5}]),
            ]
        )
        result = self.client.get("http://test.com", {}, {})
        self.assertEqual(self.client._session.request.call_count, 2)
        self.assertEqual(result, [{"id": 5}])

    # -- Network errors -------------------------------------------------------

    @patch("time.sleep", return_value=None)
    def test_timeout_is_retried(self, _mock_sleep):
        """A Timeout exception must trigger exponential-backoff retry."""
        from requests.exceptions import Timeout

        self.client._session.request = Mock(
            side_effect=[
                Timeout(),
                _make_response(200, json_data=[{"id": 6}]),
            ]
        )
        result = self.client.get("http://test.com", {}, {})
        self.assertEqual(self.client._session.request.call_count, 2)
        self.assertEqual(result, [{"id": 6}])

    @patch("time.sleep", return_value=None)
    def test_connection_error_is_retried(self, _mock_sleep):
        """A ConnectionError must trigger exponential-backoff retry."""
        from requests.exceptions import ConnectionError as ReqConnError

        self.client._session.request = Mock(
            side_effect=[
                ReqConnError(),
                _make_response(200, json_data=[{"id": 7}]),
            ]
        )
        result = self.client.get("http://test.com", {}, {})
        self.assertEqual(self.client._session.request.call_count, 2)
        self.assertEqual(result, [{"id": 7}])

    # -- Non-retryable errors -------------------------------------------------

    @patch("time.sleep", return_value=None)
    def test_400_bad_request_is_not_retried(self, _mock_sleep):
        """
        freshdeskBadRequestError (400) does NOT extend freshdeskBackoffError
        and must NOT be retried.
        """
        self.client._session.request = Mock(
            return_value=_make_response(400)
        )
        with self.assertRaises(freshdeskBadRequestError):
            self.client.get("http://test.com", {}, {})
        # Only one attempt — no retry
        self.assertEqual(self.client._session.request.call_count, 1)

    @patch("time.sleep", return_value=None)
    def test_rate_limit_exhausts_retries_and_reraises(self, _mock_sleep):
        """
        After exhausting max_tries (5) on 429s, the exception propagates.
        """
        self.client._session.request = Mock(
            return_value=_make_response(429, headers={"Retry-After": "0"})
        )
        with self.assertRaises(freshdeskRateLimitError):
            self.client.get("http://test.com", {}, {})
        self.assertEqual(self.client._session.request.call_count, 5)
