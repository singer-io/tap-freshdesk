"""Unit tests for tap_freshdesk.client.Client.check_api_credentials."""
import unittest
from unittest.mock import patch, MagicMock

from tap_freshdesk.client import Client, raise_for_error
from tap_freshdesk.exceptions import (
    freshdeskBadRequestError,
    freshdeskError,
    freshdeskUnauthorizedError,
    freshdeskForbiddenError,
)


def _make_client():
    # Return a Client with the underlying requests session mocked out.
    with patch("tap_freshdesk.client.session"):
        client = Client({"domain": "example", "api_key": "testkey"})
    return client


class TestCheckApiCredentials(unittest.TestCase):
    """Tests for Client.check_api_credentials()."""

    AGENTS_ME = "https://example.freshdesk.com/api/v2/agents/me"

    def test_calls_agents_me_on_first_invocation(self):
        client = _make_client()
        with patch.object(client, "get") as mock_get:
            client.check_api_credentials()
        mock_get.assert_called_once_with(
            endpoint=self.AGENTS_ME,
            params={},
            headers={"Accept": "application/json"},
        )

    def test_skips_get_when_already_validated(self):
        """A second call must be a no-op: no HTTP request is made."""
        client = _make_client()
        client._credentials_validated = True
        with patch.object(client, "get") as mock_get:
            client.check_api_credentials()
        mock_get.assert_not_called()

    def test_sets_validated_flag_after_success(self):
        client = _make_client()
        with patch.object(client, "get"):
            client.check_api_credentials()
        self.assertTrue(client._credentials_validated)

    def test_flag_remains_false_when_get_raises(self):
        """If the HTTP call fails the flag must NOT be flipped to True."""
        client = _make_client()
        with patch.object(
            client, "get", side_effect=freshdeskUnauthorizedError("bad key")
        ):
            with self.assertRaises(freshdeskUnauthorizedError):
                client.check_api_credentials()
        self.assertFalse(client._credentials_validated)

    def test_exception_propagates_to_caller(self):
        """Any exception from get() must bubble up unchanged."""
        client = _make_client()
        with patch.object(
            client, "get", side_effect=freshdeskForbiddenError("403")
        ):
            with self.assertRaises(freshdeskForbiddenError):
                client.check_api_credentials()

    def test_second_call_makes_no_additional_http_requests(self):
        """Calling twice should result in exactly one GET, not two."""
        client = _make_client()
        with patch.object(client, "get") as mock_get:
            client.check_api_credentials()
            client.check_api_credentials()
        mock_get.assert_called_once()

    def test_flag_is_false_before_any_call(self):
        client = _make_client()
        self.assertFalse(client._credentials_validated)

    def test_flag_set_allows_reuse_as_context_manager(self):
        """Entering __enter__ calls check_api_credentials; re-entering
        (simulated by a second __enter__) must not make a second request."""
        client = _make_client()
        with patch.object(client, "get") as mock_get:
            mock_get.return_value = {}
            client.__enter__()
            client.__enter__()  # second enter — flag already True
        mock_get.assert_called_once()


class TestRaiseForError(unittest.TestCase):
    """Tests for the raise_for_error helper."""

    def _response(self, status_code=200, payload=None):
        response = MagicMock()
        response.status_code = status_code
        if isinstance(payload, Exception):
            response.json.side_effect = payload
        else:
            response.json.return_value = payload if payload is not None else {}
        return response

    def test_raise_for_error_noop_for_200(self):
        response = self._response(200, {"ok": True})
        raise_for_error(response)

    def test_raise_for_error_uses_error_field(self):
        response = self._response(400, {"error": "validation failed"})
        with self.assertRaises(freshdeskBadRequestError) as err:
            raise_for_error(response)
        self.assertIn("validation failed", str(err.exception))

    def test_raise_for_error_uses_message_field(self):
        response = self._response(400, {"message": "bad input"})
        with self.assertRaises(freshdeskBadRequestError) as err:
            raise_for_error(response)
        self.assertIn("bad input", str(err.exception))

    def test_raise_for_error_falls_back_to_unknown_for_unmapped_status(self):
        response = self._response(418, {})
        with self.assertRaises(freshdeskError) as err:
            raise_for_error(response)
        self.assertIn("Unknown Error", str(err.exception))

    def test_raise_for_error_handles_invalid_json_payload(self):
        response = self._response(400, ValueError("not json"))
        with self.assertRaises(freshdeskBadRequestError):
            raise_for_error(response)


class TestClientRequestExecution(unittest.TestCase):
    """Tests for Client.get/post and __make_request behavior."""

    def test_get_returns_json_payload(self):
        client = _make_client()
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {"id": 1}

        with patch.object(client._session, "request", return_value=response):
            result = client.get(
                endpoint=f"{client.base_url}/agents/me",
                params={},
                headers={"Accept": "application/json"},
            )

        self.assertEqual(result, {"id": 1})

    def test_get_uses_path_when_endpoint_missing(self):
        client = _make_client()
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {"ok": True}

        with patch.object(
            client._session, "request", return_value=response
        ) as req:
            client.get(
                endpoint=None,
                params={"per_page": 1},
                headers={"Accept": "application/json"},
                path="tickets",
            )

        called_endpoint = req.call_args[0][1]
        self.assertEqual(called_endpoint, f"{client.base_url}/tickets")

    def test_post_calls_underlying_request(self):
        client = _make_client()
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {"created": True}

        with patch.object(
            client._session, "request", return_value=response
        ) as req:
            client.post(
                endpoint=f"{client.base_url}/some-endpoint",
                params={},
                headers={"Accept": "application/json"},
                body={"a": 1},
            )

        self.assertTrue(req.called)
