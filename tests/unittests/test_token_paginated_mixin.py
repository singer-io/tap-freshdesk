"""Unit tests for TokenPaginatedMixin.get_records.

Covers the token-based pagination used by csat_surveys (and survey_responses).

API response shape (from https://developers.freshdesk.com/api/#customer-satisfaction_new):
    {
        "data": [
            {
                "id": "e0de74e5-215c-4ee2-8668-3c9f405cc881",
                "title": "Advanced CSAT Survey",
                ...
            }
        ],
        "paging": {
            "next": "https://domain.freshdesk.com/api/v2/customer-satisfaction/surveys?per_page=2&next_token=abc123",
            "previous": null
        }
    }
"""

import unittest
from unittest.mock import MagicMock, call, patch

from tap_freshdesk.streams.abstracts import FullTableStream, TokenPaginatedMixin


# ---------------------------------------------------------------------------
# Minimal concrete class that combines the mixin with a base stream so we can
# instantiate it without touching the real CsatSurveys stream (which needs a
# full catalog object).
# ---------------------------------------------------------------------------

class ConcreteCsatStream(TokenPaginatedMixin, FullTableStream):
    """Minimal concrete stream for testing TokenPaginatedMixin."""

    tap_stream_id = "csat_surveys"
    key_properties = ["id"]
    replication_keys = None
    path = "customer-satisfaction/surveys"
    page_size = 2  # keep small so pagination tests are readable

    @property
    def replication_method(self):
        return "FULL_TABLE"


def _make_stream():
    """Return a ConcreteCsatStream with all Singer plumbing mocked out."""
    with patch("tap_freshdesk.streams.abstracts.metadata.to_map"):
        mock_catalog = MagicMock()
        mock_catalog.schema.to_dict.return_value = {"type": "object", "properties": {}}
        mock_catalog.metadata = []
        stream = ConcreteCsatStream(client=MagicMock(), catalog=mock_catalog)

    stream.url_endpoint = "https://domain.freshdesk.com/api/v2/customer-satisfaction/surveys"
    stream.params = {}
    return stream


# ---------------------------------------------------------------------------
# Helpers: realistic API payloads taken from the Freshdesk docs.
# ---------------------------------------------------------------------------

SURVEY_1 = {
    "id": "e0de74e5-215c-4ee2-8668-3c9f405cc881",
    "title": "Advanced CSAT Survey",
    "description": "",
    "header_message": "Thank you for your time.",
    "state": "ACTIVE",
    "language": "en",
    "questions": [],
}

SURVEY_2 = {
    "id": "a1b2c3d4-0000-1111-2222-3c9f405cc999",
    "title": "NPS Survey",
    "description": "Net Promoter Score",
    "header_message": "How likely are you to recommend us?",
    "state": "ACTIVE",
    "language": "en",
    "questions": [],
}

SURVEY_3 = {
    "id": "deadbeef-dead-beef-dead-beefdeadbeef",
    "title": "CES Survey",
    "description": "",
    "header_message": "How easy was it to resolve your issue?",
    "state": "DRAFT",
    "language": "en",
    "questions": [],
}

BASE_URL = "https://domain.freshdesk.com/api/v2/customer-satisfaction/surveys"


def _page(data, next_token=None, prev_token=None):
    """Build a paged API response like the Freshdesk CSAT endpoint returns."""
    next_url = f"{BASE_URL}?per_page=2&next_token={next_token}" if next_token else None
    prev_url = f"{BASE_URL}?per_page=2&prev_token={prev_token}" if prev_token else None
    return {"data": data, "paging": {"next": next_url, "previous": prev_url}}


# ===========================================================================
# Test cases
# ===========================================================================

class TestTokenPaginatedMixinSinglePage(unittest.TestCase):
    """get_records stops after one page when paging.next is null."""

    def setUp(self):
        self.stream = _make_stream()

    def test_yields_all_records_from_single_page(self):
        self.stream.client.get.return_value = _page([SURVEY_1, SURVEY_2])

        result = list(self.stream.get_records())

        self.assertEqual(result, [SURVEY_1, SURVEY_2])
        self.stream.client.get.assert_called_once()

    def test_initial_request_uses_per_page(self):
        self.stream.client.get.return_value = _page([SURVEY_1])

        list(self.stream.get_records())

        _, call_args, _ = self.stream.client.get.mock_calls[0]
        params_sent = call_args[1]  # second positional arg is params
        self.assertIn("per_page", params_sent)
        self.assertEqual(params_sent["per_page"], self.stream.page_size)

    def test_no_next_token_in_request_on_first_page(self):
        self.stream.client.get.return_value = _page([SURVEY_1])

        list(self.stream.get_records())

        _, call_args, _ = self.stream.client.get.mock_calls[0]
        params_sent = call_args[1]
        self.assertNotIn("next_token", params_sent)

    def test_empty_data_array_stops_immediately(self):
        self.stream.client.get.return_value = _page([])

        result = list(self.stream.get_records())

        self.assertEqual(result, [])
        self.stream.client.get.assert_called_once()


class TestTokenPaginatedMixinMultiPage(unittest.TestCase):
    """get_records fetches all pages following paging.next tokens."""

    def setUp(self):
        self.stream = _make_stream()

    def test_follows_next_token_across_two_pages(self):
        self.stream.client.get.side_effect = [
            _page([SURVEY_1, SURVEY_2], next_token="tok_page2"),
            _page([SURVEY_3]),
        ]

        result = list(self.stream.get_records())

        self.assertEqual(result, [SURVEY_1, SURVEY_2, SURVEY_3])
        self.assertEqual(self.stream.client.get.call_count, 2)

    def test_second_request_includes_next_token(self):
        self.stream.client.get.side_effect = [
            _page([SURVEY_1], next_token="tok_page2"),
            _page([SURVEY_2]),
        ]

        list(self.stream.get_records())

        second_call = self.stream.client.get.call_args_list[1]
        params_sent = second_call[0][1]
        self.assertEqual(params_sent.get("next_token"), "tok_page2")
        self.assertIn("per_page", params_sent)

    def test_three_pages_all_records_returned(self):
        self.stream.client.get.side_effect = [
            _page([SURVEY_1], next_token="tok2"),
            _page([SURVEY_2], next_token="tok3"),
            _page([SURVEY_3]),
        ]

        result = list(self.stream.get_records())

        self.assertEqual(result, [SURVEY_1, SURVEY_2, SURVEY_3])
        self.assertEqual(self.stream.client.get.call_count, 3)

    def test_stops_when_next_is_null_after_first_page(self):
        """paging.next == null on page 2 must not trigger a third request."""
        self.stream.client.get.side_effect = [
            _page([SURVEY_1], next_token="tok2"),
            _page([SURVEY_2]),          # no next_token → stop
        ]

        list(self.stream.get_records())

        self.assertEqual(self.stream.client.get.call_count, 2)

    def test_each_page_request_passes_correct_per_page(self):
        self.stream.client.get.side_effect = [
            _page([SURVEY_1], next_token="tok2"),
            _page([SURVEY_2]),
        ]

        list(self.stream.get_records())

        for c in self.stream.client.get.call_args_list:
            params = c[0][1]
            self.assertEqual(params["per_page"], self.stream.page_size)


class TestTokenPaginatedMixinPagingEdgeCases(unittest.TestCase):
    """Edge cases around the paging envelope."""

    def setUp(self):
        self.stream = _make_stream()

    def test_missing_paging_key_stops_after_first_page(self):
        """If the API omits 'paging' entirely we must not crash."""
        self.stream.client.get.return_value = {"data": [SURVEY_1]}

        result = list(self.stream.get_records())

        self.assertEqual(result, [SURVEY_1])
        self.stream.client.get.assert_called_once()

    def test_paging_next_is_none_stops_pagination(self):
        self.stream.client.get.return_value = {
            "data": [SURVEY_1],
            "paging": {"next": None, "previous": None},
        }

        result = list(self.stream.get_records())

        self.assertEqual(result, [SURVEY_1])
        self.stream.client.get.assert_called_once()

    def test_next_url_without_next_token_param_stops_pagination(self):
        """A next URL that somehow omits next_token must not loop forever."""
        self.stream.client.get.return_value = {
            "data": [SURVEY_1],
            "paging": {"next": f"{BASE_URL}?per_page=2", "previous": None},
        }

        result = list(self.stream.get_records())

        self.assertEqual(result, [SURVEY_1])
        self.stream.client.get.assert_called_once()

    def test_non_dict_response_yields_nothing(self):
        """A list (unexpected) response must not crash and must yield nothing."""
        self.stream.client.get.return_value = [SURVEY_1, SURVEY_2]

        result = list(self.stream.get_records())

        self.assertEqual(result, [])

    def test_paging_next_token_extracted_correctly_from_full_url(self):
        """The token is parsed from the full URL returned by paging.next."""
        token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
        self.stream.client.get.side_effect = [
            {
                "data": [SURVEY_1],
                "paging": {
                    "next": f"{BASE_URL}?per_page=2&state=ACTIVE&next_token={token}",
                    "previous": None,
                },
            },
            _page([SURVEY_2]),
        ]

        list(self.stream.get_records())

        second_call_params = self.stream.client.get.call_args_list[1][0][1]
        self.assertEqual(second_call_params["next_token"], token)


class TestTokenPaginatedMixinCallArguments(unittest.TestCase):
    """Verify the correct positional arguments are forwarded to client.get."""

    def setUp(self):
        self.stream = _make_stream()

    def test_client_get_called_with_url_endpoint(self):
        self.stream.client.get.return_value = _page([SURVEY_1])

        list(self.stream.get_records())

        first_call = self.stream.client.get.call_args_list[0]
        url_arg = first_call[0][0]
        self.assertEqual(url_arg, self.stream.url_endpoint)

    def test_client_get_called_with_headers(self):
        self.stream.client.get.return_value = _page([SURVEY_1])

        list(self.stream.get_records())

        first_call = self.stream.client.get.call_args_list[0]
        headers_arg = first_call[0][2]
        self.assertEqual(headers_arg, self.stream.headers)

    def test_client_get_called_with_path(self):
        self.stream.client.get.return_value = _page([SURVEY_1])

        list(self.stream.get_records())

        first_call = self.stream.client.get.call_args_list[0]
        path_arg = first_call[0][3]
        self.assertEqual(path_arg, self.stream.path)
