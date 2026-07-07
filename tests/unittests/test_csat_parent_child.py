"""Unit tests for the csat_surveys (FULL_TABLE parent) → survey_responses
(INCREMENTAL child) parent-child sync workflow.

"""
import unittest
from unittest.mock import MagicMock, patch, call

from tap_freshdesk.streams.csat_surveys import CsatSurveys
from tap_freshdesk.streams.survey_responses import SurveyResponses

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

START_DATE = "2024-01-01T00:00:00Z"

# Survey records (parent)
SURVEY_1 = {"id": 10, "title": "Survey A"}
SURVEY_2 = {"id": 20, "title": "Survey B"}

# Response records (child) — intentionally in ascending updated_at order so
# that the last-passing record is also the latest, matching expected bookmark.
RESPONSE_EARLY  = {"id": 101, "updated_at": "2024-01-05T00:00:00Z", "rating": 5}
RESPONSE_MID    = {"id": 102, "updated_at": "2024-01-15T00:00:00Z", "rating": 3}
RESPONSE_LATEST = {"id": 103, "updated_at": "2024-01-25T00:00:00Z", "rating": 4}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_client():
    """Return a MagicMock client wired with sensible defaults."""
    client = MagicMock()
    client.base_url = "https://example.freshdesk.com/api/v2"
    client.config = {"start_date": START_DATE}
    return client


def _page(records, next_url=None):
    """Build a token-paginated API response envelope."""
    return {"data": records, "paging": {"next": next_url}}


def _make_csat(client):
    stream = CsatSurveys(client=client, catalog=None)
    stream.url_endpoint = f"{client.base_url}/{stream.path}"
    return stream


def _make_responses(client):
    stream = SurveyResponses(client=client, catalog=None)
    # Reset singleton so each test starts with a fresh bookmark read.
    stream.bookmark_value = None
    return stream


def _passthrough_transformer():
    """Transformer that returns a shallow copy of every record unchanged."""
    t = MagicMock()
    t.transform.side_effect = lambda record, schema, meta: dict(record)
    return t


# ---------------------------------------------------------------------------
# Class 1 — csat_surveys is FULL_TABLE, no bookmark written
# ---------------------------------------------------------------------------


class TestCsatSurveysFullTableContract(unittest.TestCase):
    """csat_surveys must behave as a proper FULL_TABLE stream."""

    def test_replication_method_is_full_table(self):
        self.assertEqual(CsatSurveys.replication_method, "FULL_TABLE")

    def test_replication_keys_is_none(self):
        self.assertIsNone(CsatSurveys.replication_keys)

    def test_survey_responses_declared_as_child(self):
        self.assertIn("survey_responses", CsatSurveys.children)

    @patch("tap_freshdesk.streams.csat_surveys.write_record")
    @patch("tap_freshdesk.streams.csat_surveys.metrics.record_counter")
    def test_no_csat_surveys_bookmark_written_after_sync(self, _mc, _mwr):
        """After a sync cycle, 'csat_surveys' must not appear in state bookmarks."""
        client = _make_client()
        client.get.return_value = _page([SURVEY_1])

        csat = _make_csat(client)
        state = {}
        csat.sync(state, _passthrough_transformer())

        self.assertNotIn("csat_surveys", state.get("bookmarks", {}))

    @patch("tap_freshdesk.streams.csat_surveys.write_record")
    @patch("tap_freshdesk.streams.csat_surveys.metrics.record_counter")
    def test_no_bookmarks_key_in_state_at_all_for_parent_only_sync(self, _mc, _mwr):
        """When no child is attached, the state stays empty after a csat sync."""
        client = _make_client()
        client.get.return_value = _page([SURVEY_1])

        csat = _make_csat(client)
        state = {}
        csat.sync(state, _passthrough_transformer())

        self.assertEqual(state, {})


# ---------------------------------------------------------------------------
# Class 2 — survey_responses bookmark structure
# ---------------------------------------------------------------------------


class TestSurveyResponsesBookmarkStructure(unittest.TestCase):
    """survey_responses bookmark must be written correctly after a parent-child sync."""

    def _run_full_sync(self, surveys, responses_per_survey, prior_state=None):
        """
        Run csat.sync() with survey_responses as child.

        ``responses_per_survey`` is a list of record-lists, one per survey in
        order.  E.g. for two surveys: ``[[resp_a], [resp_b]]``.

        Returns the mutated state dict.
        """
        client = _make_client()
        # First call → surveys page; subsequent calls → one responses page each.
        client.get.side_effect = (
            [_page(surveys)]
            + [_page(r) for r in responses_per_survey]
        )

        csat = _make_csat(client)
        responses = _make_responses(client)
        csat.child_to_sync = [responses]

        state = dict(prior_state) if prior_state else {}

        with patch("tap_freshdesk.streams.csat_surveys.write_record"), \
             patch("tap_freshdesk.streams.abstracts.write_record"), \
             patch("tap_freshdesk.streams.csat_surveys.metrics.record_counter"), \
             patch("tap_freshdesk.streams.abstracts.metrics.record_counter"):
            csat.sync(state, _passthrough_transformer())

        return state

    # --- key presence ---

    def test_survey_responses_bookmark_key_exists_after_sync(self):
        state = self._run_full_sync([SURVEY_1], [[RESPONSE_MID]])
        self.assertIn("survey_responses", state.get("bookmarks", {}))

    def test_survey_responses_bookmark_has_updated_at(self):
        state = self._run_full_sync([SURVEY_1], [[RESPONSE_MID]])
        self.assertIn("updated_at", state["bookmarks"]["survey_responses"])

    # --- value correctness ---

    def test_bookmark_set_to_latest_response_updated_at(self):
        """Bookmark advances to the updated_at of the latest emitted record."""
        state = self._run_full_sync(
            [SURVEY_1],
            # Records in ascending order so the last is the latest.
            [[RESPONSE_EARLY, RESPONSE_MID, RESPONSE_LATEST]],
        )
        self.assertEqual(
            state["bookmarks"]["survey_responses"]["updated_at"],
            RESPONSE_LATEST["updated_at"],
        )

    def test_bookmark_set_correctly_across_multiple_surveys(self):
        """With two surveys each having one response, the latest wins."""
        state = self._run_full_sync(
            [SURVEY_1, SURVEY_2],
            [[RESPONSE_MID], [RESPONSE_LATEST]],
        )
        # After two child syncs the singleton bookmark_value is already set from
        # the first survey's call; the second survey's responses are compared
        # against the same threshold.  The final bookmark comes from the second
        # write_child_bookmark_with_parent call.
        self.assertIn("updated_at", state["bookmarks"]["survey_responses"])

    # --- parent bookmark MUST NOT appear ---

    def test_csat_surveys_updated_at_not_in_responses_bookmark(self):
        """
        Parent is FULL_TABLE — its updated_at must NOT appear inside the child
        bookmark entry.
        """
        state = self._run_full_sync([SURVEY_1], [[RESPONSE_MID]])
        child_bm = state["bookmarks"]["survey_responses"]
        self.assertNotIn("csat_surveys_updated_at", child_bm)

    def test_no_csat_surveys_key_in_bookmarks_at_all(self):
        """The parent stream must have no bookmark entry whatsoever."""
        state = self._run_full_sync([SURVEY_1], [[RESPONSE_MID]])
        self.assertNotIn("csat_surveys", state["bookmarks"])


# ---------------------------------------------------------------------------
# Class 3 — Incremental filtering on survey_responses
# ---------------------------------------------------------------------------


class TestSurveyResponsesIncrementalFiltering(unittest.TestCase):
    """Only responses at-or-after the existing bookmark are emitted."""

    def _run_child_sync(self, parent_survey, responses, prior_state=None):
        """
        Run SurveyResponses.sync() directly for a single parent survey.

        Returns ``(state, [written_records])``.
        """
        client = _make_client()
        client.get.return_value = _page(responses)

        stream = _make_responses(client)
        state = dict(prior_state) if prior_state else {}

        written = []
        with patch(
            "tap_freshdesk.streams.abstracts.write_record",
            side_effect=lambda _stream_id, rec: written.append(rec),
        ), patch("tap_freshdesk.streams.abstracts.metrics.record_counter"):
            stream.sync(state, _passthrough_transformer(), parent_obj=parent_survey)

        return state, written

    # --- filtering ---

    def test_record_before_bookmark_is_not_emitted(self):
        prior = {"bookmarks": {"survey_responses": {"updated_at": "2024-01-10T00:00:00Z"}}}
        _, written = self._run_child_sync(SURVEY_1, [RESPONSE_EARLY], prior_state=prior)
        self.assertEqual(written, [])

    def test_record_after_bookmark_is_emitted(self):
        prior = {"bookmarks": {"survey_responses": {"updated_at": "2024-01-10T00:00:00Z"}}}
        _, written = self._run_child_sync(SURVEY_1, [RESPONSE_MID], prior_state=prior)
        self.assertEqual(len(written), 1)
        self.assertEqual(written[0]["id"], RESPONSE_MID["id"])

    def test_mixed_records_only_newer_ones_emitted(self):
        """RESPONSE_EARLY is before bookmark; MID and LATEST are after."""
        prior = {"bookmarks": {"survey_responses": {"updated_at": "2024-01-10T00:00:00Z"}}}
        _, written = self._run_child_sync(
            SURVEY_1,
            [RESPONSE_EARLY, RESPONSE_MID, RESPONSE_LATEST],
            prior_state=prior,
        )
        written_ids = {r["id"] for r in written}
        self.assertNotIn(RESPONSE_EARLY["id"], written_ids)
        self.assertIn(RESPONSE_MID["id"], written_ids)
        self.assertIn(RESPONSE_LATEST["id"], written_ids)

    # --- bookmark advancement ---

    def test_bookmark_advances_after_sync(self):
        prior = {"bookmarks": {"survey_responses": {"updated_at": "2024-01-10T00:00:00Z"}}}
        state, _ = self._run_child_sync(
            SURVEY_1,
            [RESPONSE_MID, RESPONSE_LATEST],  # ascending order
            prior_state=prior,
        )
        self.assertEqual(
            state["bookmarks"]["survey_responses"]["updated_at"],
            RESPONSE_LATEST["updated_at"],
        )

    def test_bookmark_unchanged_when_all_records_before_bookmark(self):
        """No qualifying records → bookmark stays at its prior value."""
        existing_bm = "2024-01-10T00:00:00Z"
        prior = {"bookmarks": {"survey_responses": {"updated_at": existing_bm}}}
        state, _ = self._run_child_sync(SURVEY_1, [RESPONSE_EARLY], prior_state=prior)
        self.assertEqual(
            state["bookmarks"]["survey_responses"]["updated_at"],
            existing_bm,
        )

    def test_first_run_uses_start_date_as_lower_bound(self):
        """With no prior state, all records >= start_date are emitted."""
        # RESPONSE_EARLY (2024-01-05) >= START_DATE (2024-01-01) → emitted
        _, written = self._run_child_sync(SURVEY_1, [RESPONSE_EARLY, RESPONSE_MID])
        self.assertEqual(len(written), 2)

    def test_second_run_uses_existing_bookmark(self):
        """Second run re-uses saved bookmark to filter old records."""
        prior = {"bookmarks": {"survey_responses": {"updated_at": "2024-01-15T00:00:00Z"}}}
        # RESPONSE_EARLY (2024-01-05) < bookmark → skipped
        # RESPONSE_LATEST (2024-01-25) >= bookmark → emitted
        _, written = self._run_child_sync(
            SURVEY_1,
            [RESPONSE_EARLY, RESPONSE_LATEST],
            prior_state=prior,
        )
        written_ids = [r["id"] for r in written]
        self.assertNotIn(RESPONSE_EARLY["id"], written_ids)
        self.assertIn(RESPONSE_LATEST["id"], written_ids)


# ---------------------------------------------------------------------------
# Class 4 — URL endpoint uses the parent survey id
# ---------------------------------------------------------------------------


class TestSurveyResponsesUrlEndpoint(unittest.TestCase):
    """URL endpoint for survey_responses must embed the parent survey id."""

    def test_url_endpoint_includes_parent_survey_id(self):
        client = _make_client()
        stream = _make_responses(client)
        ep = stream.get_url_endpoint(parent_obj={"id": 42})
        self.assertIn("surveys/42/responses", ep)

    def test_url_endpoint_full_path_structure(self):
        client = _make_client()
        stream = _make_responses(client)
        ep = stream.get_url_endpoint(parent_obj={"id": 42})
        expected = (
            f"{client.base_url}/customer-satisfaction/surveys/42/responses"
        )
        self.assertEqual(ep, expected)

    def test_url_endpoint_changes_per_parent_survey(self):
        """Each survey gets its own distinct responses URL."""
        client = _make_client()
        stream = _make_responses(client)
        ep1 = stream.get_url_endpoint(parent_obj={"id": 10})
        ep2 = stream.get_url_endpoint(parent_obj={"id": 20})
        self.assertNotEqual(ep1, ep2)
        self.assertIn("surveys/10/responses", ep1)
        self.assertIn("surveys/20/responses", ep2)


# ---------------------------------------------------------------------------
# Class 5 — child sync is driven once per parent record
# ---------------------------------------------------------------------------


class TestChildSyncDrivenPerParentRecord(unittest.TestCase):
    """survey_responses.sync() must be called exactly once per survey record."""

    @patch("tap_freshdesk.streams.csat_surveys.write_record")
    @patch("tap_freshdesk.streams.csat_surveys.metrics.record_counter")
    def test_child_sync_called_once_per_survey(self, _mc, _mwr):
        client = _make_client()
        # One surveys page with two surveys
        client.get.return_value = _page([SURVEY_1, SURVEY_2])

        csat = _make_csat(client)
        mock_child = MagicMock()
        csat.child_to_sync = [mock_child]

        csat.sync({}, _passthrough_transformer())

        self.assertEqual(mock_child.sync.call_count, 2)

    @patch("tap_freshdesk.streams.csat_surveys.write_record")
    @patch("tap_freshdesk.streams.csat_surveys.metrics.record_counter")
    def test_child_sync_receives_correct_parent_obj(self, _mc, _mwr):
        """Each child.sync() call must receive its corresponding survey record."""
        client = _make_client()
        client.get.return_value = _page([SURVEY_1, SURVEY_2])

        csat = _make_csat(client)
        mock_child = MagicMock()
        csat.child_to_sync = [mock_child]

        csat.sync({}, _passthrough_transformer())

        actual_parents = [
            c.kwargs["parent_obj"] for c in mock_child.sync.call_args_list
        ]
        self.assertEqual(actual_parents[0], SURVEY_1)
        self.assertEqual(actual_parents[1], SURVEY_2)

    @patch("tap_freshdesk.streams.csat_surveys.write_record")
    @patch("tap_freshdesk.streams.csat_surveys.metrics.record_counter")
    def test_child_sync_not_called_when_no_surveys(self, _mc, _mwr):
        """No surveys → child sync is never triggered."""
        client = _make_client()
        client.get.return_value = _page([])  # empty survey page

        csat = _make_csat(client)
        mock_child = MagicMock()
        csat.child_to_sync = [mock_child]

        csat.sync({}, _passthrough_transformer())

        mock_child.sync.assert_not_called()

    def test_child_sync_returns_zero_with_no_parent_obj(self):
        """SurveyResponses.sync() called without a parent record → returns 0."""
        client = _make_client()
        stream = _make_responses(client)
        result = stream.sync({}, _passthrough_transformer(), parent_obj=None)
        self.assertEqual(result, 0)

    def test_child_sync_writes_no_records_when_no_parent_obj(self):
        """No parent object → no write_record calls from child."""
        client = _make_client()
        stream = _make_responses(client)
        with patch("tap_freshdesk.streams.abstracts.write_record") as mock_wr:
            stream.sync({}, _passthrough_transformer(), parent_obj=None)
        mock_wr.assert_not_called()


# ---------------------------------------------------------------------------
# Class 6 — survey_responses incremental stream contract
# ---------------------------------------------------------------------------


class TestSurveyResponsesIncrementalContract(unittest.TestCase):
    """Basic INCREMENTAL contract for SurveyResponses."""

    def test_replication_method_is_incremental(self):
        self.assertEqual(SurveyResponses.replication_method, "INCREMENTAL")

    def test_replication_key_is_updated_at(self):
        self.assertEqual(SurveyResponses.replication_keys, ["updated_at"])

    def test_parent_is_csat_surveys(self):
        self.assertEqual(SurveyResponses.parent, "csat_surveys")

    def test_modify_object_attaches_parent_id(self):
        """Every response record must carry the parent survey id."""
        client = _make_client()
        stream = _make_responses(client)
        result = stream.modify_object({"id": 101}, parent_record={"id": 10})
        self.assertEqual(result["parent_id"], 10)

    def test_modify_object_no_parent_record(self):
        """modify_object should not crash when parent_record is None."""
        client = _make_client()
        stream = _make_responses(client)
        result = stream.modify_object({"id": 101}, parent_record=None)
        self.assertNotIn("parent_id", result)
