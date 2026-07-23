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


# ---------------------------------------------------------------------------
# Class 7 — SurveyResponses.check_access: 404 treated as accessible
# ---------------------------------------------------------------------------


class TestSurveyResponsesCheckAccess(unittest.TestCase):
    """Tests for the overridden check_access on SurveyResponses.

    The base class (BaseStream.check_access) fetches a real parent survey
    and probes the responses endpoint.  SurveyResponses overrides this so
    that a 404 from the responses probe returns True (no responses yet ≠
    no permission), while 401/403 still propagate as False.
    """

    BASE_URL = "https://example.freshdesk.com/api/v2"

    def _make_stream(self):
        client = _make_client()
        client.base_url = self.BASE_URL
        stream = _make_responses(client)
        return stream

    # --- 404 from the responses endpoint ---

    @patch("tap_freshdesk.streams.abstracts.BaseStream.check_access")
    def test_404_from_base_check_access_returns_true(self, mock_base):
        """A freshdeskNotFoundError raised by the base probe must return True."""
        from tap_freshdesk.exceptions import freshdeskNotFoundError
        mock_base.side_effect = freshdeskNotFoundError("404 - no responses")
        stream = self._make_stream()
        self.assertTrue(stream.check_access())

    @patch("tap_freshdesk.streams.abstracts.BaseStream.check_access")
    def test_404_does_not_propagate(self, mock_base):
        """freshdeskNotFoundError must be swallowed, not re-raised."""
        from tap_freshdesk.exceptions import freshdeskNotFoundError
        mock_base.side_effect = freshdeskNotFoundError("404 - no responses")
        stream = self._make_stream()
        try:
            result = stream.check_access()
        except freshdeskNotFoundError:
            self.fail(
                "check_access() re-raised freshdeskNotFoundError "
                "instead of returning True"
            )
        self.assertTrue(result)

    # --- 200 (accessible) ---

    @patch("tap_freshdesk.streams.abstracts.BaseStream.check_access",
           return_value=True)
    def test_accessible_survey_returns_true(self, _mock_base):
        """When the base probe succeeds, check_access must return True."""
        stream = self._make_stream()
        self.assertTrue(stream.check_access())

    @patch("tap_freshdesk.streams.abstracts.BaseStream.check_access",
           return_value=True)
    def test_base_check_access_called_exactly_once(self, mock_base):
        """check_access must delegate to super() exactly once."""
        stream = self._make_stream()
        stream.check_access()
        mock_base.assert_called_once()

    # --- 401 / 403 still mean no permission ---

    @patch("tap_freshdesk.streams.abstracts.BaseStream.check_access",
           return_value=False)
    def test_unauthorized_propagates_as_false(self, _mock_base):
        """When base returns False (401/403), check_access must return False."""
        stream = self._make_stream()
        self.assertFalse(stream.check_access())

    # --- no parent surveys ---

    @patch("tap_freshdesk.streams.abstracts.BaseStream.check_access",
           return_value=True)
    def test_no_parent_surveys_delegates_to_base(self, mock_base):
        """When the parent has no records, base handles it; we just forward."""
        stream = self._make_stream()
        result = stream.check_access()
        self.assertTrue(result)
        mock_base.assert_called_once()

    # --- integration: real client wired ---

    def test_check_access_via_real_client_404_on_responses_returns_true(self):
        """End-to-end: parent returns one survey; responses returns 404."""
        from tap_freshdesk.exceptions import freshdeskNotFoundError
        from tap_freshdesk.streams import STREAMS

        client = _make_client()
        client.base_url = self.BASE_URL
        # First call → csat_surveys list (parent fetch)
        # Second call → responses for that survey → 404
        client.get.side_effect = [
            [{"id": "uuid-abc", "title": "Survey A"}],
            freshdeskNotFoundError("404 - no responses for this survey"),
        ]

        stream = SurveyResponses(client=client, catalog=None)
        stream.bookmark_value = None

        self.assertTrue(stream.check_access())
        self.assertEqual(client.get.call_count, 2)
        # Second call must target the responses endpoint for the real survey id
        second_endpoint = client.get.call_args_list[1].kwargs["endpoint"]
        self.assertIn("uuid-abc", second_endpoint)
        self.assertIn("responses", second_endpoint)

    def test_check_access_via_real_client_403_returns_false(self):
        """End-to-end: parent returns one survey; responses returns 403."""
        from tap_freshdesk.exceptions import freshdeskForbiddenError

        client = _make_client()
        client.base_url = self.BASE_URL
        client.get.side_effect = [
            [{"id": "uuid-abc", "title": "Survey A"}],
            freshdeskForbiddenError("403 - forbidden"),
        ]

        stream = SurveyResponses(client=client, catalog=None)
        stream.bookmark_value = None

        self.assertFalse(stream.check_access())


# ---------------------------------------------------------------------------
# Class 8 — SurveyResponses.sync: 404 per-survey skips gracefully
# ---------------------------------------------------------------------------


class TestSurveyResponsesSyncNotFoundHandling(unittest.TestCase):
    """Tests for the 404 guard added to SurveyResponses.sync.

    When the Freshdesk API returns 404 for a specific survey's responses
    endpoint, sync() must log a warning and return 0 (rather than crashing),
    so that the parent CsatSurveys loop continues to the next survey.
    """

    def _make_stream(self):
        client = _make_client()
        stream = _make_responses(client)
        return stream

    def _passthrough_transformer(self):
        t = MagicMock()
        t.transform.side_effect = lambda rec, schema, meta: dict(rec)
        return t

    # --- 404 on a specific survey ---

    def test_sync_returns_zero_on_404(self):
        """sync() must return 0 when the survey endpoint returns 404."""
        from tap_freshdesk.exceptions import freshdeskNotFoundError
        stream = self._make_stream()
        stream.client.get.side_effect = freshdeskNotFoundError("404")
        result = stream.sync(
            {}, self._passthrough_transformer(), parent_obj=SURVEY_1
        )
        self.assertEqual(result, 0)

    def test_sync_does_not_reraise_on_404(self):
        """freshdeskNotFoundError must be caught, not propagated."""
        from tap_freshdesk.exceptions import freshdeskNotFoundError
        stream = self._make_stream()
        stream.client.get.side_effect = freshdeskNotFoundError("404")
        try:
            stream.sync(
                {}, self._passthrough_transformer(), parent_obj=SURVEY_1
            )
        except freshdeskNotFoundError:
            self.fail(
                "sync() re-raised freshdeskNotFoundError "
                "instead of catching it"
            )

    def test_sync_writes_no_records_on_404(self):
        """No records must be emitted when the survey returns 404."""
        from tap_freshdesk.exceptions import freshdeskNotFoundError
        stream = self._make_stream()
        stream.client.get.side_effect = freshdeskNotFoundError("404")
        with patch(
            "tap_freshdesk.streams.abstracts.write_record"
        ) as mock_wr:
            stream.sync(
                {}, self._passthrough_transformer(), parent_obj=SURVEY_1
            )
        mock_wr.assert_not_called()

    def test_sync_continues_after_404_on_one_survey(self):
        """CsatSurveys must call sync for the next survey even after a 404."""
        from tap_freshdesk.exceptions import freshdeskNotFoundError
        client = _make_client()
        # surveys page → two surveys
        # survey 1 responses → 404
        # survey 2 responses → one record
        client.get.side_effect = [
            _page([SURVEY_1, SURVEY_2]),
            freshdeskNotFoundError("404 - survey 1 has no responses"),
            _page([RESPONSE_MID]),
        ]

        csat = _make_csat(client)
        responses = _make_responses(client)
        csat.child_to_sync = [responses]

        with patch(
            "tap_freshdesk.streams.csat_surveys.write_record"
        ), patch(
            "tap_freshdesk.streams.csat_surveys.metrics.record_counter"
        ), patch(
            "tap_freshdesk.streams.abstracts.write_record"
        ), patch(
            "tap_freshdesk.streams.abstracts.metrics.record_counter"
        ):
            csat.sync({}, _passthrough_transformer())

        # 3 get calls: surveys page + 2 × responses page
        self.assertEqual(client.get.call_count, 3)

    def test_sync_returns_zero_on_none_parent(self):
        """sync() called without a parent must return 0 immediately."""
        stream = self._make_stream()
        result = stream.sync(
            {}, self._passthrough_transformer(), parent_obj=None
        )
        self.assertEqual(result, 0)
        stream.client.get.assert_not_called()

    # --- happy path still works ---

    def test_sync_returns_count_on_success(self):
        """When responses exist, sync() must return the number emitted."""
        client = _make_client()
        prior = {"bookmarks": {"survey_responses": {"updated_at": START_DATE}}}
        client.get.return_value = _page(
            [RESPONSE_MID, RESPONSE_LATEST]
        )
        stream = _make_responses(client)

        with patch(
            "tap_freshdesk.streams.abstracts.write_record"
        ), patch(
            "tap_freshdesk.streams.abstracts.metrics.record_counter"
        ) as mock_counter:
            mock_counter.return_value.__enter__ = MagicMock(
                return_value=MagicMock(value=2)
            )
            mock_counter.return_value.__exit__ = MagicMock(return_value=False)
            stream.sync(
                prior, _passthrough_transformer(), parent_obj=SURVEY_1
            )
        # No exception raised — test passes if we reach here

    def test_sync_state_not_mutated_on_404(self):
        """State must be unchanged when a survey returns 404."""
        from tap_freshdesk.exceptions import freshdeskNotFoundError
        prior = {
            "bookmarks": {
                "survey_responses": {"updated_at": "2024-01-01T00:00:00Z"}
            }
        }
        import copy
        original_state = copy.deepcopy(prior)

        stream = self._make_stream()
        stream.client.get.side_effect = freshdeskNotFoundError("404")
        stream.sync(prior, _passthrough_transformer(), parent_obj=SURVEY_1)

        self.assertEqual(prior, original_state)
