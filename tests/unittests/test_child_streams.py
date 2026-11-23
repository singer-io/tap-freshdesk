"""Unit tests for child-stream 404 handling and satisfaction_ratings normalization."""
import json
import os
import unittest
from unittest import mock

from singer import Transformer

import tap_freshdesk
from tap_freshdesk.exceptions import freshdeskNotFoundError
from tap_freshdesk.streams.time_entries import TimeEntries
from tap_freshdesk.streams.conversations import Conversations
from tap_freshdesk.streams.satisfaction_ratings import SatisfactionRatings

SCHEMA_DIR = os.path.join(os.path.dirname(tap_freshdesk.__file__), "schemas")


def load_schema(name):
    with open(os.path.join(SCHEMA_DIR, f"{name}.json")) as f:
        return json.load(f)


class FakeClient:
    base_url = "https://example.freshdesk.com/api/v2"
    config = {"start_date": "2024-10-31T00:00:00Z"}

    def __init__(self, responses):
        self.responses = responses  # path suffix -> list or Exception

    def get(self, url, params, headers, path):
        for suffix, resp in self.responses.items():
            if url.endswith(suffix):
                if isinstance(resp, Exception):
                    raise resp
                return resp if params.get("page", 1) == 1 else []
        raise AssertionError(f"unexpected url {url}")


def make_stream(cls, name, client):
    stream = cls(client=client)
    stream.schema = load_schema(name)
    stream.metadata = {}
    stream.bookmark_value = None
    return stream


STATE = {"bookmarks": {"tickets_spam": {"updated_at": "2026-11-12T14:53:11Z"}}}
SPAM_TICKET = {"id": 243067, "filter": "spam"}


class TestChild404(unittest.TestCase):
    @mock.patch("tap_freshdesk.streams.abstracts.write_record")
    def test_time_entries_404_on_spam_ticket_is_skipped(self, write_record):
        client = FakeClient({"/tickets/243067/time_entries":
                             freshdeskNotFoundError("The resource you have specified cannot be found.")})
        stream = make_stream(TimeEntries, "time_entries", client)
        with Transformer() as transformer:
            stream.sync(state=json.loads(json.dumps(STATE)), transformer=transformer,
                        parent_obj=SPAM_TICKET)
        write_record.assert_not_called()

    @mock.patch("tap_freshdesk.streams.abstracts.write_record")
    def test_conversations_still_written(self, write_record):
        conv = [{"id": 1, "ticket_id": 243067, "body_text": "hi",
                 "created_at": "2026-11-13T00:00:00Z", "updated_at": "2026-11-13T00:00:00Z"}]
        client = FakeClient({"/tickets/243067/conversations": conv})
        stream = make_stream(Conversations, "conversations", client)
        with Transformer() as transformer:
            stream.sync(state=json.loads(json.dumps(STATE)), transformer=transformer,
                        parent_obj=SPAM_TICKET)
        self.assertEqual(write_record.call_count, 1)


class TestRatings(unittest.TestCase):
    @mock.patch("tap_freshdesk.streams.abstracts.write_record")
    def test_ratings_object_is_converted_and_synced(self, write_record):
        rating = [{"id": 9, "ticket_id": 243100, "survey_id": 1, "feedback": "ok",
                   "ratings": {"default_question": 103, "question_2": "102"},
                   "created_at": "2026-11-13T00:00:00Z", "updated_at": "2026-11-13T00:00:00Z"}]
        client = FakeClient({"/tickets/243100/satisfaction_ratings": rating})
        stream = make_stream(SatisfactionRatings, "satisfaction_ratings", client)
        with Transformer() as transformer:
            stream.sync(state={"bookmarks": {"tickets": {"updated_at": "2026-11-12T00:00:00Z"}}},
                        transformer=transformer, parent_obj={"id": 243100})
        self.assertEqual(write_record.call_count, 1)
        written = write_record.call_args[0][1]
        self.assertEqual(written["ratings"], [{"question": "default_question", "value": 103},
                                              {"question": "question_2", "value": 102}])


if __name__ == "__main__":
    unittest.main()
