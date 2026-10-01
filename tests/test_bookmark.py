from base import FreshdeskBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest

# Suffixes that the tap appends to base stream names when writing per-category
# (spam / deleted) bookmark entries into state.
_CATEGORY_SUFFIXES = ("_spam", "_deleted")


class FreshdeskBookMarkTest(BookmarkTest, FreshdeskBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""

    start_date = "2020-02-01T00:00:00Z"
    bookmark_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    initial_bookmarks = {
        "bookmarks": {
            "contacts": {"updated_at": "2022-02-02T00:00:00.000000Z"},
            "companies": {"updated_at": "2022-08-17T13:58:07.000000Z"}
        }
    }

    @staticmethod
    def get_stream_name(stream_id):
        """Map a state bookmark key to its canonical stream name.

        The Freshdesk tap writes per-category bookmark entries for tickets and
        their child streams (e.g. ``tickets_spam``, ``conversations_deleted``).
        Strip the known suffixes so the framework can match these keys back to
        the base stream name used in ``streams_to_test()``.
        """
        for suffix in _CATEGORY_SUFFIXES:
            if stream_id.endswith(suffix):
                return stream_id[: -len(suffix)]
        return stream_id

    @staticmethod
    def name():
        return "tap_tester_freshdesk_bookmark_test"

    def streams_to_test(self):
        streams_to_exclude = {
            "satisfaction_ratings",
            "time_entries",
            "agents",
            "groups",
            "roles",
            "tickets_spam",
            "conversations_spam",
            "tickets_deleted",
            "conversations_deleted",
            "conversations",
            "tickets",
            "scenario_automations",
            "contact_fields",
            "email_mailboxes",
            "company_fields",
            "email_configs",
            "sla_policies",
            "ticket_fields",
            "business_hours",
            "account",  # Full Table
            "csat_surveys",  # Full Table
            "skills",
            "products",
            "survey_responses",
            "surveys"
            }
        return self.expected_stream_names().difference(streams_to_exclude)

    def calculate_new_bookmarks(self):
        """Calculates new bookmarks by looking through sync 1 data to determine
        a bookmark that will sync 2 records in sync 2 (plus any necessary look
        back data)"""
        new_bookmarks = {
            "contacts": {"updated_at": "2026-07-20T12:13:09.000000Z"},
            "companies": {"updated_at": "2026-07-22T06:45:12.000000Z"}
        }

        return new_bookmarks
