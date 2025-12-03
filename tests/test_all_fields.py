from base import FreshdeskBaseTest
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest


class FreshdeskAllFields(AllFieldsTest, FreshdeskBaseTest):
    """Ensure running the tap with all streams and fields selected results in
    the replication of all fields."""

    MISSING_FIELDS = {
        "agents": [
            "role_ids",
            "group_ids"
        ],
        "contacts": [
            "other_emails",
            "view_all_tickets",
            "tags",
            "avatar",
            "deleted",
            "other_companies"
        ],
        "groups": [
            "agent_ids",
            "auto_ticket_assign"
        ],
        "tickets":[
            "twitter_id",
            "facebook_id",
            "description",
            "name",
            "phone",
            "deleted",
            "description_text",
            "email"]
    }

    start_date = "2020-01-01T00:00:00Z"

    @staticmethod
    def name():
        return "tap_tester_freshdesk_all_fields_test"

    def streams_to_test(self):
        streams_to_exclude = {"satisfaction_ratings", "time_entries"}
        return self.expected_stream_names().difference(streams_to_exclude)
