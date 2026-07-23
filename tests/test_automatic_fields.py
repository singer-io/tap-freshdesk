"""Test that with no fields selected for a stream automatic fields are still
replicated."""
from base import FreshdeskBaseTest
from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest


class FreshdeskAutomaticFields(MinimumSelectionTest, FreshdeskBaseTest):
    """Test that with no fields selected for a stream automatic fields are
    still replicated."""

    start_date = "2020-02-01T00:00:00Z"

    @staticmethod
    def name():
        return "tap_tester_freshdesk_automatic_fields_test"

    def streams_to_test(self):
        streams_to_exclude = {
            "satisfaction_ratings",
            "time_entries",
            "email_mailboxes",
            "email_configs",
            "sla_policies",
            "scenario_automations",
            "contact_fields",
            "company_fields",
            "skills",
            "products"
            }
        return self.expected_stream_names().difference(streams_to_exclude)
