from tap_tester.base_suite_tests.pagination_test import PaginationTest
from base import FreshdeskBaseTest


class FreshdeskPaginationTest(PaginationTest, FreshdeskBaseTest):
    """Ensure tap can replicate multiple pages of data for streams that use
    pagination."""

    @staticmethod
    def name():
        return "tap_tester_freshdesk_pagination_test"

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
