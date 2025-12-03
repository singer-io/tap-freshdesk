from base import FreshdeskBaseTest
from tap_tester.base_suite_tests.start_date_test import StartDateTest


class FreshdeskStartDateTest(StartDateTest, FreshdeskBaseTest):
    """Instantiate start date according to the desired data set and run the
    test."""

    @staticmethod
    def name():
        return "tap_tester_freshdesk_start_date_test"

    def streams_to_test(self):
        streams_to_exclude = {
            "satisfaction_ratings",
            "time_entries",
            "agents",
            "groups",
            "roles"
        }
        return self.expected_stream_names().difference(streams_to_exclude)

    @property
    def start_date_1(self):
        return "2020-01-01T00:00:00Z"

    @property
    def start_date_2(self):
        return "2022-08-01T00:00:00Z"
