import os
from tap_tester.base_suite_tests.base_case import BaseCase


class FreshdeskBaseTest(BaseCase):
    """Setup expectations for test sub classes.

    Metadata describing streams. A bunch of shared methods that are used
    in tap-tester tests. Shared tap-specific methods (as needed).
    """

    start_date = "2017-01-01T00:00:00Z"

    @staticmethod
    def tap_name():
        """The name of the tap."""
        return "tap-freshdesk"

    @staticmethod
    def get_type():
        """The name of the tap."""
        return "platform.freshdesk"

    @classmethod
    def expected_metadata(cls):
        """The expected streams and metadata about the streams."""
        return  {
            "agents": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.EXPECTED_PAGE_SIZE: 100,
                cls.API_LIMIT: 1     # MAZIMUM SMALL LIMIT FOR TESTING, ACTUALLY IT IS 100
            },
            "companies": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updated_at"},
                cls.EXPECTED_PAGE_SIZE: 100,
                cls.API_LIMIT: 100
            },
            "contacts": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updated_at"},
                cls.EXPECTED_PAGE_SIZE: 100,
                cls.API_LIMIT: 100
            },
            "conversations": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updated_at"},
                cls.EXPECTED_PAGE_SIZE: 100,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: "tickets"
            },
            "groups": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.EXPECTED_PAGE_SIZE: 100,
                cls.API_LIMIT: 100
            },
            "roles": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.EXPECTED_PAGE_SIZE: 100,
                cls.API_LIMIT: 2       # MAZIMUM SMALL LIMIT FOR TESTING, ACTUALLY IT IS 100
            },
            "satisfaction_ratings": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updated_at"},
                cls.EXPECTED_PAGE_SIZE: 100,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: "tickets"
            },
            "tickets": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updated_at"},
                cls.EXPECTED_PAGE_SIZE: 100,
                cls.API_LIMIT: 100
            },
            "time_entries": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updated_at"},
                cls.EXPECTED_PAGE_SIZE: 100,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: "tickets"
            },
        }

    @staticmethod
    def get_credentials():
        """Authentication information for the test account."""
        credentials_dict = {}
        creds = {
            "api_key": "TAP_FRESHDESK_API_KEY",
            "domain": "TAP_FRESHDESK_SUBDOMAIN"
        }

        for cred in creds:
            credentials_dict[cred] = os.getenv(creds[cred])

        return credentials_dict

    def get_properties(self, original: bool = True):
        """Configuration of properties required for the tap."""
        return_value = {
            "start_date": self.start_date,
            "user_agent": "Stitch Tap (+support@stitchdata.com)"
        }

        return return_value
