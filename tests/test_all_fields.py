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
            "email"
        ],
        "ticket_fields": [
            "field_update_in_progress"
        ],
        "email_mailboxes": [
            "mailbox_type",
            "created_at",
            "group_id",
            "default_reply_email",
            "name",
            "id",
            "freshdesk_mailbox",
            "active",
            "updated_at",
            "support_email",
            "product_id",
            "custom_mailbox"
        ],
        "email_configs": [
            "created_at",
            "group_id",
            "to_email",
            "id",
            "name",
            "primary_role",
            "active",
            "updated_at",
            "reply_email",
            "product_id"
        ]
    }

    start_date = "2020-01-01T00:00:00Z"

    @staticmethod
    def name():
        return "tap_tester_freshdesk_all_fields_test"

    def streams_to_test(self):
        streams_to_exclude = {
            "satisfaction_ratings",
            "time_entries",
            "email_mailboxes",
            "email_configs",
            "sla_policies",
            "scenario_automations",
            "contact_fields",
            "company_fields"
            }
        return self.expected_stream_names().difference(streams_to_exclude)
