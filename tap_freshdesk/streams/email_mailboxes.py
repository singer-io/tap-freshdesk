from singer import get_logger

from tap_freshdesk.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class EmailMailboxes(IncrementalStream):
    tap_stream_id = "email_mailboxes"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    path = "email/mailboxes"
