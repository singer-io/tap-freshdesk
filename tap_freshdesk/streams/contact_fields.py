from singer import get_logger

from tap_freshdesk.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class ContactFields(IncrementalStream):
    tap_stream_id = "contact_fields"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    path = "contact_fields"
