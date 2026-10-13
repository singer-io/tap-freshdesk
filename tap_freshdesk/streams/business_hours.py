from singer import get_logger

from tap_freshdesk.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class BusinessHours(IncrementalStream):
    tap_stream_id = "business_hours"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    path = "business_hours"
