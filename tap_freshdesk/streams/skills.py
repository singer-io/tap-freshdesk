from singer import get_logger

from tap_freshdesk.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class Skills(IncrementalStream):
    tap_stream_id = "skills"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    path = "admin/skills"
