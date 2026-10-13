from singer import get_logger

from tap_freshdesk.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class EmailConfigs(IncrementalStream):
    tap_stream_id = "email_configs"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    path = "email_configs"
