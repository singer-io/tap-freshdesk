from singer import get_logger

from tap_freshdesk.streams.abstracts import FullTableStream

LOGGER = get_logger()

class Roles(FullTableStream):
    tap_stream_id = "roles"
    key_properties = ["id"]
    path = "roles"
