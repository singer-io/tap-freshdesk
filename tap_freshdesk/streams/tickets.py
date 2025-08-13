from typing import Any, Dict

from singer import (
    Transformer,
    get_logger,
    metrics,
    write_record,
    write_bookmark,
)

from tap_freshdesk.streams.abstracts import ParentBaseStream

LOGGER = get_logger()


class Tickets(ParentBaseStream):
    tap_stream_id = "tickets"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    children = ["conversations", "satisfaction_ratings", "time_entries"]
    path = "tickets"
