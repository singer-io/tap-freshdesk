from typing import Any, Dict

from singer import get_logger

from tap_freshdesk.streams.abstracts import ChildBaseStream

LOGGER = get_logger()


class SatisfactionRatings(ChildBaseStream):
    tap_stream_id = "satisfaction_ratings"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    path = "tickets/{}/satisfaction_ratings"
    parent = "tickets"

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """Modify the record before writing to the stream."""
        record["parent_id"] = parent_record["id"]
        record = super().modify_object(record, parent_record)
        return record
