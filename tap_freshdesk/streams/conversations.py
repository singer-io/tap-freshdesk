from typing import Any, Dict

from singer import get_logger, write_bookmark

from tap_freshdesk.streams.abstracts import ChildBaseStream

LOGGER = get_logger()


class Conversations(ChildBaseStream):
    tap_stream_id = "conversations"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    path = "tickets/{}/conversations"
    parent = "tickets"

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """Modify the record before writing to the stream."""
        record["parent_id"] = parent_record["id"]
        record = super().modify_object(record, parent_record)
        return record
