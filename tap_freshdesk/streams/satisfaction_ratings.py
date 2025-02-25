from typing import Any, Dict

from singer import get_logger

from tap_freshdesk.streams.abstracts import IncrementalStream

LOGGER = get_logger()

class Satisfaction_ratings(IncrementalStream):
    tap_stream_id = "satisfaction_ratings"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    path = "tickets/{}/satisfaction_ratings"
    parent = "tickets"

    def get_url_endpoint(self, parent_obj=None):
        return f"{self.client.base_url}/{self.path.format(parent_obj['id'])}"

    def write_bookmark(self, state: dict, key: Any = None, value: Any = None) -> Dict:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        pass
