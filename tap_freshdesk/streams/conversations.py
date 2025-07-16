from typing import Any, Dict

from singer import get_logger, write_bookmark

from tap_freshdesk.streams.tickets import Tickets

LOGGER = get_logger()


class Conversations(Tickets):
    tap_stream_id = "conversations"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    path = "tickets/{}/conversations"
    parent = "tickets"

    def get_url_endpoint(self, parent_obj=None):
        return f"{self.client.base_url}/{self.path.format(parent_obj['id'])}"

    def write_bookmark(self, state: dict, key: Any = None, value: Any = None) -> Dict:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        pass

    def get_bookmark(self, state: Dict, stream: str, key: Any = None) -> int:
        """Singleton bookmark value for child streams."""
        if not self.bookmark_value:
            # Set bookmark value as singleton
            self.bookmark_value = super().get_bookmark(state, stream)

        return self.bookmark_value
