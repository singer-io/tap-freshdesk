from typing import Dict, Iterator, List

from singer import Transformer, get_logger, metrics, write_record
from singer.utils import strftime, strptime_to_utc

from tap_freshdesk.streams.abstracts import FullTableStream

LOGGER = get_logger()


class Agents(FullTableStream):
    tap_stream_id = "agents"
    key_properties = ["id"]
    path = "agents"

    def get_url_endpoint(self, parent_obj=None):
        """Get the URL endpoint for the agents stream."""
        return f"{self.client.base_url}/{self.path}"
