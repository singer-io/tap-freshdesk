from singer import get_logger

from tap_freshdesk.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class Skills(IncrementalStream):
    tap_stream_id = "skills"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    path = "skills"

    def get_url_endpoint(self, parent_obj=None):
        """Get the URL endpoint for the skills stream."""
        return f"{self.client.base_url}/{self.path}"
