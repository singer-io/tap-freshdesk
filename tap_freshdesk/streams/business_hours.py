from singer import get_logger

from tap_freshdesk.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class BusinessHours(IncrementalStream):
    tap_stream_id = "business_hours"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    path = "business_hours"

    def get_url_endpoint(self, parent_obj=None):
        """Get the URL endpoint for the business_hours stream."""
        return f"{self.client.base_url}/{self.path}"
