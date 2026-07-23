from singer import get_logger

from tap_freshdesk.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class Products(IncrementalStream):
    tap_stream_id = "products"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    path = "products"

    def get_url_endpoint(self, parent_obj=None):
        """Get the URL endpoint for the products stream."""
        return f"{self.client.base_url}/{self.path}"
