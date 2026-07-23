from singer import get_logger

from tap_freshdesk.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class EmailConfigs(IncrementalStream):
    tap_stream_id = "email_configs"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    path = "email_configs"

    def get_url_endpoint(self, parent_obj=None):
        """Get the URL endpoint for the email_configs stream."""
        return f"{self.client.base_url}/{self.path}"
