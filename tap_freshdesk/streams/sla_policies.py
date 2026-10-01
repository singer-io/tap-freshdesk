from singer import get_logger

from tap_freshdesk.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class SlaPolicies(IncrementalStream):
    tap_stream_id = "sla_policies"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    path = "sla_policies"

    def get_url_endpoint(self, parent_obj=None):
        """Get the URL endpoint for the sla_policies stream."""
        return f"{self.client.base_url}/{self.path}"
