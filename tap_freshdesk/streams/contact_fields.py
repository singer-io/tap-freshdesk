from singer import get_logger

from tap_freshdesk.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class ContactFields(IncrementalStream):
    tap_stream_id = "contact_fields"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    path = "contact_fields"

    def get_url_endpoint(self, parent_obj=None):
        """Get the URL endpoint for the contact_fields stream."""
        return f"{self.client.base_url}/{self.path}"
