from singer import get_logger

from tap_freshdesk.streams.abstracts import FullTableStream

LOGGER = get_logger()


class Roles(FullTableStream):
    tap_stream_id = "roles"
    key_properties = ["id"]
    path = "roles"

    def get_url_endpoint(self, parent_obj=None):
        """Get the URL endpoint for the roles stream."""
        return f"{self.client.base_url}/{self.path}"
