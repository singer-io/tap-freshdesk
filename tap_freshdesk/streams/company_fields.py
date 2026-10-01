from singer import get_logger

from tap_freshdesk.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class CompanyFields(IncrementalStream):
    tap_stream_id = "company_fields"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    path = "company_fields"

    def get_url_endpoint(self, parent_obj=None):
        """Get the URL endpoint for the company_fields stream."""
        return f"{self.client.base_url}/{self.path}"
