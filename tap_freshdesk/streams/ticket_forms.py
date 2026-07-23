from singer import get_logger

from tap_freshdesk.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class TicketForms(IncrementalStream):
    tap_stream_id = "ticket_forms"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    path = "ticket-forms"

    def get_url_endpoint(self, parent_obj=None):
        """Get the URL endpoint for the ticket_forms stream."""
        return f"{self.client.base_url}/{self.path}"
