from singer import get_logger

from tap_freshdesk.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class Surveys(IncrementalStream):
    """Legacy Customer Satisfaction Survey stream.

    Ref: https://developers.freshdesk.com/api/#list_all_survey
    """

    tap_stream_id = "surveys"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    path = "surveys"

    def get_url_endpoint(self, parent_obj=None):
        """Get the URL endpoint for the surveys stream."""
        return f"{self.client.base_url}/{self.path}"
