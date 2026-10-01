from singer import get_logger

from tap_freshdesk.streams.abstracts import FullTableStream

LOGGER = get_logger()


class Account(FullTableStream):
    tap_stream_id = "account"
    # The account endpoint returns a single object, not a list.
    # account_id is used as the primary key because there is no top-level id.
    key_properties = ["account_id"]
    path = "account"

    def get_url_endpoint(self, parent_obj=None):
        """Get the URL endpoint for the account stream."""
        return f"{self.client.base_url}/{self.path}"

    def get_records(self):
        """Override to handle the singleton account response.

        The /account endpoint returns a single JSON object rather than
        an array, so we wrap the response in a list before yielding.
        """
        self.url_endpoint = self.get_url_endpoint()
        record = self.client.get(
            self.url_endpoint, self.params, self.headers, self.path
        )
        if record:
            yield record
