from typing import Dict, Iterator, List

from singer import Transformer, get_logger, metrics, write_record
from singer.utils import strftime, strptime_to_utc

from tap_freshdesk.streams.abstracts import IncrementalStream

LOGGER = get_logger()

class Tickets(IncrementalStream):
    tap_stream_id = 'tickets'
    key_properties = ['id']
    replication_keys = ['updated_at']
    path = 'tickets'

    def get_records(self, state: Dict) -> List:
        """Fetch tickets using updated_since from the get_bookmark function."""
        extraction_url = self.url_endpoint
        page_count = 1

        # Fetch the bookmark for incremental sync
        updated_since = self.get_bookmark(state)

        # Set initial parameters for API call
        self.params.update({
            "per_page": self.page_size,
            "page": page_count,
            "order_by": self.replication_keys[0],
            "order_type": "asc",
            "include": "requester,company,stats",
            "updated_since": updated_since
        })

        while True:
            LOGGER.info("Fetching Page %s for Tickets Stream", page_count)
            response = self.client.get(extraction_url, self.params, self.headers, self.path)
            raw_records = response

            if not raw_records:
                LOGGER.warning("No records found on Page %s", page_count)
                break

            yield from raw_records

            # Pagination: if results fill the page, fetch next
            if len(raw_records) == self.page_size:
                page_count += 1
                self.params["page"] = page_count
            else:
                break
