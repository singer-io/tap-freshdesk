from typing import Dict, Iterator, List

from singer import Transformer, get_logger, metrics, write_record
from singer.utils import strftime, strptime_to_utc

from tap_freshdesk.streams.abstracts import IncrementalStream

LOGGER = get_logger()

class Conversations(IncrementalStream):
    tap_stream_id = 'conversations'
    key_properties = ['id']
    replication_keys = ['created_at']
    path = 'tickets/{ticket_id}/conversations'

