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

