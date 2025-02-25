from typing import Dict, Iterator, List

from singer import Transformer, get_logger, metrics, write_record
from singer.utils import strftime, strptime_to_utc

from tap_freshdesk.streams.abstracts import IncrementalStream

LOGGER = get_logger()

class Time_entries(IncrementalStream):
    tap_stream_id = 'time_entries'
    key_properties = ['id']
    replication_keys = ['updated_at']
    path = 'tickets/{ticket_id}/time_entries'
