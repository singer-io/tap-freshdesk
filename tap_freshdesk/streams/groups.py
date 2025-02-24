from typing import Dict, Iterator, List

from singer import Transformer, get_logger, metrics, write_record
from singer.utils import strftime, strptime_to_utc

from tap_freshdesk.streams.abstracts import FullTableStream

LOGGER = get_logger()

class Groups(FullTableStream):
    tap_stream_id = 'groups'
    key_properties = ['id']
    path = 'groups'
