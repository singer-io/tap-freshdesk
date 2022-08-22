from datetime import datetime
import singer
from singer import bookmarks


LOGGER = singer.get_logger()


def get_bookmark(state, stream_name, form_id, bookmark_key, start_date):
    """
    Return bookmark value if available in the state otherwise return start date
    """
    if form_id:
        return bookmarks.get_bookmark(state, stream_name, form_id, {}).get(bookmark_key, start_date)
    return bookmarks.get_bookmark(state, stream_name, bookmark_key, start_date)

def get_min_bookmark(stream, selected_streams, bookmark, start_date, state, form_id, bookmark_key):
    """
    Get the minimum bookmark from the parent and its corresponding child bookmarks.
    """

    stream_obj = STREAMS[stream]()
    min_bookmark = bookmark
    if stream in selected_streams:
        min_bookmark = min(min_bookmark, get_bookmark(state, stream, form_id, bookmark_key, start_date))

    for child in filter(lambda x: x in selected_streams, stream_obj.children):
        min_bookmark = min(min_bookmark, get_min_bookmark(child, selected_streams, bookmark, start_date, state, form_id, bookmark_key))

    return min_bookmark

def get_schema(catalog, stream_id):
    """
    Return catalog of the specified stream.
    """
    stream_catalog = [cat for cat in catalog if cat['tap_stream_id'] == stream_id ][0]
    return stream_catalog

def write_bookmarks(stream, selected_streams, bookmark_value, state):
    stream_obj = STREAMS[stream]()
    # If the stream is selected, write the bookmark.
    if stream in selected_streams:
        singer.write_bookmark(state, stream_obj.tap_stream_id, stream_obj.replication_keys[0], bookmark_value)

class Stream:
    """
    Base class representing tap-freshdesk streams.
    """
    tap_stream_id = None
    replication_method = None
    replication_keys = None
    key_properties = []
    endpoint = None
    filter_param = False
    children = []
    headers = {}
    params = {}
    parent = None
    data_key = None
    child_data_key = None
    records_count = {}

    def add_fields_at_1st_level(self, record, additional_data={}):
        pass

class Agents(Stream):
    tap_stream_id = 'agents'
    key_properties = 'id'
    replication_keys = 'updated_at'
    replication_method = 'INCREMENTAL'

class Companies(Stream):
    tap_stream_id = 'companies'
    key_properties = 'id'
    replication_keys = 'updated_at'
    replication_method = 'INCREMENTAL'

class Conversations(Stream):
    tap_stream_id = 'conversations'
    key_properties = 'id'
    replication_keys = 'updated_at'
    replication_method = 'INCREMENTAL'

class Groups(Stream):
    tap_stream_id = 'groups'
    key_properties = 'id'
    replication_keys = 'updated_at'
    replication_method = 'INCREMENTAL'

class Roles(Stream):
    tap_stream_id = 'roles'
    key_properties = 'id'
    replication_keys = 'updated_at'
    replication_method = 'INCREMENTAL'

class SatisfactionRatings(Stream):
    tap_stream_id = 'satisfaction_ratings'
    key_properties = 'id'
    replication_keys = 'updated_at'
    replication_method = 'INCREMENTAL'

class Tickets(Stream):
    tap_stream_id = 'tickets'
    key_properties = 'id'
    replication_keys = 'updated_at'
    replication_method = 'INCREMENTAL'

class TimeEntries(Stream):
    tap_stream_id = 'time_entries'
    key_properties = 'id'
    replication_keys = 'updated_at'
    replication_method = 'INCREMENTAL'

STREAMS = {
    "agents": Agents,
    "companies": Companies,
    "conversations": Conversations,
    "groups": Groups,
    "roles": Roles,
    "satisfaction_ratings": SatisfactionRatings,
    "tickets": Tickets,
    "time_entries": TimeEntries
}
