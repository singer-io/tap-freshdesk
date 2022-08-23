from datetime import datetime
import singer
from singer import bookmarks


LOGGER = singer.get_logger()
PAGE_SIZE = 100


def get_bookmark(state, stream_name, bookmark_key, start_date):
    """
    Return bookmark value if available in the state otherwise return start date
    """
    return bookmarks.get_bookmark(state, stream_name, bookmark_key, start_date)

def get_min_bookmark(stream, selected_streams, bookmark, start_date, state, bookmark_key):
    """
    Get the minimum bookmark from the parent and its corresponding child bookmarks.
    """

    stream_obj = STREAMS[stream]()
    min_bookmark = bookmark
    if stream in selected_streams:
        min_bookmark = min(min_bookmark, get_bookmark(state, stream, bookmark_key, start_date))

    for child in filter(lambda x: x in selected_streams, stream_obj.children):
        min_bookmark = min(min_bookmark, get_min_bookmark(child, selected_streams, bookmark, start_date, state, bookmark_key))

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
    params = {"per_page": PAGE_SIZE, "page": 1}
    paginate = True
    parent = None
    data_key = None
    child_data_key = None
    records_count = {}
    force_str = False

    def add_fields_at_1st_level(self, record, additional_data={}):
        pass

    def transform_dict(self, d, key_key="name", value_key="value", force_str=False):
        # Custom fields are expected to be strings, but sometimes the API sends
        # booleans. We cast those to strings to match the schema.
        rtn = []
        for k, v in d.items():
            if force_str:
                v = str(v).lower()
            rtn.append({key_key: k, value_key: v})
        return rtn

    def build_url(self, base_url, *args):
        return base_url +  '/api/v2/'+ self.path.format(*args)

    def sync_obj(self, state, start_date, client, catalog, selected_streams, records_count):
        stream_catalog = get_schema(catalog, self.tap_stream_id)
        bookmark = get_bookmark(state, self.tap_stream_id, self.replication_keys[0], start_date)
        max_bookmark = bookmark
        full_url = self.build_url(client.base_url)

        LOGGER.info("Syncing {} from {}".format(self.tap_stream_id, bookmark))
        while self.paginate:
            with singer.metrics.record_counter(self.tap_stream_id) as counter: 
                with singer.Transformer() as transformer:
                    extraction_time = singer.utils.now()
                    stream_metadata = singer.metadata.to_map(stream_catalog['metadata'])
                    data = client.request(full_url, self.params)
                    self.paginate = len(data) >= PAGE_SIZE
                    self.params['page'] += 1
                    for row in data:
                        if row[self.replication_keys[0]] >= bookmark:
                            if 'custom_fields' in row:
                                row['custom_fields'] = self.transform_dict(row['custom_fields'], force_str=self.force_str)

                            rec = transformer.transform(row, stream_catalog['schema'], stream_metadata)
                            singer.write_record(self.tap_stream_id, rec, time_extracted=extraction_time)
                            max_bookmark = max(max_bookmark, rec[self.replication_keys[0]])
                            counter.increment(1)
                            singer.write_bookmark(state, self.tap_stream_id, self.replication_keys[0], max_bookmark)

        singer.write_state(state)


class Agents(Stream):
    tap_stream_id = 'agents'
    key_properties = 'id'
    replication_keys = ['updated_at']
    replication_method = 'INCREMENTAL'
    path = 'agents'

class Companies(Stream):
    tap_stream_id = 'companies'
    key_properties = 'id'
    replication_keys = ['updated_at']
    replication_method = 'INCREMENTAL'
    path = 'companies'

class Conversations(Stream):
    tap_stream_id = 'conversations'
    key_properties = 'id'
    replication_keys = ['updated_at']
    replication_method = 'INCREMENTAL'
    path = 'tickets/{}/conversations'

class Groups(Stream):
    tap_stream_id = 'groups'
    key_properties = 'id'
    replication_keys = ['updated_at']
    replication_method = 'INCREMENTAL'
    path = 'groups'

class Roles(Stream):
    tap_stream_id = 'roles'
    key_properties = 'id'
    replication_keys = ['updated_at']
    replication_method = 'INCREMENTAL'
    path = 'roles'

class SatisfactionRatings(Stream):
    tap_stream_id = 'satisfaction_ratings'
    key_properties = 'id'
    replication_keys = ['updated_at']
    replication_method = 'INCREMENTAL'
    path = 'tickets/{}/satisfaction_ratings'

class Tickets(Stream):
    tap_stream_id = 'tickets'
    key_properties = 'id'
    replication_keys = ['updated_at']
    replication_method = 'INCREMENTAL'
    path = 'tickets'

class TimeEntries(Stream):
    tap_stream_id = 'time_entries'
    key_properties = 'id'
    replication_keys = ['updated_at']
    replication_method = 'INCREMENTAL'
    path = 'tickets/{}/time_entries'

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
