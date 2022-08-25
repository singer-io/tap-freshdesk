import copy
from datetime import datetime as dt
import singer
from singer.bookmarks import get_bookmark


LOGGER = singer.get_logger()
PAGE_SIZE = 100
DATETIME_FMT = "%Y-%m-%dT%H:%M:%SZ"


def get_min_bookmark(stream, streams_to_sync, start_date, state, bookmark_key, predefined_filter=None):
    """
    Get the minimum bookmark from the parent and its corresponding child bookmarks.
    """

    stream_obj = STREAMS[stream]()
    min_bookmark =  dt.strftime(dt.now(), DATETIME_FMT)
    if stream in streams_to_sync:
        if predefined_filter:
            stream = stream + '_' + predefined_filter
        min_bookmark = min(min_bookmark, get_bookmark(state, stream, bookmark_key, start_date))

    for child in filter(lambda x: x in streams_to_sync, stream_obj.children):
        min_bookmark = min(min_bookmark, get_min_bookmark(child, streams_to_sync, start_date, state, bookmark_key))

    return min_bookmark

def get_schema(catalog, stream_id):
    """
    Return the catalog of the specified stream.
    """
    stream_catalog = [cat for cat in catalog if cat['tap_stream_id'] == stream_id ][0]
    return stream_catalog

def write_bookmark(stream, selected_streams, bookmark_value, state, predefined_filter=None):
    """
    Write the bookmark in case the stream is selected.
    """
    stream_obj = STREAMS[stream]()
    stream_id = stream_obj.tap_stream_id
    if stream in selected_streams:
        if predefined_filter:
            stream_id = stream_id + '_' + predefined_filter
        singer.write_bookmark(state, stream_id, stream_obj.replication_keys[0], bookmark_value)

class Stream:
    """
    Base class representing tap-freshdesk streams.
    """
    tap_stream_id = None
    replication_method = None
    replication_keys = []
    key_properties = []
    endpoint = None
    filter_param = False
    children = []
    path = ''
    headers = {}
    params = {"per_page": PAGE_SIZE, "page": 1}
    paginate = True
    parent = None
    id_key = None
    records_count = {}
    force_str = False
    date_filter = ''
    parent_id = None
    filters = []
    filter_keyword = ''

    def transform_dict(self, d, key_key="name", value_key="value", force_str=False):
        """
        Custom fields are expected to be strings, but sometimes the API sends
        booleans. We cast those to strings to match the schema.
        """
        rtn = []
        for k, v in d.items():
            if force_str:
                v = str(v).lower()
            rtn.append({key_key: k, value_key: v})
        return rtn

    def build_url(self, base_url, *args):
        """
        Build the full url with parameters and attributes.
        """
        return base_url +  '/api/v2/'+ self.path.format(*args)

    def write_records(self, catalog, state, selected_streams, start_date, data, max_bookmark, client, streams_to_sync, predefined_filter=None):
        """
        Transform the chunk of records according to the schema and write the records based on the bookmark.
        """
        stream_catalog = get_schema(catalog, self.tap_stream_id)
        stream_id = self.tap_stream_id

        # Append the predefined filter in case it's present
        if predefined_filter:
            self.params[self.filter_keyword] = predefined_filter
            stream_id = stream_id + '_' + predefined_filter
        bookmark = get_bookmark(state, stream_id, self.replication_keys[0], start_date)
        # The max bookmark so far for the child stream
        child_max_bookmark = None
        child_max_bookmarks = {}

        with singer.metrics.record_counter(self.tap_stream_id) as counter:
            with singer.Transformer() as transformer:
                extraction_time = singer.utils.now()
                stream_metadata = singer.metadata.to_map(stream_catalog['metadata'])
                for row in data:
                    if self.tap_stream_id in selected_streams and row[self.replication_keys[0]] >= bookmark:
                        if 'custom_fields' in row:
                            row['custom_fields'] = self.transform_dict(row['custom_fields'], force_str=self.force_str)

                        rec = transformer.transform(row, stream_catalog['schema'], stream_metadata)
                        singer.write_record(self.tap_stream_id, rec, time_extracted=extraction_time)
                        max_bookmark = max(max_bookmark, rec[self.replication_keys[0]])
                        counter.increment(1)

                    # Sync the child streams if they are selected
                    for child in self.children:
                        child_obj = STREAMS[child]()
                        child_max_bookmark = get_bookmark(state, child_obj.tap_stream_id, child_obj.replication_keys[0], start_date)
                        if child in selected_streams:
                            child_obj.parent_id = row['id']
                            # Update the child's max_bookmark as the max of the two
                            child_max_bookmark = max(child_max_bookmark, child_obj.sync_obj(state, start_date, client, catalog, selected_streams, streams_to_sync))
                            child_max_bookmarks[child] = child_max_bookmark
        return max_bookmark, child_max_bookmarks

    def sync_obj(self, state, start_date, client, catalog, selected_streams, streams_to_sync, predefined_filter=None):
        full_url = self.build_url(client.base_url, self.parent_id)
        if predefined_filter:
            LOGGER.info("Syncing tickets with filter %s", predefined_filter)
            self.params[self.filter_keyword] = predefined_filter
        min_bookmark = get_min_bookmark(self.tap_stream_id, streams_to_sync, start_date, state, self.replication_keys[0], predefined_filter)
        max_bookmark = min_bookmark

        # Add the `updated_since` param if the date_filter attribute is True
        if self.date_filter:
            self.params[self.date_filter] = min_bookmark
        self.params['page'] = 1
        self.paginate = True

        LOGGER.info("Syncing %s from %s", self.tap_stream_id, min_bookmark)
        # Paginate through the request
        while self.paginate:
            data = client.request(full_url, self.params)
            self.paginate = len(data) >= PAGE_SIZE
            self.params['page'] += 1
            max_bookmark, child_max_bookmarks = self.write_records(catalog, state, selected_streams, start_date, data, max_bookmark, client, streams_to_sync, predefined_filter)
        write_bookmark(self.tap_stream_id, selected_streams, max_bookmark, state, predefined_filter)

        # Write the max_bookmark for the child streams in the state files if they are selected.
        for key, value in child_max_bookmarks.items():
            write_bookmark(key, selected_streams, value, state, None)
        return state


class Agents(Stream):
    tap_stream_id = 'agents'
    key_properties = ['id']
    replication_keys = ['updated_at']
    replication_method = 'INCREMENTAL'
    path = 'agents'

class Companies(Stream):
    tap_stream_id = 'companies'
    key_properties = ['id']
    replication_keys = ['updated_at']
    replication_method = 'INCREMENTAL'
    path = 'companies'

class Groups(Stream):
    tap_stream_id = 'groups'
    key_properties = ['id']
    replication_keys = ['updated_at']
    replication_method = 'INCREMENTAL'
    path = 'groups'

class Roles(Stream):
    tap_stream_id = 'roles'
    key_properties = ['id']
    replication_keys = ['updated_at']
    replication_method = 'INCREMENTAL'
    path = 'roles'

class DateFilteredStream(Stream):
    def sync_obj(self, state, start_date, client, catalog, selected_streams, streams_to_sync, predefined_filter=None):
        """
        The overridden sync_obj() method to fetch the ticket records with different filters.
        """
        dup_state = copy.deepcopy(state)
        max_child_bms = {}
        for each_filter in self.filters:
            # Update child bookmark to original_state
            for child in filter(lambda s: s in selected_streams, self.children):
                singer.write_bookmark(state, child, "updated_at", get_bookmark(dup_state, child, "updated_at", start_date))

            super().sync_obj(state, start_date, client, catalog, selected_streams, streams_to_sync, each_filter)

            max_child_bms.update({child: max(max_child_bms.get(child, ""), get_bookmark(state, child, "updated_at", start_date))
                                  for child in self.children
                                  if child in selected_streams})

        for child, bm in max_child_bms.items():
            singer.write_bookmark(state, child, "updated_at", bm)
        return state

class Tickets(DateFilteredStream):
    tap_stream_id = 'tickets'
    key_properties = ['id']
    replication_keys = ['updated_at']
    replication_method = 'INCREMENTAL'
    path = 'tickets'
    children = ['conversations', 'satisfaction_ratings', 'time_entries']
    id_key = 'id'
    date_filter = 'updated_since'
    params = {
        "per_page": PAGE_SIZE,
        'order_by': replication_keys[0],
        'order_type': "asc",
        'include': "requester,company,stats"
    }
    filter_keyword = 'filter'
    filters = [None, 'deleted', 'spam']

class Contacts(DateFilteredStream):
    tap_stream_id = 'contacts'
    key_properties = ['id']
    replication_keys = ['updated_at']
    replication_method = 'INCREMENTAL'
    path = 'contacts'
    id_key = 'id'
    date_filter = '_updated_since'
    filter_keyword = 'state'
    filters = [None, 'deleted', 'blocked']


class ChildStream(Stream):

    def sync_obj(self, state, start_date, client, catalog, selected_streams, streams_to_sync, predefined_filter=None):
        """
        The child stream sync_obj() method to sync the child records
        """
        full_url = self.build_url(client.base_url, self.parent_id)
        min_bookmark = get_min_bookmark(self.tap_stream_id, streams_to_sync, start_date, state, self.replication_keys[0], None)
        max_bookmark = min_bookmark
        self.params['page'] = 1
        self.paginate = True

        LOGGER.info("Syncing %s from %s", self.tap_stream_id, min_bookmark)
        # Paginate through the records
        while self.paginate:
            data = client.request(full_url, self.params)
            self.paginate = len(data) >= PAGE_SIZE
            self.params['page'] += 1
            bookmark, _ = self.write_records(catalog, state, selected_streams, start_date, data, max_bookmark, client, streams_to_sync, None)
            max_bookmark = max(max_bookmark, bookmark)
        return max_bookmark

class Conversations(ChildStream):
    tap_stream_id = 'conversations'
    key_properties = ['id']
    replication_keys = ['updated_at']
    replication_method = 'INCREMENTAL'
    path = 'tickets/{}/conversations'
    parent = 'tickets'


class SatisfactionRatings(ChildStream):
    tap_stream_id = 'satisfaction_ratings'
    key_properties = ['id']
    replication_keys = ['updated_at']
    replication_method = 'INCREMENTAL'
    path = 'tickets/{}/satisfaction_ratings'
    parent = 'tickets'

class TimeEntries(ChildStream):
    tap_stream_id = 'time_entries'
    key_properties = ['id']
    replication_keys = ['updated_at']
    replication_method = 'INCREMENTAL'
    path = 'tickets/{}/time_entries'
    parent = 'tickets'


STREAMS = {
    "agents": Agents,
    "companies": Companies,
    "contacts": Contacts,
    "conversations": Conversations,
    "groups": Groups,
    "roles": Roles,
    "satisfaction_ratings": SatisfactionRatings,
    "tickets": Tickets,
    "time_entries": TimeEntries
}
