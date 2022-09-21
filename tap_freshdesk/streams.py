import copy
from datetime import datetime as dt
import singer
from singer.bookmarks import get_bookmark


LOGGER = singer.get_logger()
DEFAULT_PAGE_SIZE = 100
DATETIME_FMT = "%Y-%m-%dT%H:%M:%SZ"


def get_min_bookmark(stream, selected_streams, bookmark, start_date, state, bookmark_key, predefined_filter=None):
    """
    Get the minimum bookmark from the parent and its corresponding child bookmarks.
    """

    stream_obj = STREAMS[stream]()
    min_bookmark = bookmark
    if stream in selected_streams:
        # Get minimum of stream's bookmark(start date in case of no bookmark) and min_bookmark
        if predefined_filter:
            stream = stream + '_' + predefined_filter
        min_bookmark = min(min_bookmark, get_bookmark(state, stream, bookmark_key, start_date))

    # Iterate through all children and return minimum bookmark among all.
    for child in stream_obj.children:
        min_bookmark = min(min_bookmark, get_min_bookmark(
            child, selected_streams, bookmark, start_date, state, bookmark_key))

    return min_bookmark


def get_schema(catalog, stream_id):
    """
    Return the catalog of the specified stream.
    """
    stream_catalog = [cat for cat in catalog if cat['tap_stream_id'] == stream_id][0]
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
    replication_method = 'INCREMENTAL'
    replication_keys = ['updated_at']
    key_properties = ['id']
    endpoint = None
    filter_param = False
    children = []
    path = ''
    headers = {}
    params = {"per_page": DEFAULT_PAGE_SIZE, "page": 1}
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
        return base_url + '/api/v2/' + self.path.format(*args)

    def add_fields_at_1st_level(self, record):
        """Adding nested fields at first level."""

    def write_records(self, catalog, state, selected_streams, start_date, data, max_bookmark,
                      client, streams_to_sync, child_max_bookmarks, predefined_filter=None):
        """
        Transform the chunk of records according to the schema and write the records based on the bookmark.
        """
        stream_catalog = get_schema(catalog, self.tap_stream_id)
        stream_id = self.tap_stream_id

        # Append the predefined filter in case it's present
        if predefined_filter:
            stream_id = stream_id + '_' + predefined_filter
        bookmark = get_bookmark(state, stream_id, self.replication_keys[0], start_date)
        # The max bookmark so far for the child stream
        child_max_bookmark = None

        with singer.metrics.record_counter(self.tap_stream_id) as counter:
            with singer.Transformer() as transformer:
                extraction_time = singer.utils.now()
                stream_metadata = singer.metadata.to_map(stream_catalog['metadata'])
                for row in data:
                    self.add_fields_at_1st_level(row)
                    if self.tap_stream_id in selected_streams and row[self.replication_keys[0]] >= bookmark:
                        # Custom fields are expected to be strings, but sometimes the API sends
                        # booleans. We cast those to strings to match the schema.
                        if 'custom_fields' in row:
                            row['custom_fields'] = self.transform_dict(row['custom_fields'], force_str=self.force_str)

                        rec = transformer.transform(row, stream_catalog['schema'], stream_metadata)
                        singer.write_record(self.tap_stream_id, rec, time_extracted=extraction_time)
                        max_bookmark = max(max_bookmark, rec[self.replication_keys[0]])
                        counter.increment(1)

                    # Sync the child streams if they are selected
                    for child in self.children:
                        child_obj = STREAMS[child]()
                        if child in selected_streams:
                            child_obj.parent_id = row['id']
                            child_max_bookmark = get_bookmark(state, child_obj.tap_stream_id,
                                                              child_obj.replication_keys[0], start_date)
                            # Update the child's max_bookmark as the max of the already present max value and the return value
                            child_max_bookmark = max(child_max_bookmarks.get(child, child_max_bookmark), child_obj.sync_obj(
                                state, start_date, client, catalog, selected_streams, streams_to_sync))
                            child_max_bookmarks[child] = child_max_bookmark
        return max_bookmark, child_max_bookmarks

    def sync_obj(self, state, start_date, client, catalog, selected_streams, streams_to_sync, predefined_filter=None):
        """
        The base stream class sync_obj() function to fetch records.
        """
        params = {**self.params, "per_page": client.page_size}
        full_url = self.build_url(client.base_url, self.parent_id)

        # Update the filter keyword in the params for date-filtered streams
        if predefined_filter:
            LOGGER.info("Syncing %s with filter %s", self.tap_stream_id, predefined_filter)
            params[self.filter_keyword] = predefined_filter

        current_time = dt.strftime(dt.now(), DATETIME_FMT)
        # Get the minimum bookmark from the parent and the child streams
        min_bookmark = get_min_bookmark(self.tap_stream_id, selected_streams, current_time,
                                        start_date, state, self.replication_keys[0], predefined_filter)
        max_bookmark = min_bookmark
        # Initialize the child_max_bookmarks dictionary
        child_max_bookmarks = {}

        # Add the `updated_since` param if the date_filter attribute is True
        if self.date_filter:
            params[self.date_filter] = min_bookmark
        params['page'] = 1
        self.paginate = True

        LOGGER.info("Syncing %s from %s", self.tap_stream_id, min_bookmark)
        # Paginate through the request
        while self.paginate:
            data = client.request(full_url, params)
            self.paginate = len(data) >= client.page_size
            params['page'] += 1
            max_bookmark, child_max_bookmarks = self.write_records(
                catalog, state, selected_streams, start_date, data, max_bookmark, client, streams_to_sync,
                child_max_bookmarks, predefined_filter)
        write_bookmark(self.tap_stream_id, selected_streams, max_bookmark, state, predefined_filter)

        # Write the max_bookmark for the child streams in the state files if they are selected.
        for key, value in child_max_bookmarks.items():
            write_bookmark(key, selected_streams, value, state, None)
        return state


class Agents(Stream):
    """
    https://developer.freshdesk.com/api/#list_all_agents
    """
    tap_stream_id = 'agents'
    path = 'agents'


class Companies(Stream):
    """
    https://developer.freshdesk.com/api/#list_all_companies
    """
    tap_stream_id = 'companies'
    path = 'companies'


class Groups(Stream):
    """
    https://developer.freshdesk.com/api/#list_all_groups
    """
    tap_stream_id = 'groups'
    path = 'groups'


class Roles(Stream):
    """
    https://developer.freshdesk.com/api/#list_all_roles
    """
    tap_stream_id = 'roles'
    path = 'roles'


class DateFilteredStream(Stream):
    """
    Base class for all the streams that can be filtered by date.
    """

    def sync_obj(self, state, start_date, client, catalog, selected_streams, streams_to_sync, predefined_filter=None):
        """
        The overridden sync_obj() method to fetch the records with different filters.
        """
        dup_state = copy.deepcopy(state)
        max_child_bms = {}
        for each_filter in self.filters:
            # Update child bookmark to original_state
            for child in filter(lambda s: s in selected_streams, self.children):
                singer.write_bookmark(state, child, "updated_at", get_bookmark(
                    dup_state, child, "updated_at", start_date))

            super().sync_obj(state, start_date, client, catalog, selected_streams, streams_to_sync, each_filter)

            # Update the max child bookmarks dictionary with the maximum from the child and the existing bookmark
            max_child_bms.update({child: max(max_child_bms.get(child, ""), get_bookmark(
                state, child, "updated_at", start_date)) for child in self.children if child in selected_streams})

        # Write the child stream bookmarks with the max value found
        for child, bm in max_child_bms.items():
            singer.write_bookmark(state, child, "updated_at", bm)


class Tickets(DateFilteredStream):
    """
    https://developer.freshdesk.com/api/#list_all_tickets
    """
    tap_stream_id = 'tickets'
    path = 'tickets'
    children = ['conversations', 'satisfaction_ratings', 'time_entries']
    id_key = 'id'
    date_filter = 'updated_since'
    params = {
        "per_page": DEFAULT_PAGE_SIZE,
        'order_by': "updated_at",
        'order_type': "asc",
        'include': "requester,company,stats"
    }
    filter_keyword = 'filter'
    filters = [None, 'deleted', 'spam']


class Contacts(DateFilteredStream):
    """
    https://developer.freshdesk.com/api/#list_all_contacts
    """
    tap_stream_id = 'contacts'
    path = 'contacts'
    id_key = 'id'
    date_filter = '_updated_since'
    filter_keyword = 'state'
    filters = [None, 'deleted', 'blocked']


class ChildStream(Stream):
    """
    Base class for all the child streams.
    """

    def sync_obj(self, state, start_date, client, catalog, selected_streams, streams_to_sync, predefined_filter=None):
        """
        The child stream sync_obj() method to sync the child records
        """
        params = {**self.params, "per_page": client.page_size}
        # Build the url for the request
        full_url = self.build_url(client.base_url, self.parent_id)

        current_time = dt.strftime(dt.now(), DATETIME_FMT)
        # Get the min bookmark from the parent and the child streams
        min_bookmark = get_min_bookmark(self.tap_stream_id, selected_streams, current_time,
                                        start_date, state, self.replication_keys[0], None)
        max_bookmark = min_bookmark
        params['page'] = 1
        self.paginate = True

        LOGGER.info("Syncing %s from %s", self.tap_stream_id, min_bookmark)
        # Paginate through the records
        while self.paginate:
            data = client.request(full_url, params)
            self.paginate = len(data) >= client.page_size
            params['page'] += 1
            # Write the records based on the bookmark and return the max_bookmark for the page
            bookmark, _ = self.write_records(catalog, state, selected_streams, start_date,
                                             data, max_bookmark, client, streams_to_sync, None)
            max_bookmark = max(max_bookmark, bookmark)
        return max_bookmark


class Conversations(ChildStream):
    """
    https://developer.freshdesk.com/api/#list_all_ticket_notes
    """
    tap_stream_id = 'conversations'
    path = 'tickets/{}/conversations'
    parent = 'tickets'

    def add_fields_at_1st_level(self, record):
        """
        Overwrite updated__at value.
        """
        # For edited conversations `last_edited_at` value gets update instead of `updated_at`
        # Hence `updated_at` will be overwritten if `last_edited_at` > `updated_at`
        if record.get("last_edited_at"):
            record["updated_at"] = max(record["updated_at"], record["last_edited_at"])


class SatisfactionRatings(ChildStream):
    """
    https://developer.freshdesk.com/api/#view_ticket_satisfaction_ratings
    """
    tap_stream_id = 'satisfaction_ratings'
    path = 'tickets/{}/satisfaction_ratings'
    parent = 'tickets'


class TimeEntries(ChildStream):
    """
    https://developer.freshdesk.com/api/#list_all_ticket_timeentries
    """
    tap_stream_id = 'time_entries'
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
