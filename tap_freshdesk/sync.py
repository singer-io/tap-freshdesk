import singer
from tap_freshdesk.streams import STREAMS

LOGGER = singer.get_logger()


def write_schemas(stream_id, catalog, selected_streams):
    """
    Write the schemas for each stream.
    """
    stream_obj = STREAMS[stream_id]()

    if stream_id in selected_streams:
        # Get catalog object for a particular stream.
        stream = [cat for cat in catalog['streams'] if cat['tap_stream_id'] == stream_id][0]
        singer.write_schema(stream_id, stream['schema'], stream['key_properties'])

    for child in stream_obj.children:
        write_schemas(child, catalog, selected_streams)


def get_selected_streams(catalog):
    '''
    Gets selected streams.  Checks schema's 'selected'
    first -- and then checks metadata, looking for an empty
    breadcrumb and mdata with a 'selected' entry
    '''
    selected_streams = []
    for stream in catalog['streams']:
        stream_metadata = stream['metadata']
        for entry in stream_metadata:
            # Stream metadata will have an empty breadcrumb
            if not entry['breadcrumb'] and entry['metadata'].get('selected', None):
                selected_streams.append(stream['tap_stream_id'])
    return selected_streams


def update_currently_syncing(state, stream_name):
    """
    Updates currently syncing stream in the state.
    """
    if not stream_name and singer.get_currently_syncing(state):
        del state['currently_syncing']
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


def get_ordered_stream_list(currently_syncing, streams_to_sync):
    """
    Get an ordered list of remaining streams to sync other streams followed by synced streams.
    """
    stream_list = list(sorted(streams_to_sync))
    if currently_syncing in stream_list:
        index = stream_list.index(currently_syncing)
        stream_list = stream_list[index:] + stream_list[:index]
    return stream_list


def get_stream_to_sync(selected_streams):
    """
    Get the streams for which the sync function should be called
    (the parent in case of selected child streams).
    """
    streams_to_sync = []

    # Loop thru all selected streams
    for stream_name in selected_streams:
        stream_obj = STREAMS[stream_name]
        # If the stream has a parent_stream, then it is a child stream
        parent_stream = hasattr(stream_obj, 'parent') and stream_obj.parent

        # Append selected parent streams
        if not parent_stream:
            streams_to_sync.append(stream_name)
        else:
            # Append un-selected parent streams of selected children
            if parent_stream not in selected_streams and parent_stream not in streams_to_sync:
                streams_to_sync.append(parent_stream)
    return streams_to_sync


def sync(client, config, state, catalog):
    """
    Sync selected streams.
    """

    # Get selected streams, make sure stream dependencies are met
    selected_streams = get_selected_streams(catalog)
    streams_to_sync = get_stream_to_sync(selected_streams)
    LOGGER.info("Selected Streams: %s", selected_streams)
    LOGGER.info("Syncing Streams: %s", streams_to_sync)

    singer.write_state(state)
    currently_syncing = singer.get_currently_syncing(state)
    streams_to_sync = get_ordered_stream_list(currently_syncing, streams_to_sync)
    for stream in streams_to_sync:
        stream_obj = STREAMS[stream]()

        write_schemas(stream, catalog, selected_streams)
        update_currently_syncing(state, stream)

        stream_obj.sync_obj(state, config["start_date"], client, catalog['streams'],
                            selected_streams, streams_to_sync)
        singer.write_state(state)
        update_currently_syncing(state, None)
