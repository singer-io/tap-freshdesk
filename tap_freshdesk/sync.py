import singer
from typing import Dict
from tap_freshdesk.streams import STREAMS
from tap_freshdesk.client import Client

LOGGER = singer.get_logger()


def update_currently_syncing(state: Dict, stream_name: str) -> None:
    if not stream_name and singer.get_currently_syncing(state):
        del state["currently_syncing"]
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


def collect_child_to_sync(stream, client, selected_streams, catalog) -> None:
    """Collect nested child streams to sync"""
    for child in stream.children:
        if child in selected_streams:
            child_stream_catalog = catalog.get_stream(child)
            child_schema = child_stream_catalog.schema.to_dict()
            child_metadata = singer.metadata.to_map(child_stream_catalog.metadata)

            child_obj = STREAMS[child](client, child_schema, child_metadata)
            child_obj.write_schema()
            stream.child_to_sync.append(child_obj)

            collect_child_to_sync(child_obj, client, selected_streams, catalog)


def sync(client: Client, config: Dict, catalog: singer.Catalog, state) -> None:
    """Sync selected streams from catalog"""

    selected_streams = []
    for stream in catalog.get_selected_streams(state):
        selected_streams.append(stream.stream)
    LOGGER.info("selected_streams: {}".format(selected_streams))

    last_stream = singer.get_currently_syncing(state)
    LOGGER.info("last/currently syncing stream: {}".format(last_stream))

    with singer.Transformer() as transformer:
        for stream_name in selected_streams:

            stream_catalog = catalog.get_stream(stream_name)
            stream_schema = stream_catalog.schema.to_dict()
            stream_metadata = singer.metadata.to_map(stream_catalog.metadata)
            stream = STREAMS[stream_name](client, stream_schema, stream_metadata)
            if stream.parent:
                # Skip child stream if parent stream is not selected
                continue

            stream.write_schema()
            collect_child_to_sync(stream, client, selected_streams, catalog)

            LOGGER.info("START Syncing: {}".format(stream_name))
            update_currently_syncing(state, stream_name)
            total_records = stream.sync(state=state, transformer=transformer)

            update_currently_syncing(state, None)
            LOGGER.info(
                "FINISHED Syncing: {}, total_records: {}".format(
                    stream_name, total_records
                )
            )
