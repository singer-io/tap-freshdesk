import os
import json
import singer
from typing import Dict, Tuple
from singer import metadata
from tap_freshdesk.streams import STREAMS

LOGGER = singer.get_logger()


def get_abs_path(path: str) -> str:
    """Get the absolute path for the schema files."""
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema_references() -> Dict:
    """Load the schema files from the schema folder and return the schema references."""
    shared_schema_path = get_abs_path("schemas/shared")

    shared_file_names = []
    if os.path.exists(shared_schema_path):
        shared_file_names = [
            f
            for f in os.listdir(shared_schema_path)
            if os.path.isfile(os.path.join(shared_schema_path, f))
        ]

    refs = {}
    for shared_schema_file in shared_file_names:
        with open(os.path.join(shared_schema_path, shared_schema_file)) as data_file:
            refs["shared/" + shared_schema_file] = json.load(data_file)

    return refs


def get_schemas() -> Tuple[Dict, Dict]:
    """Load the schema references, prepare metadata for each streams and return
    schema and metadata for the catalog."""
    schemas = {}
    field_metadata = {}

    refs = load_schema_references()
    for stream_name, stream_obj in STREAMS.items():
        schema_path = get_abs_path(f"schemas/{stream_name}.json")
        with open(schema_path) as file:
            schema = json.load(file)

        schemas[stream_name] = schema
        schema = singer.resolve_schema_references(schema, refs)

        mdata = metadata.new()
        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=getattr(stream_obj, "key_properties"),
            valid_replication_keys=(getattr(stream_obj, "replication_keys") or []),
            replication_method=getattr(stream_obj, "replication_method"),
        )
        mdata = metadata.to_map(mdata)

        automatic_keys = getattr(stream_obj, "replication_keys") or []
        for field_name in schema["properties"].keys():
            if field_name in automatic_keys:
                mdata = metadata.write(
                    mdata, ("properties", field_name), "inclusion", "automatic"
                )

        mdata = metadata.to_list(mdata)
        field_metadata[stream_name] = mdata

    return schemas, field_metadata

def write_schema(stream, client, streams_to_sync, catalog) -> None:
    """Collect nested child streams to sync and write schema for selected
    streams."""
    if stream.is_selected():
        stream.write_schema()

    for child in stream.children:
        child_obj = STREAMS[child](client, catalog.get_stream(child))
        write_schema(child_obj, client, streams_to_sync, catalog)
        if child in streams_to_sync:
            stream.child_to_sync.append(child_obj)
