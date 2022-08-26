import os
import json
from singer import metadata
import singer
from tap_freshdesk.streams import STREAMS

def get_abs_path(path):
    """
    Get the absolute path for the schema files.
    """
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def get_schemas():
    """
    Load the schema references, prepare metadata for each stream and return schema and metadata for the catalog.
    """
    schemas = {}
    field_metadata = {}

    refs = {}
    for stream_name, stream_metadata in STREAMS.items():
        schema_path = get_abs_path('schemas/{}.json'.format(stream_name))

        with open(schema_path) as file: # pylint: disable=unspecified-encoding
            schema = json.load(file)

        schemas[stream_name] = schema
        schema = singer.resolve_schema_references(schema, refs)

        mdata = metadata.new()
        mdata = metadata.get_standard_metadata(
                schema=schema,
                key_properties = (hasattr(stream_metadata, 'key_properties') or None) and stream_metadata.key_properties,
                valid_replication_keys = (hasattr(stream_metadata, 'replication_keys') or None) and stream_metadata.replication_keys,
                replication_method = (hasattr(stream_metadata, 'replication_method') or None) and stream_metadata.replication_method
            )
        mdata = metadata.to_map(mdata)

        # Loop through all keys and make replication keys of automatic inclusion
        for field_name in schema['properties'].keys():

            replication_keys = (hasattr(stream_metadata, 'replication_keys') or None) and stream_metadata.replication_keys
            if replication_keys and field_name in replication_keys:
                mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')

        mdata = metadata.to_list(mdata)
        field_metadata[stream_name] = mdata

    return schemas, field_metadata
