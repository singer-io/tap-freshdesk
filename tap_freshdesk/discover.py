import singer
from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_freshdesk.schema import get_schemas
from tap_freshdesk.streams import STREAMS
from tap_freshdesk.exceptions import freshdeskNoAccessibleStreamsError

LOGGER = singer.get_logger()


def check_stream_access(client, stream_class) -> bool:
    """Verify access for a stream by probing its endpoint."""
    stream_obj = stream_class(client=client)
    return stream_obj.check_access()


def _prune_inaccessible_children(schemas: dict, field_metadata: dict) -> list:
    """Remove child streams whose parent stream is unavailable."""
    inaccessible_children = []
    removed_child = True
    while removed_child:
        removed_child = False
        for stream_name, stream_obj in list(STREAMS.items()):
            if (
                stream_name in schemas
                and stream_obj.parent
                and stream_obj.parent not in schemas
            ):
                LOGGER.warning(
                    "Stream '%s' excluded from catalog because its parent stream '%s' is not accessible.",
                    stream_name,
                    stream_obj.parent,
                )
                schemas.pop(stream_name, None)
                field_metadata.pop(stream_name, None)
                inaccessible_children.append(stream_name)
                removed_child = True
    return inaccessible_children


def _apply_access_checks(client, schemas: dict, field_metadata: dict) -> None:
    """Probe stream access and remove inaccessible streams in place."""
    inaccessible_streams = [
        stream_name
        for stream_name in list(schemas.keys())
        if not check_stream_access(client, STREAMS[stream_name])
    ]

    for stream_name in inaccessible_streams:
        schemas.pop(stream_name, None)
        field_metadata.pop(stream_name, None)

    inaccessible_children = _prune_inaccessible_children(schemas, field_metadata)

    if not schemas:
        raise freshdeskNoAccessibleStreamsError(
            "The credentials do not have read access to any of the supported streams."
        )

    all_inaccessible = inaccessible_streams + [
        stream_name for stream_name in inaccessible_children if stream_name not in inaccessible_streams
    ]

    if all_inaccessible:
        LOGGER.warning(
            "Unauthorized streams excluded from catalog: %s",
            ", ".join(all_inaccessible),
        )


def discover(client) -> Catalog:
    """Run the discovery mode, prepare the catalog file and return the catalog.
    Probes each stream endpoint to verify access; inaccessible streams are
    excluded from the returned catalog.
    """
    client.check_api_credentials()
    schemas, field_metadata = get_schemas()
    _apply_access_checks(client, schemas, field_metadata)
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        try:
            schema = Schema.from_dict(schema_dict)
            mdata = field_metadata[stream_name]
        except Exception as err:
            LOGGER.error(err)
            LOGGER.error("stream_name: {}".format(stream_name))
            LOGGER.error("type schema_dict: {}".format(type(schema_dict)))
            raise err

        key_properties = metadata.to_map(mdata).get((), {}).get("table-key-properties")

        catalog.streams.append(
            CatalogEntry(
                stream=stream_name,
                tap_stream_id=stream_name,
                key_properties=key_properties,
                schema=schema,
                metadata=mdata,
            )
        )

    return catalog
