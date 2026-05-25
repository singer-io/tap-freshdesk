import singer
from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_freshdesk.schema import get_schemas
from tap_freshdesk.streams import STREAMS
from tap_freshdesk.exceptions import freshdeskUnauthorizedError, freshdeskForbiddenError
from tap_freshdesk.utils import check_stream_access

LOGGER = singer.get_logger()


def _check_stream_access(client, stream_name, stream_class) -> bool:
    """
    Probes a stream endpoint with page_size=1 to verify the credentials have
    access. Child streams whose path contains '{}' (they require a parent ID)
    are skipped and assumed accessible.
    Returns True if accessible, False on 401/403.
    """
    if '{}' in stream_class.path:
        # Child streams cannot be probed without a parent ID — skip and include.
        return True

    endpoint = f"{client.base_url}/{stream_class.path}"
    return check_stream_access(
        stream_name,
        probe_fn=lambda: client.get(
            endpoint=endpoint,
            params={"per_page": 1, "page": 1},
            headers={"Accept": "application/json"},
        ),
        auth_error_types=(freshdeskUnauthorizedError, freshdeskForbiddenError),
    )


def discover(client) -> Catalog:
    """Run the discovery mode, prepare the catalog file and return the catalog.
    Probes each top-level stream endpoint to verify access; streams that return
    401/403 are excluded from the catalog.
    """
    schemas, field_metadata = get_schemas()
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        if not _check_stream_access(client, stream_name, STREAMS[stream_name]):
            continue

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
