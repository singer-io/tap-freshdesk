import singer
from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_freshdesk.schema import get_schemas
from tap_freshdesk.streams import STREAMS
from tap_freshdesk.exceptions import freshdeskUnauthorizedError, freshdeskForbiddenError, freshdeskNoAccessibleStreamsError

LOGGER = singer.get_logger()


def check_stream_access(stream_name, probe_fn, auth_error_types, fallback_accessible=False):
    """
    Probe a stream endpoint and return True if accessible, False on auth error.

    :param stream_name: Used in log messages.
    :param probe_fn: Zero-argument callable that performs the API probe.
    :param auth_error_types: Exception type(s) indicating 401/403 — returns False.
    :param fallback_accessible: If True, non-auth errors (e.g. 400 from minimal
                                probe params) are treated as auth-OK and return True.
                                If False (default), they are re-raised.
    """
    try:
        probe_fn()
        LOGGER.info("Stream '%s' is accessible.", stream_name)
        return True
    except auth_error_types:
        LOGGER.warning(
            "Stream '%s' is not accessible with the provided credentials.",
            stream_name,
        )
        return False
    except Exception:  # pylint: disable=broad-except
        if fallback_accessible:
            LOGGER.info("Stream '%s' endpoint reachable (auth OK).", stream_name)
            return True
        raise


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
            LOGGER.warning(
                "Stream '%s' will be excluded from the catalog due to insufficient permissions.",
                stream_name,
            )
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

    if not catalog.streams:
        raise freshdeskNoAccessibleStreamsError(
            "No stream endpoints are accessible with the provided credentials. "
            "Verify that the API key has the required permissions."
        )

    return catalog
