#!/usr/bin/env python3
import singer
from singer import utils
from tap_freshdesk.discover import discover as _discover
from tap_freshdesk.sync import sync as _sync
from tap_freshdesk.client import FreshdeskClient

REQUIRED_CONFIG_KEYS = ["start_date", "domain", "api_key"]

LOGGER = singer.get_logger()


@utils.handle_top_exception(LOGGER)
def main():
    """
    Run discover mode or sync mode.
    """
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config
    with FreshdeskClient(config) as client:
        if args.discover:
            catalog = _discover()
            catalog.dump()
        else:
            catalog = args.catalog \
                if args.catalog else _discover()
            _sync(client, config, args.state, catalog.to_dict())


if __name__ == "__main__":
    main()
