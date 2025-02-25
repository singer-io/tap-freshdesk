import sys
import json
import singer
from tap_freshdesk.client import Client
from tap_freshdesk.discover import discover
from tap_freshdesk.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["api_key", "domain", "start_date", "user_agent"]

def do_discover():

    LOGGER.info("Starting discover")
    catalog = discover()
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info("Finished discover")


@singer.utils.handle_top_exception(LOGGER)
def main():

    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    state = {}
    if parsed_args.state:
        state = parsed_args.state

    with Client(parsed_args.config) as client:
        if parsed_args.discover:
            do_discover()
        elif parsed_args.catalog:
            sync(client=client,
                    config=parsed_args.config,
                    catalog=parsed_args.catalog,
                    state=state)

if __name__ == "__main__":
    main()