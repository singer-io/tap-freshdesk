#!/usr/bin/env python3

import argparse
import datetime
import json
import logging
import logging.config
import sys

import backoff
import dateutil.parser
import requests
import stitchstream


API_KEY = None
BASE_URL = "https://{domain}.freshdesk.com"
PER_PAGE = 100

logging.config.fileConfig("/etc/stitch/logging.conf")
logger = logging.getLogger("stitch.streamer")

default_start_date = datetime.datetime(2000, 1, 1).isoformat()
state = {
    "tickets": default_start_date,
    "conversations": default_start_date,
    "contacts": default_start_date,
    "agents": default_start_date,
    "roles": default_start_date,
    "groups": default_start_date,
    "companies": default_start_date,
}

endpoints = {
    "tickets": "/api/v2/tickets",
    "conversations": "/api/v2/tickets/{ticket_id}/conversations",
    "contacts": "/api/v2/contacts",
    "agents": "/api/v2/agents",
    "roles": "/api/v2/roles",
    "groups": "/api/v2/groups",
    "companies": "/api/v2/companies",
}


def load_config(config_file):
    global API_KEY
    global BASE_URL

    with open(config_file) as f:
        config = json.load(f)

    API_KEY = config['api_key']
    BASE_URL = BASE_URL.format(domain=config['domain'])


def load_state(state_file):
    with open(state_file) as f:
        state = json.load(f)

    state.update(state)


@backoff.on_exception(backoff.expo,
                      (requests.exceptions.RequestException),
                      max_tries=5,
                      giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500,
                      factor=2)
def request(url, params=None):
    params = params or {}
    response = requests.get(url, params=params, auth=(API_KEY, "notused"))
    response.raise_for_status()
    return response


def api_request(endpoint, **kwargs):
    url = BASE_URL + endpoints[endpoint]
    params = {k: v for k, v in kwargs.items() if k not in url}
    url = url.format(**kwargs)
    request(url, params)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('func', choices=['check', 'sync'])
    args = parser.parse_args()
    parser.add_argument('-c', '--config', help='Config file', required=True)
    parser.add_argument('-s', '--state', help='State file')
    parser.add_argument('-d', '--debug', dest='debug', action='store_true',
                        help='Sets the log level to DEBUG (default INFO)')
    parser.set_defaults(debug=False)

    if args.debug:
        logger.setLevel(logging.DEBUG)

    load_config()
    if args.state:
        logger.info("Loading state from " + args.state)
        load_state(args.state)

    if args.func == "check":
        do_check()
    else:
        do_sync()


if __name__ == '__main__':
    main()
