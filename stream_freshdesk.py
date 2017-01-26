#!/usr/bin/env python3

import argparse
import datetime
import json
import logging
import logging.config
import os
import sys

import backoff
import dateutil.parser
import requests
import stitchstream


API_KEY = None
BASE_URL = "https://{domain}.freshdesk.com"
PER_PAGE = 100
DATETIME_FMT = "%Y-%m-%dT%H:%M:%SZ"
DEFAULT_START_DATE = datetime.datetime(2000, 1, 1).strftime(DATETIME_FMT)

GET_COUNT = 0

state = {}
endpoints = {
    "tickets": "/api/v2/tickets",
    "sub_ticket": "/api/v2/tickets/{ticket_id}/{entity}",
    "agents": "/api/v2/agents",
    "roles": "/api/v2/roles",
    "groups": "/api/v2/groups",
    "companies": "/api/v2/companies",
    "contacts": "/api/v2/contacts",
}

logging.config.fileConfig("/etc/stitch/logging.conf")
logger = logging.getLogger("stitch.streamer")
session = requests.Session()


def stream(method, data=None, entity_type=None):
    if not QUIET:
        if method == "state":
            stitchstream.write_state(state)
        elif method == "schema":
            stitchstream.write_schema(entity_type, data)
        elif method == "records":
            stitchstream.write_records(entity_type, data)
        else:
            raise ValueError("Unknown method {}".format(method))


def load_config(config_file):
    global API_KEY
    global BASE_URL

    with open(config_file) as f:
        data = json.load(f)

    API_KEY = data['api_key']
    BASE_URL = BASE_URL.format(domain=data['domain'])


def load_state(state_file):
    with open(state_file) as f:
        data = json.load(f)

    for entity in entities:
        state[entity] = data.get(entity, DEFAULT_START_DATE)


@backoff.on_exception(backoff.expo,
                      (requests.exceptions.RequestException),
                      max_tries=5,
                      giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500,
                      factor=2)
def request(url, params=None):
    global GET_COUNT
    params = params or {}

    logger.debug("Making request: GET {} {}".format(url, params))
    response = session.get(url, params=params, auth=(API_KEY, "notused"))
    logger.debug("Got response code: {}".format(response.status_code))

    GET_COUNT += 1
    response.raise_for_status()
    return response


def get_url_and_params(endpoint, **kwargs):
    url = BASE_URL + endpoints[endpoint]
    params = {k: v for k, v in kwargs.items() if k not in url}
    url = url.format(**kwargs)
    return (url, params)


def api_request(endpoint, **kwargs):
    return request(get_url_and_params(endpoint, **kwargs))


def get_list(endpoint, **kwargs):
    url, params = get_url_and_params(endpoint, **kwargs)

    if created_at:
        params

    has_more = True
    page = 1
    items = []
    while has_more:
        if page > 1:
            params['page'] = page

        resp = request(url, params)
        items.extend(data)
        if len(data) == PER_PAGE:
            page += 1
        else:
            has_more = False

    return items


def _sync_entity(endpoint,
                 transform=None,
                 sync_state=True,
                 **kwargs)
    entity = kwargs.get("entity", endpoint)

    logger.info("{}: Starting sync".format(entity))
    items = get_list(endpoint, **kwargs)
    fetched_count = len(items)
    logger.info("{}: Got {}".format(entity, fetched_count))

    if items:
        if transform:
            items = transform(items)
            logger.info(
                "{}: After filter {}/{}".format(entity, len(items), fetched_count))

        stream("records", entity, items)
        logger.info("{}: Persisted {} records".format(entity, len(items)))
    else:
        logger.info("{}: None found".format(entity))

    if sync_state:
        state[entity] = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        stream("state")
        logger.info("{}: State synced".format(entity))

    return items


def _transform_tickets(tickets):
    rtn = []
    for ticket in tickets:
        ticket.pop('attachments')
        if 'custom_field' in ticket:
            pack = lambda x: [{"name": k, "value": v} for k, v in x.items()]
            ticket['custom_fields'] = pack(ticket['custom_fields'])

        rtn.append(ticket)

    return rtn


def _mk_updated_at(entity, cmp_key):
    def _transform_updated_at(items):
        return [item for item in items if item[cmp_key] >= state[entity]]

    return _transform_updated_at


def do_sync():
    # Tickets can be filtered and sorted by last updated, but the custom_fields
    # dict needs transforming. Also, the attachments field can be up to 15MB,
    # so we won't support that for now.
    tickets = _sync_entity("tickets",
                           updated_since=state['tickets'],
                           order_by="updated_at",
                           order_type="asc",
                           transform=_transform_ticket,
                           sync_state=False)

    # Each ticket has conversations, time_entries, and satisfaction_ratings
    # linked. So let's get those for each ticket that's been updated.
    #
    # NOTE!: If these things can update or be created without the ticket being
    # touched, then this approach won't work and we're need to rethink. Maybe
    # look at tickets updated in the last week and iterate through those.
    for ticket in tickets:
        _sync_entity("sub_ticket",
                     entity="conversations",
                     ticket_id=ticket['id']
                     sync_state=False)

        _sync_entity("sub_ticket",
                     entity="satisfaction_ratings",
                     ticket_id=ticket['id'],
                     sync_state=False)

        # The only endpoint sporting a timestamp of any kind is time_entries.
        # Filter that it's been updated since the last time we synced tickets.
        _sync_entity("sub_ticket",
                     entity="time_entries",
                     ticket_id=ticket['id'],
                     transform=_mk_updated_at("tickets", "updated_at"),
                     updated_at=state['tickets'],
                     cmp_key="created_at",
                     sync_state=False)


    # Once all tickets' subitems have been processed, we can update the ticket
    # state to now and push it to the persister.
    state['tickets'] = datetime.datetime.utcnow().strftime(DATETIME_FMT)
    stream("state")

    # Agents, roles, groups, and companies are not filterable, but they have
    # updated_at fields that can be used after grabbing them from the api.
    _sync_entity("agents", transform=_mk_updated_at("agents", "updated_at"))
    _sync_entity("roles", transform=_mk_updated_at("roles", "updated_at"))
    _sync_entity("groups", transform=_mk_updated_at("groups", "updated_at"))
    _sync_entity("companies", transform=_mk_updated_at("companies", "updated_at"))
    _sync_entity("contacts")


def do_check():
    try:
        api_request("roles")
    except requests.exceptions.RequestException as e:
        logger.fatal("Error checking connection using {e.request.url}; "
                     "received status {e.response.status_code}: {e.response.test}".format(e=e))
        sys.exit(-1)


def main():
    global QUIET

    parser = argparse.ArgumentParser()
    parser.add_argument('func', choices=['check', 'sync'])
    args = parser.parse_args()
    parser.add_argument('-c', '--config', help='Config file', required=True)
    parser.add_argument('-s', '--state', help='State file')
    parser.add_argument('-d', '--debug', dest='debug', action='store_true',
                        help='Sets the log level to DEBUG (default INFO)')
    parser.add_argument('-q', '--quiet', dest='quiet', action='store_true',
                        help='Do not output to stdout (no persisting)')
    parser.set_defaults(debug=False, quiet=False)

    QUIET = args.quiet

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
