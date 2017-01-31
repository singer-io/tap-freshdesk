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


QUIET = False
API_KEY = None
DOMAIN = None
BASE_URL = "https://{domain}.freshdesk.com"
PER_PAGE = 100
DATETIME_FMT = "%Y-%m-%dT%H:%M:%SZ"
DEFAULT_START_DATE = datetime.datetime(2000, 1, 1).strftime(DATETIME_FMT)
GET_COUNT = 0
PERSISTED_COUNT = 0

state = {
    "tickets": DEFAULT_START_DATE,
    "agents": DEFAULT_START_DATE,
    "roles": DEFAULT_START_DATE,
    "groups": DEFAULT_START_DATE,
    "companies": DEFAULT_START_DATE,
    "contacts": DEFAULT_START_DATE,
}

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


def stream_state():
    if not QUIET:
        stitchstream.write_state(state)
    else:
        logger.debug("Stream state")


def stream_schema(entity, schema):
    if not QUIET:
        stitchstream.write_schema(entity, schema)
    else:
        logger.debug("Stream schema {}".format(entity))


def stream_records(entity, records):
    if not QUIET:
        stitchstream.write_records(entity, records)
    else:
        logger.debug("Stream records {} ({})".format(entity, len(records)))


def load_config(config_file):
    global API_KEY
    global BASE_URL
    global DOMAIN

    with open(config_file) as f:
        data = json.load(f)

    API_KEY = data['api_key']
    DOMAIN = data['domain']
    BASE_URL = BASE_URL.format(domain=DOMAIN)


def load_state(state_file):
    with open(state_file) as f:
        state.update(json.load(f))


def load_schema(entity):
    path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                        "stream_freshdesk",
                        "{}.json".format(entity))
    with open(path) as f:
        return json.load(f)


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


def get_list(endpoint, **kwargs):
    url, params = get_url_and_params(endpoint, **kwargs)

    has_more = True
    page = 1
    items = []
    while has_more:
        if page > 1:
            params['page'] = page

        resp = request(url, params)
        data = resp.json()
        items.extend(data)
        if len(data) == PER_PAGE:
            page += 1
        else:
            has_more = False

    return items


def _sync_entity(endpoint, transform=None, sync_state=True, **kwargs):
    global PERSISTED_COUNT

    entity = kwargs.get("entity", endpoint)
    logger.info("{}: Starting sync".format(entity))

    schema = load_schema(entity)
    stream_schema(entity, schema)
    logger.info("{}: Sent schema".format(entity))

    items = get_list(endpoint, **kwargs)
    fetched_count = len(items)
    logger.info("{}: Got {}".format(entity, fetched_count))

    if items:
        if transform:
            items = transform(items)
            logger.info(
                "{}: After filter {}/{}".format(entity, len(items), fetched_count))

        stream_records(entity, items)
        PERSISTED_COUNT += len(items)
        logger.info("{}: Persisted {} records".format(entity, len(items)))
    else:
        logger.info("{}: None found".format(entity))

    if sync_state:
        state[entity] = datetime.datetime.utcnow().strftime(DATETIME_FMT)
        stream_state()
        logger.info("{}: State synced".format(entity))

    return items


def _transform_custom_fields(items):
    for item in items:
        if 'custom_fields' in item:
            transform = lambda x: [{"name": k, "value": v} for k, v in x.items()]
            item['custom_fields'] = transform(item['custom_fields'])

    return items

def _transform_remove_attachments(items):
    for item in items:
        item.pop('attachments', None)

    return items


def _mk_updated_at(entity, cmp_key):
    def _transform_updated_at(items):
        return [item for item in items if item[cmp_key] >= state[entity]]

    return _transform_updated_at


def _transform_tickets(items):
    items = _transform_remove_attachments(items)
    items = _transform_custom_fields(items)
    return items


def _transform_satisfaction_ratings(satisfaction_ratings):
    for satisfaction_rating in satisfaction_ratings:
        transform = lambda x: [{"question": k, "value": v} for k, v in x.items()]
        satisfaction_rating['ratings'] = transform(satisfaction_rating['ratings'])

    return satisfaction_ratings


def _transform_companies(items):
    _transform_updated_at = _mk_updated_at("companies", "updated_at")
    items = _transform_custom_fields(items)
    items = _transform_updated_at(items)
    return items


def do_sync():
    logger.info("Starting FreshDesk sync for {}".format(DOMAIN))

    # Tickets can be filtered and sorted by last updated, but the custom_fields
    # dict needs transforming. Also, the attachments field can be up to 15MB,
    # so we won't support that for now.
    tickets = _sync_entity("tickets",
                           updated_since=state['tickets'],
                           order_by="updated_at",
                           order_type="asc",
                           transform=_transform_tickets,
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
                     ticket_id=ticket['id'],
                     transform=_transform_remove_attachments,
                     sync_state=False)

        _sync_entity("sub_ticket",
                     entity="satisfaction_ratings",
                     ticket_id=ticket['id'],
                     transform=_transform_satisfaction_ratings,
                     sync_state=False)

        # The only endpoint sporting a timestamp of any kind is time_entries.
        # Filter that it's been updated since the last time we synced tickets.
        _sync_entity("sub_ticket",
                     entity="time_entries",
                     ticket_id=ticket['id'],
                     transform=_mk_updated_at("tickets", "updated_at"),
                     sync_state=False)


    # Once all tickets' subitems have been processed, we can update the ticket
    # state to now and push it to the persister.
    state['tickets'] = datetime.datetime.utcnow().strftime(DATETIME_FMT)
    stream_state()

    # Agents, roles, groups, and companies are not filterable, but they have
    # updated_at fields that can be used after grabbing them from the api.
    _sync_entity("agents", transform=_mk_updated_at("agents", "updated_at"))
    _sync_entity("roles", transform=_mk_updated_at("roles", "updated_at"))
    _sync_entity("groups", transform=_mk_updated_at("groups", "updated_at"))
    _sync_entity("companies", transform=_transform_companies)
    _sync_entity("contacts", transform=_transform_custom_fields)

    logger.info("Completed FreshDesk sync for {}. requests: {}, rows synced: {}"
                .format(DOMAIN, GET_COUNT, PERSISTED_COUNT))


def do_check():
    try:
        request(get_url_and_params("roles", **kwargs))
    except requests.exceptions.RequestException as e:
        logger.fatal("Error checking connection using {e.request.url}; "
                     "received status {e.response.status_code}: {e.response.test}".format(e=e))
        sys.exit(-1)


def main():
    global QUIET

    parser = argparse.ArgumentParser()
    parser.add_argument('func', choices=['check', 'sync'])
    parser.add_argument('-c', '--config', help='Config file', required=True)
    parser.add_argument('-s', '--state', help='State file')
    parser.add_argument('-d', '--debug', dest='debug', action='store_true',
                        help='Sets the log level to DEBUG (default INFO)')
    parser.add_argument('-q', '--quiet', dest='quiet', action='store_true',
                        help='Do not output to stdout (no persisting)')
    parser.set_defaults(debug=False, quiet=False)
    args = parser.parse_args()

    QUIET = args.quiet

    if args.debug:
        logger.setLevel(logging.DEBUG)

    load_config(args.config)
    if args.state:
        logger.info("Loading state from " + args.state)
        load_state(args.state)

    if args.func == "check":
        do_check()
    else:
        do_sync()


if __name__ == '__main__':
    main()
