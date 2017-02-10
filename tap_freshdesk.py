#!/usr/bin/env python3

import argparse
import datetime
import json
import os

import backoff
import requests
import stitchstream


API_KEY = None
DOMAIN = None
BASE_URL = "https://{domain}.freshdesk.com"
PER_PAGE = 100
DATETIME_FMT = "%Y-%m-%dT%H:%M:%SZ"
DEFAULT_START_DATE = datetime.datetime(2000, 1, 1).strftime(DATETIME_FMT)
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

logger = stitchstream.get_logger()


def load_schema(entity):
    path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "tap_freshdesk",
        "{}.json".format(entity))

    with open(path) as f:
        return json.load(f)


@backoff.on_exception(backoff.expo,
                      (requests.exceptions.RequestException),
                      max_tries=5,
                      giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500,
                      factor=2)
def request(url, params=None):
    params = params or {}
    response = requests.get(url, params=params, auth=(API_KEY, ""))
    response.raise_for_status()
    return response


def get_url_and_params(endpoint, **kwargs):
    url = BASE_URL + endpoints[endpoint]
    params = {k: v for k, v in kwargs.items() if k not in url}
    url = url.format(**kwargs)
    return (url, params)


def _sync_entity(endpoint, transform=None, sync_state=True, **kwargs):
    global PERSISTED_COUNT

    entity = kwargs.get("entity", endpoint)
    logger.info("{}: Starting sync".format(entity))

    schema = load_schema(entity)
    stitchstream.write_schema(entity, schema, "id")
    logger.info("{}: Sent schema".format(entity))

    url, params = get_url_and_params(endpoint, **kwargs)

    has_more = True
    page = 1
    ids = []
    while has_more:
        params['page'] = page
        resp = request(url, params)
        data = resp.json()

        if data:
            if transform:
                data = transform(data)

            for record in data:
                stitchstream.write_record(entity, record)
                PERSISTED_COUNT += 1
                ids.append(record['id'])

        if sync_state:
            state[entity] = datetime.datetime.utcnow().strftime(DATETIME_FMT)
            stitchstream.write_state(state)

        if len(data) == PER_PAGE:
            page += 1
        else:
            has_more = False

    logger.info("{}: Sent {} rows".format(entity, len(ids)))
    return ids


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


def _transform_remove_body(items):
    for item in items:
        item.pop('body', None)

    return items


def _mk_updated_at(entity, cmp_key):
    def _transform_updated_at(items):
        return [item for item in items if item[cmp_key] >= state[entity]]

    return _transform_updated_at


def _transform_tickets(items):
    items = _transform_remove_attachments(items)
    items = _transform_custom_fields(items)
    return items


def _transform_conversations(items):
    items = _transform_remove_attachments(items)
    items = _transform_remove_body(items)
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
    ticket_ids = _sync_entity("tickets",
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
    for ticket_id in ticket_ids:
        _sync_entity("sub_ticket",
                     entity="conversations",
                     ticket_id=ticket_id,
                     transform=_transform_conversations,
                     sync_state=False)

        _sync_entity("sub_ticket",
                     entity="satisfaction_ratings",
                     ticket_id=ticket_id,
                     transform=_transform_satisfaction_ratings,
                     sync_state=False)

        # The only endpoint sporting a timestamp of any kind is time_entries.
        # Filter that it's been updated since the last time we synced tickets.
        _sync_entity("sub_ticket",
                     entity="time_entries",
                     ticket_id=ticket_id,
                     transform=_mk_updated_at("tickets", "updated_at"),
                     sync_state=False)

    state['tickets'] = datetime.datetime.utcnow().strftime(DATETIME_FMT)

    # Once all tickets' subitems have been processed, we can update the ticket
    # state to now and push it to the persister.
    stitchstream.write_state(state)

    # Agents, roles, groups, and companies are not filterable, but they have
    # updated_at fields that can be used after grabbing them from the api.
    _sync_entity("agents", transform=_mk_updated_at("agents", "updated_at"))
    _sync_entity("roles", transform=_mk_updated_at("roles", "updated_at"))
    _sync_entity("groups", transform=_mk_updated_at("groups", "updated_at"))
    _sync_entity("companies", transform=_transform_companies)
    _sync_entity("contacts", transform=_transform_custom_fields)

    logger.info("Completed FreshDesk sync for {}. Rows synced: {}"
                .format(DOMAIN, PERSISTED_COUNT))


def main():
    global API_KEY
    global BASE_URL
    global DOMAIN

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file', required=True)
    parser.add_argument('-s', '--state', help='State file')
    args = parser.parse_args()

    with open(args.config) as f:
        data = json.load(f)

    API_KEY = data['api_key']
    DOMAIN = data['domain']
    BASE_URL = BASE_URL.format(domain=DOMAIN)

    if args.state:
        logger.info("Loading state from " + args.state)
        with open(args.state) as f:
            state.update(json.load(f))

    do_sync()


if __name__ == '__main__':
    main()
