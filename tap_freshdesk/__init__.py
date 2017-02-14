#!/usr/bin/env python3

import datetime

import requests
import singer

from . import utils


PER_PAGE = 100
CONFIG = {
    'base_url': "https://{}.freshdesk.com",
    'default_start_date': utils.strftime(datetime.datetime.utcnow() - datetime.timedelta(days=365)),

    # in config.json
    'api_key': None,
    'domain': None,
}
STATE = {}

endpoints = {
    "tickets": "/api/v2/tickets",
    "sub_ticket": "/api/v2/tickets/{ticket_id}/{entity}",
    "agents": "/api/v2/agents",
    "roles": "/api/v2/roles",
    "groups": "/api/v2/groups",
    "companies": "/api/v2/companies",
    "contacts": "/api/v2/contacts",
}

logger = singer.get_logger()


def request(url, params=None):
    params = params or {}
    response = requests.get(url, params=params, auth=(CONFIG['api_key'], ""))
    response.raise_for_status()
    return response


def get_url_and_params(endpoint, **kwargs):
    url = CONFIG['base_url'].format(CONFIG['domain']) + endpoints[endpoint]
    params = {k: v for k, v in kwargs.items() if k not in url}
    url = url.format(**kwargs)
    return (url, params)


def _sync_entity(endpoint, transform=None, sync_state=True, **kwargs):
    entity = kwargs.get("entity", endpoint)
    logger.info("{}: Starting sync".format(entity))

    schema = utils.load_schema(entity)
    singer.write_schema(entity, schema, "id")
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
                singer.write_record(entity, record)
                ids.append(record['id'])

        if sync_state:
            utils.update_state(STATE, entity, datetime.datetime.utcnow())
            singer.write_state(STATE)

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
    start_time = STATE.get(entity, CONFIG['default_start_date'])
    def _transform_updated_at(items):
        return [item for item in items if item[cmp_key] >= start_time]

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
    logger.info("Starting FreshDesk sync")

    # Tickets can be filtered and sorted by last updated, but the custom_fields
    # dict needs transforming. Also, the attachments field can be up to 15MB,
    # so we won't support that for now.
    ticket_ids = _sync_entity("tickets",
                              updated_since=STATE.get('tickets', CONFIG['default_start_date']),
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

    # Once all tickets' subitems have been processed, we can update the ticket
    # state to now and push it to the persister.
    utils.update_state(STATE, "tickets", datetime.datetime.utcnow())
    singer.write_state(STATE)

    # Agents, roles, groups, and companies are not filterable, but they have
    # updated_at fields that can be used after grabbing them from the api.
    _sync_entity("agents", transform=_mk_updated_at("agents", "updated_at"))
    _sync_entity("roles", transform=_mk_updated_at("roles", "updated_at"))
    _sync_entity("groups", transform=_mk_updated_at("groups", "updated_at"))
    _sync_entity("companies", transform=_transform_companies)
    _sync_entity("contacts", transform=_transform_custom_fields)

    logger.info("Completed sync")


def main():
    args = utils.parse_args()
    CONFIG.update(utils.load_json(args.config))
    if args.state:
        STATE.update(utils.load_json(args.state))
    do_sync()


if __name__ == '__main__':
    main()
