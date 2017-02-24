#!/usr/bin/env python3

import datetime

import requests
import singer

from tap_freshdesk import utils


PER_PAGE = 100
BASE_URL = "https://{}.freshdesk.com"
CONFIG = {
    'api_key': None,
    'domain': None,
    'start_date': None,
}
STATE = {}

endpoints = {
    "tickets": "/api/v2/tickets",
    "sub_ticket": "/api/v2/tickets/{id}/{entity}",
    "agents": "/api/v2/agents",
    "roles": "/api/v2/roles",
    "groups": "/api/v2/groups",
    "companies": "/api/v2/companies",
    "contacts": "/api/v2/contacts",
}

logger = singer.get_logger()
session = requests.Session()


def get_url(endpoint, **kwargs):
    return BASE_URL.format(CONFIG['domain']) + endpoints[endpoint].format(**kwargs)


def get_start(entity):
    if entity not in STATE:
        STATE[entity] = CONFIG['start_date']

    return STATE[entity]


def gen_request(url, params=None):
    params = params or {}
    page = 1
    while True:
        params['page'] = page
        req = requests.Request('GET', url, params=params, auth=(CONFIG['api_key'], "")).prepare()
        logger.info("GET {}".format(req.url))
        resp = session.send(req)
        resp.raise_for_status()
        data = resp.json()

        for row in data:
            yield row

        if len(data) == PER_PAGE:
            page += 1
        else:
            break


def transform_dict(d, key_key="name", value_key="value"):
    return [{key_key: k, value_key: v} for k, v in d.items()]


def sync_tickets():
    singer.write_schema("tickets", utils.load_schema("tickets"), ["id"])
    singer.write_schema("conversations", utils.load_schema("conversations"), ["id"])
    singer.write_schema("satisfaction_ratings", utils.load_schema("satisfaction_ratings"), ["id"])
    singer.write_schema("time_entries", utils.load_schema("time_entries"), ["id"])

    start = get_start("tickets")
    params = {
        'updated_since': start,
        'order_by': "updated_at",
        'order_type': "asc",
    }
    for row in gen_request(get_url("tickets"), params):
        logger.info("Ticket {}: Syncing".format(row['id']))
        row.pop('attachments', None)
        row['custom_fields'] = transform_dict(row['custom_fields'])

        # get all sub-entities and save them
        logger.info("Ticket {}: Syncing conversations".format(row['id']))
        for subrow in gen_request(get_url("sub_ticket", id=row['id'], entity="conversations")):
            subrow.pop("attachments", None)
            subrow.pop("body", None)
            singer.write_record("conversations", subrow)

        logger.info("Ticket {}: Syncing satisfaction ratings".format(row['id']))
        for subrow in gen_request(get_url("sub_ticket", id=row['id'], entity="satisfaction_ratings")):
            subrow['ratings'] = transform_dict(subrow['ratings'], key_key="question")
            singer.write_record("satisfaction_ratings", subrow)

        logger.info("Ticket {}: Syncing time entries".format(row['id']))
        for subrow in gen_request(get_url("sub_ticket", id=row['id'], entity="time_entries")):
            if subrow['updated_at'] >= start:
                singer.write_record("time_entries", subrow)

        utils.update_state(STATE, "tickets", row['updated_at'])
        singer.write_record("tickets", row)
        singer.write_state(STATE)


def sync_time_filtered(entity):
    singer.write_schema(entity, utils.load_schema(entity), ["id"])
    start = get_start(entity)

    logger.info("Syncing {} from {}".format(entity, start))
    for row in gen_request(get_url(entity)):
        if row['updated_at'] >= start:
            if 'custom_fields' in row:
                row['custom_fields'] = transform_dict(row.get('custom_fields', {}))

            utils.update_state(STATE, entity, row['updated_at'])
            singer.write_record(entity, row)

    singer.write_state(STATE)


def do_sync():
    logger.info("Starting FreshDesk sync")

    sync_tickets()
    sync_time_filtered("agents")
    sync_time_filtered("roles")
    sync_time_filtered("groups")
    sync_time_filtered("contacts")
    sync_time_filtered("companies")

    logger.info("Completed sync")


def main():
    args = utils.parse_args()

    logger.setLevel(0)

    config = utils.load_json(args.config)
    utils.check_config(config, ['api_key', 'domain', 'start_date'])
    CONFIG.update(config)

    if args.state:
        STATE.update(utils.load_json(args.state))

    do_sync()


if __name__ == '__main__':
    main()
