#!/usr/bin/env python3

import sys
import time

import backoff
import requests
from requests.exceptions import HTTPError
import singer

from tap_freshdesk import utils


REQUIRED_CONFIG_KEYS = ['api_key', 'domain', 'start_date']
PER_PAGE = 100
BASE_URL = "https://{}.freshdesk.com"
CONFIG = {}
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


@backoff.on_exception(backoff.expo,
                      (requests.exceptions.RequestException),
                      max_tries=5,
                      giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500,
                      factor=2)
@utils.ratelimit(1, 2)
def request(url, params=None):
    params = params or {}
    headers = {}
    if 'user_agent' in CONFIG:
        headers['User-Agent'] = CONFIG['user_agent']

    req = requests.Request('GET', url, params=params, auth=(CONFIG['api_key'], ""), headers=headers).prepare()
    logger.info("GET {}".format(req.url))
    resp = session.send(req)

    if 'Retry-After' in resp.headers:
        retry_after = int(resp.headers['Retry-After'])
        logger.info("Rate limit reached. Sleeping for {} seconds".format(retry_after))
        time.sleep(retry_after)
        return request(url, params)

    resp.raise_for_status()

    return resp


def get_start(entity):
    if entity not in STATE:
        STATE[entity] = CONFIG['start_date']

    return STATE[entity]


def gen_request(url, params=None):
    params = params or {}
    params["per_page"] = PER_PAGE
    page = 1
    while True:
        params['page'] = page
        data = request(url, params).json()
        for row in data:
            yield row

        if len(data) == PER_PAGE:
            page += 1
        else:
            break


def transform_dict(d, key_key="name", value_key="value", force_str=False):
    # Custom fields are expected to be strings, but sometimes the API sends
    # booleans. We cast those to strings to match the schema.
    rtn = []
    for k, v in d.items():
        if force_str:
            v = str(v).lower()
        rtn.append({key_key: k, value_key: v})
    return rtn


def sync_tickets():
    bookmark_property = 'updated_at'

    singer.write_schema("tickets",
                        utils.load_schema("tickets"),
                        ["id"],
                        bookmark_properties=[bookmark_property])

    singer.write_schema("conversations",
                        utils.load_schema("conversations"),
                        ["id"],
                        bookmark_properties=[bookmark_property])

    singer.write_schema("satisfaction_ratings",
                        utils.load_schema("satisfaction_ratings"),
                        ["id"],
                        bookmark_properties=[bookmark_property])

    singer.write_schema("time_entries",
                        utils.load_schema("time_entries"),
                        ["id"],
                        bookmark_properties=[bookmark_property])

    start = get_start("tickets")
    params = {
        'updated_since': start,
        'order_by': bookmark_property,
        'order_type': "asc",
    }
    for i, row in enumerate(gen_request(get_url("tickets"), params)):
        logger.info("Ticket {}: Syncing".format(row['id']))
        row.pop('attachments', None)
        row['custom_fields'] = transform_dict(row['custom_fields'], force_str=True)

        # get all sub-entities and save them
        logger.info("Ticket {}: Syncing conversations".format(row['id']))

        try:
            for subrow in gen_request(get_url("sub_ticket", id=row['id'], entity="conversations")):
                subrow.pop("attachments", None)
                subrow.pop("body", None)
                if subrow[bookmark_property] >= start:
                    singer.write_record("conversations", subrow, time_extracted=singer.utils.now())
        except HTTPError as e:
            if e.response.status_code == 403:
                logger.info('Invalid ticket ID requested from Freshdesk {0}'.format(row['id']))
            else:
                raise

        try:
            logger.info("Ticket {}: Syncing satisfaction ratings".format(row['id']))
            for subrow in gen_request(get_url("sub_ticket", id=row['id'], entity="satisfaction_ratings")):
                subrow['ratings'] = transform_dict(subrow['ratings'], key_key="question")
                if subrow[bookmark_property] >= start:
                    singer.write_record("satisfaction_ratings", subrow, time_extracted=singer.utils.now())
        except HTTPError as e:
            if e.response.status_code == 403:
                logger.info("The Surveys feature is unavailable. Skipping the satisfaction_ratings stream.")
            else:
                raise

        try:
            logger.info("Ticket {}: Syncing time entries".format(row['id']))
            for subrow in gen_request(get_url("sub_ticket", id=row['id'], entity="time_entries")):
                if subrow[bookmark_property] >= start:
                    singer.write_record("time_entries", subrow, time_extracted=singer.utils.now())

        except HTTPError as e:
            if e.response.status_code == 403:
                logger.info("The Timesheets feature is unavailable. Skipping the time_entries stream.")
            else:
                raise

        utils.update_state(STATE, "tickets", row[bookmark_property])
        singer.write_record("tickets", row, time_extracted=singer.utils.now())
        singer.write_state(STATE)


def sync_time_filtered(entity):
    bookmark_property = 'updated_at'

    singer.write_schema(entity,
                        utils.load_schema(entity),
                        ["id"],
                        bookmark_properties=[bookmark_property])
    start = get_start(entity)

    logger.info("Syncing {} from {}".format(entity, start))
    for row in gen_request(get_url(entity)):
        if row[bookmark_property] >= start:
            if 'custom_fields' in row:
                row['custom_fields'] = transform_dict(row['custom_fields'], force_str=True)

            utils.update_state(STATE, entity, row[bookmark_property])
            singer.write_record(entity, row, time_extracted=singer.utils.now())

    singer.write_state(STATE)


def do_sync():
    logger.info("Starting FreshDesk sync")

    try:
        sync_tickets()
        sync_time_filtered("agents")
        sync_time_filtered("roles")
        sync_time_filtered("groups")
        # commenting out this high-volume endpoint for now
        #sync_time_filtered("contacts")
        sync_time_filtered("companies")
    except HTTPError as e:
        logger.critical(
            "Error making request to Freshdesk API: GET %s: [%s - %s]",
            e.request.url, e.response.status_code, e.response.content)
        sys.exit(1)

    logger.info("Completed sync")


def main_impl():
    config, state = utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(config)
    STATE.update(state)
    do_sync()

def main():
    try:
        main_impl()
    except Exception as exc:
        logger.critical(exc)
        raise exc

if __name__ == '__main__':
    main()
