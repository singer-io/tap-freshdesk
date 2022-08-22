import time

import backoff
import requests
import singer
from tap_freshdesk import utils

LOGGER = singer.get_logger()
DEFAULT_TIMEOUT = 300
BASE_URL = "https://{}.freshdesk.com"


class FreshdeskClient:

    def __init__(self, config):
        self.config = config
        self.session = requests.Session()
        self.base_url = BASE_URL.format(config.get("domain"))

    def __enter__(self):
        self.check_access_token()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        # Kill the session instance.
        self.session.close()

    def check_access_token(self):
        self.request(self.base_url+"/api/v2/roles", {"per_page": 1, "page": 1})

    @backoff.on_exception(backoff.expo,
                          (requests.exceptions.RequestException),
                          max_tries=5,
                          giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500,
                          factor=2)
    @utils.ratelimit(1, 2)
    def request(self, url, params={}):
        headers = {}
        if 'user_agent' in self.config:
            headers['User-Agent'] = self.config['user_agent']

        req = requests.Request('GET', url, params=params, auth=(self.config['api_key'], ""), headers=headers).prepare()
        LOGGER.info("GET {}".format(req.url))
        response = self.session.send(req)

        if 'Retry-After' in response.headers:
            retry_after = int(response.headers['Retry-After'])
            LOGGER.info("Rate limit reached. Sleeping for {} seconds".format(retry_after))
            time.sleep(retry_after)
            return self.request(url, params)

        response.raise_for_status()

        return response.json()


