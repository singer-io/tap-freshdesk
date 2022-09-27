import time
import backoff
import requests
import singer
from singer import utils


LOGGER = singer.get_logger()
BASE_URL = "https://{}.freshdesk.com"
DEFAULT_PAGE_SIZE = 100


class FreshdeskClient:
    """
    The client class is used for making REST calls to the Freshdesk API.
    """

    def __init__(self, config):
        self.config = config
        self.session = requests.Session()
        self.base_url = BASE_URL.format(config.get("domain"))
        self.page_size = self.get_page_size()

    def __enter__(self):
        self.check_access_token()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        # Kill the session instance.
        self.session.close()

    def get_page_size(self):
        """
        This function will get page size from config,
        and will return the default value if an invalid page size is given.
        """
        page_size = self.config.get('page_size')

        # return a default value if no page size is given in the config
        if page_size is None:
            return DEFAULT_PAGE_SIZE

        # Return integer value if the valid value is given
        if (type(page_size) in [int, float] and page_size > 0) or \
                (isinstance(page_size, str) and page_size.replace('.', '', 1).isdigit() and (float(page_size) > 0)):
            return int(float(page_size))
        # Raise an exception for 0, "0" or invalid value of page_size
        raise Exception("The entered page size is invalid, it should be a valid integer.")

    def check_access_token(self):
        """
        Check if the access token is valid.
        """
        self.request(self.base_url+"/api/v2/roles", {"per_page": 1, "page": 1})

    @backoff.on_exception(backoff.expo,
                          (requests.exceptions.RequestException),
                          max_tries=5,
                          giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500,
                          factor=2)
    @utils.ratelimit(1, 2)
    def request(self, url, params=None):
        """
        Call rest API and return the response in case of status code 200.
        """
        headers = {}
        if 'user_agent' in self.config:
            headers['User-Agent'] = self.config['user_agent']

        req = requests.Request('GET', url, params=params, auth=(self.config['api_key'], ""), headers=headers).prepare()
        LOGGER.info("GET %s", req.url)
        response = self.session.send(req)

        # Call the function again if the rate limit is exceeded
        if 'Retry-After' in response.headers:
            retry_after = int(response.headers['Retry-After'])
            LOGGER.info("Rate limit reached. Sleeping for %s seconds", retry_after)
            time.sleep(retry_after)
            return self.request(url, params)

        response.raise_for_status()

        return response.json()
