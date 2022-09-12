
import backoff
import requests
import pendulum
from singer import utils
import singer
import time

LOGGER = singer.get_logger()

BASE_ID_URL = "https://id.getharvest.com/api/v2/"
BASE_API_URL = "https://api.harvestapp.com/v2/"
# timeout request after 300 seconds
REQUEST_TIMEOUT = 300

class HarvestError(Exception):
    pass

class Server5xxError(Exception):
    pass

class HarvestBadRequestError(HarvestError):
    pass

class HarvestUnauthorizedError(HarvestError):
    pass

class HarvestNotFoundError(HarvestError):
    pass

class HarvestForbiddenError(HarvestError):
    pass

class HarvestUnprocessableEntityError(HarvestError):
    pass

class HarvestRateLimitExceeededError(HarvestError):
    pass

class HarvestInternalServiceError(Server5xxError):
    pass

ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": HarvestBadRequestError,
        "message": "The request is missing or has a bad parameter."
    },
    401: {
        "raise_exception": HarvestUnauthorizedError,
        "message": "Invalid authorization credentials."
    },
    403: {
        "raise_exception": HarvestForbiddenError,
        "message": "User does not have permission to access the resource or "\
                   "related feature is disabled."
    },
    404: {
        "raise_exception": HarvestNotFoundError,
        "message": "The resource you have specified cannot be found."
    },
    422: {
        "raise_exception": HarvestUnprocessableEntityError,
        "message": "The request was not able to process right now."
    },
    429: {
        "raise_exception": HarvestRateLimitExceeededError,
        "message": "API rate limit exceeded."
    },
    500: {
        "raise_exception": HarvestInternalServiceError,
        "message": "An error has occurred at Harvest's end."
    }
}

def raise_for_error(response):
    """
    Forming a custom response message for raising an exception.
    """

    error_code = response.status_code
    try:
        response_json = response.json()
    except Exception:
        response_json = {}

    if error_code not in ERROR_CODE_EXCEPTION_MAPPING and error_code > 500:
        # Raise `Server5xxError` for all 5xx unknown error
        exc = Server5xxError
    else:
        exc = ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("raise_exception", HarvestError)
    error_message = response_json.get("error_description", ERROR_CODE_EXCEPTION_MAPPING.get(
            error_code, {}).get("message", "An Unknown Error occurred."))
    message = "HTTP-error-code: {}, Error: {}".format(error_code, error_message)

    raise exc(message) from None

class HarvestClient: #pylint: disable=too-many-instance-attributes
    """
    The client class is used for making REST calls to the Harvest API.
    """

    def __init__(self, config):
        self.config = config
        self._client_id = config['client_id']
        self._client_secret = config['client_secret']
        self._refresh_token = config['refresh_token']
        self._user_agent = config['user_agent']
        self._account_id = None
        self.session = requests.Session()
        self._access_token = None
        self._expires_at = None
        self.request_timeout = self.get_request_timeout()

    def __enter__(self):
        self._refresh_access_token()

    def __exit__(self, exception_type, exception_value, traceback):
        self.session.close()

    def get_request_timeout(self):
        """
        Get timeout value from config, if the value is passed.
        Else return the default value.
        """
        # Get `request_timeout` value from config.
        config_request_timeout = self.config.get('request_timeout')

        # If timeout is not passed in the config then set it to the default(300 seconds)
        if config_request_timeout is None:
            return REQUEST_TIMEOUT

        # If config request_timeout is other than 0,"0" or invalid string then use request_timeout
        if ((type(config_request_timeout) in [int, float]) or
                (isinstance(config_request_timeout,str) and config_request_timeout.replace('.', '', 1).isdigit())) and float(config_request_timeout):
            return float(config_request_timeout)
        raise Exception("The entered timeout is invalid, it should be a valid none-zero integer.")

    @backoff.on_exception(backoff.expo,
                          (HarvestRateLimitExceeededError, Server5xxError,
                           requests.Timeout, requests.ConnectionError),
                          max_tries=5,
                          factor=2)
    def _refresh_access_token(self):
        """
        Create an access token using the refresh token.
        """
        LOGGER.info("Refreshing access token")
        resp = self.session.request('POST',
                                    url=BASE_ID_URL + 'oauth2/token',
                                    data={
                                        'client_id': self._client_id,
                                        'client_secret': self._client_secret,
                                        'refresh_token': self._refresh_token,
                                        'grant_type': 'refresh_token',
                                    },
                                    headers={"User-Agent": self._user_agent})

        expires_in_seconds = resp.json().get('expires_in', 17 * 60 * 60)
        self._expires_at = pendulum.now().add(seconds=expires_in_seconds)
        resp_json = {}
        try:
            resp_json = resp.json()
            self._access_token = resp_json['access_token']
        # If an access token is not provided in response, raise an error
        except KeyError:
            if resp_json.get('error'):
                LOGGER.critical(resp_json.get('error'))
            if resp_json.get('error_description'):
                LOGGER.critical(resp_json.get('error_description'))
            raise_for_error(resp)
        LOGGER.info("Got refreshed access token")

    def get_access_token(self):
        """
        Return access token if available or generate one.
        """
        if self._access_token is not None and self._expires_at > pendulum.now():
            return self._access_token

        self._refresh_access_token()
        return self._access_token

    @backoff.on_exception(backoff.expo,
                          (HarvestRateLimitExceeededError, Server5xxError,
                           requests.Timeout, requests.ConnectionError),
                          max_tries=5,
                          factor=2)
    def get_account_id(self):
        """
        Get the account Id of the Active Harvest account.
        It will throw an exception if no active harvest account is found.
        """
        if self._account_id is not None:
            return self._account_id

        response = self.session.request('GET',
                                        url=BASE_ID_URL + 'accounts',
                                        headers={'Authorization': 'Bearer ' + self._access_token,
                                                 'User-Agent': self._user_agent},
                                        timeout=self.request_timeout)

        # Call the function again if the rate limit is exceeded
        if 'Retry-After' in response.headers:
            retry_after = int(response.headers['Retry-After'])
            LOGGER.info("Rate limit reached. Sleeping for %s seconds", retry_after)
            time.sleep(retry_after)
            return self.get_account_id()

        if response.status_code != 200:
            raise_for_error(response)

        if response.json().get('accounts'):
            self._account_id = str(response.json()['accounts'][0]['id'])
            return self._account_id

        raise Exception("No Active Harvest Account found") from None

    @backoff.on_exception(backoff.expo,
                          (HarvestRateLimitExceeededError, Server5xxError,
                           requests.Timeout, requests.ConnectionError),
                          max_tries=5,
                          factor=2)
    @utils.ratelimit(100, 15)
    def request(self, url, params=None):
        """
        Call rest API and return the response in case of status code 200.
        """
        params = params or {}
        access_token = self.get_access_token()
        headers = {"Accept": "application/json",
                   "Harvest-Account-Id": self.get_account_id(),
                   "Authorization": "Bearer " + access_token,
                   "User-Agent": self._user_agent}
        req = requests.Request("GET", url=url, params=params, headers=headers).prepare()
        LOGGER.info("GET %s", req.url)
        resp = self.session.send(req, timeout=self.request_timeout)

        # Call the function again if the rate limit is exceeded
        if 'Retry-After' in resp.headers:
            retry_after = int(resp.headers['Retry-After'])
            LOGGER.info("Rate limit reached. Sleeping for %s seconds", retry_after)
            time.sleep(retry_after)
            return self.request(url, params)

        if resp.status_code != 200:
            raise_for_error(resp)

        return resp.json()
