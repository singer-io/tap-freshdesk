import time

import backoff
import requests
import singer
from tap_freshdesk import utils

LOGGER = singer.get_logger()
BASE_URL = "https://{}.freshdesk.com"
DEFAULT_TIMEOUT = 6

class FreshdeskException(Exception):
    pass

class FresdeskValidationError(FreshdeskException):
    pass

class FresdeskAuthenticationError(FreshdeskException):
    pass

class FresdeskAccessDeniedError(FreshdeskException):
    pass

class FresdeskNotFoundError(FreshdeskException):
    pass

class FresdeskMethodNotAllowedError(FreshdeskException):
    pass

class FresdeskUnsupportedAcceptHeaderError(FreshdeskException):
    pass

class FresdeskConflictingStateError(FreshdeskException):
    pass

class FresdeskUnsupportedContentError(FreshdeskException):
    pass

class FresdeskRateLimitError(FreshdeskException):
    pass

class Server5xxError(FreshdeskException):
    pass

class FresdeskServerError(Server5xxError):
    pass


ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": FresdeskValidationError,
        "message": "The request body/query string is not in the correct format."
    },
    401: {
        "raise_exception": FresdeskAuthenticationError,
        "message": "The Authorization header is either missing or incorrect."
    },
    403: {
        "raise_exception": FresdeskAccessDeniedError,
        "message": "The agent whose credentials were used in making this request was not authorized to perform this API call."
    },
    404: {
        "raise_exception": FresdeskNotFoundError,
        "message": "The request contains invalid ID/Freshdesk domain in the URL or an invalid URL itself."
    },
    405: {
        "raise_exception": FresdeskMethodNotAllowedError,
        "message": "This API request used the wrong HTTP verb/method."
    },
    406: {
        "raise_exception": FresdeskUnsupportedAcceptHeaderError,
        "message": "Only application/json and */* are supported in 'Accepted' header."
    },
    409: {
        "raise_exception": FresdeskConflictingStateError,
        "message": "The resource that is being created/updated is in an inconsistent or conflicting state."
    },
    415: {
        "raise_exception": FresdeskUnsupportedContentError,
        "message": "Content type application/xml is not supported. Only application/json is supported."
    },
    429: {
        "raise_exception": FresdeskRateLimitError,
        "message": "The API rate limit allotted for your Freshdesk domain has been exhausted."
    },
    500: {
        "raise_exception": FresdeskServerError,
        "message": "Unexpected Server Error."
    },
}

def raise_for_error(response):
    """
    Retrieve the error code and the error message from the response and return custom exceptions accordingly.
    """
    try:
        response.raise_for_status()
    except (requests.HTTPError) as error:
        error_code = response.status_code
        # Forming a response message for raising a custom exception
        try:
            response_json = response.json()
        except Exception:
            response_json = {}

        if error_code not in ERROR_CODE_EXCEPTION_MAPPING and error_code > 500:
            # Raise `Server5xxError` for all 5xx unknown error
            exc = Server5xxError
        else:
            exc = ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("raise_exception", FreshdeskException)
        message = response_json.get("description", ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("message", "Unknown Error"))
        formatted_message = "HTTP-error-code: {}, Error: {}".format(error_code, message)
        raise exc(formatted_message) from None

class FreshdeskClient:
    """
    The client class is used for making REST calls to the Freshdesk API.
    """

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
        """
        Check if the access token is valid.
        """
        try:
            self.request(self.base_url+"/api/v2/tickets/1/time_entries", {"per_page": 1, "page": 1})
        except FresdeskAccessDeniedError:
            LOGGER.warning("The `Surveys` and the `Timesheets` features are not supported in your current plan. "\
                            "So, Data collection cannot be initiated for satisfaction_ratings and time_entries streams. "\
                            "Please upgrade your account to `Pro` plan to use it.")

    @backoff.on_exception(backoff.expo,
                          (TimeoutError, ConnectionError, Server5xxError),
                          max_tries=5,
                          factor=2)
    @utils.ratelimit(1, 2)
    def request(self, url, params={}):
        """
        Call rest API and return the response in case of status code 200.
        """
        headers = {}
        if 'user_agent' in self.config:
            headers['User-Agent'] = self.config['user_agent']

        req = requests.Request('GET', url, params=params, auth=(self.config['api_key'], ""), headers=headers).prepare()
        LOGGER.info("GET {}".format(req.url))
        response = self.session.send(req, timeout=DEFAULT_TIMEOUT)

        # Call the function again if the rate limit is exceeded
        if 'Retry-After' in response.headers:
            retry_after = int(response.headers['Retry-After'])
            LOGGER.info("Rate limit reached. Sleeping for {} seconds".format(retry_after))
            time.sleep(retry_after)
            return self.request(url, params)

        if response.status_code != 200:
            raise_for_error(response)

        return response.json()
