import time
import backoff
import requests
import singer
from simplejson import JSONDecodeError
from singer import utils

LOGGER = singer.get_logger()
BASE_URL = "https://{}.freshdesk.com"
REQUEST_TIMEOUT = 300
DEFAULT_PAGE_SIZE = 100

class FreshdeskException(Exception):
    """Custom error class for all the freshdesk errors."""

class FreshdeskValidationError(FreshdeskException):
    """Custom error class for validation error."""

class FreshdeskAuthenticationError(FreshdeskException):
    """Custom error class for authentication error."""

class FreshdeskAccessDeniedError(FreshdeskException):
    """Custom error class for access denied error."""

class FreshdeskNotFoundError(FreshdeskException):
    """Custom error class for not found error."""

class FreshdeskMethodNotAllowedError(FreshdeskException):
    """Custom error class for method not allowed."""

class FreshdeskUnsupportedAcceptHeaderError(FreshdeskException):
    """Custom error class for unsupported accept error."""

class FreshdeskConflictingStateError(FreshdeskException):
    """Custom error class for conflicting state."""

class FreshdeskUnsupportedContentError(FreshdeskException):
    """Custom error class for unsupported content."""

class FreshdeskRateLimitError(FreshdeskException):
    """Custom error class for rate limit error."""

class Server5xxError(FreshdeskException):
    """Custom error class for all the 5xx errors."""

class FreshdeskServerError(Server5xxError):
    """Custom error class for 500 server error"""


ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": FreshdeskValidationError,
        "message": "The request body/query string is not in the correct format."
    },
    401: {
        "raise_exception": FreshdeskAuthenticationError,
        "message": "The Authorization header is either missing or incorrect."
    },
    403: {
        "raise_exception": FreshdeskAccessDeniedError,
        "message": "The agent whose credentials were used to make this request was not authorized to perform this API call."
    },
    404: {
        "raise_exception": FreshdeskNotFoundError,
        "message": "The request contains invalid ID/Freshdesk domain in the URL or an invalid URL itself."
    },
    405: {
        "raise_exception": FreshdeskMethodNotAllowedError,
        "message": "This API request used the wrong HTTP verb/method."
    },
    406: {
        "raise_exception": FreshdeskUnsupportedAcceptHeaderError,
        "message": "Only application/json and */* are supported in 'Accepted' header."
    },
    409: {
        "raise_exception": FreshdeskConflictingStateError,
        "message": "The resource that is being created/updated is in an inconsistent or conflicting state."
    },
    415: {
        "raise_exception": FreshdeskUnsupportedContentError,
        "message": "Content type application/xml is not supported. Only application/json is supported."
    },
    429: {
        "raise_exception": FreshdeskRateLimitError,
        "message": "The API rate limit allotted for your Freshdesk domain has been exhausted."
    },
    500: {
        "raise_exception": FreshdeskServerError,
        "message": "Unexpected Server Error."
    }
}

def raise_for_error(response):
    """
    Retrieve the error code and the error message from the response and return custom exceptions accordingly.
    """
    error_code = response.status_code
    # Forming a response message for raising a custom exception
    try:
        response_json = response.json()
    except JSONDecodeError:
        response_json = {}

    if error_code not in ERROR_CODE_EXCEPTION_MAPPING and error_code > 500:
        # Raise `Server5xxError` for all 5xx unknown error
        exc = Server5xxError
    else:
        exc = ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("raise_exception", FreshdeskException)
    message = response_json.get("description", ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("message", "Unknown Error"))
    code = response_json.get("code", "")
    if code:
        error_code = f"{str(error_code)} {code}"
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
        self.timeout = REQUEST_TIMEOUT
        self.set_timeout()
        self.page_size = self.get_page_size()

    def __enter__(self):
        self.check_access_token()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        # Kill the session instance.
        self.session.close()

    def set_timeout(self):
        """
        Set timeout value from config, if the value is passed.
        Else raise an exception.
        """
        timeout = self.config.get("request_timeout", REQUEST_TIMEOUT)
        if ((type(timeout) in [int, float]) or
            (isinstance(timeout, str) and timeout.replace('.', '', 1).isdigit())) and float(timeout):
            self.timeout = float(timeout)
        else:
            raise Exception("The entered timeout is invalid, it should be a valid none-zero integer.")

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
                          (requests.Timeout, requests.ConnectionError, Server5xxError),
                          max_tries=5,
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
        response = self.session.send(req, timeout=self.timeout)

        # Call the function again if the rate limit is exceeded
        if 'Retry-After' in response.headers:
            retry_after = int(response.headers['Retry-After'])
            LOGGER.info("Rate limit reached. Sleeping for %s seconds", retry_after)
            time.sleep(retry_after)
            return self.request(url, params)

        if response.status_code != 200:
            raise_for_error(response)

        return response.json()
