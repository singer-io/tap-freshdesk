class freshdeskError(Exception):
    """class representing Generic Http error."""

    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response


class freshdeskBackoffError(freshdeskError):
    """class representing backoff error handling."""
    pass

class freshdeskBadRequestError(freshdeskError):
    """class representing 400 status code."""
    pass

class freshdeskUnauthorizedError(freshdeskError):
    """class representing 401 status code."""
    pass


class freshdeskForbiddenError(freshdeskError):
    """class representing 403 status code."""
    pass

class freshdeskNotFoundError(freshdeskError):
    """class representing 404 status code."""
    pass

class freshdeskConflictError(freshdeskError):
    """class representing 406 status code."""
    pass

class freshdeskUnprocessableEntityError(freshdeskBackoffError):
    """class representing 409 status code."""
    pass

class freshdeskRateLimitError(freshdeskBackoffError):
    """class representing 429 status code."""
    pass

class freshdeskInternalServerError(freshdeskBackoffError):
    """class representing 500 status code."""
    pass

class freshdeskNotImplementedError(freshdeskBackoffError):
    """class representing 501 status code."""
    pass

class freshdeskBadGatewayError(freshdeskBackoffError):
    """class representing 502 status code."""
    pass

class freshdeskServiceUnavailableError(freshdeskBackoffError):
    """class representing 503 status code."""
    pass

ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": freshdeskBadRequestError,
        "message": "A validation exception has occurred."
    },
    401: {
        "raise_exception": freshdeskUnauthorizedError,
        "message": "The access token provided is expired, revoked, malformed or invalid for other reasons."
    },
    403: {
        "raise_exception": freshdeskForbiddenError,
        "message": "You are missing the following required scopes: read"
    },
    404: {
        "raise_exception": freshdeskNotFoundError,
        "message": "The resource you have specified cannot be found."
    },
    409: {
        "raise_exception": freshdeskConflictError,
        "message": "The API request cannot be completed because the requested operation would conflict with an existing item."
    },
    422: {
        "raise_exception": freshdeskUnprocessableEntityError,
        "message": "The request content itself is not processable by the server."
    },
    429: {
        "raise_exception": freshdeskRateLimitError,
        "message": "The API rate limit for your organisation/application pairing has been exceeded."
    },
    500: {
        "raise_exception": freshdeskInternalServerError,
        "message": "The server encountered an unexpected condition which prevented" \
            " it from fulfilling the request."
    },
    501: {
        "raise_exception": freshdeskNotImplementedError,
        "message": "The server does not support the functionality required to fulfill the request."
    },
    502: {
        "raise_exception": freshdeskBadGatewayError,
        "message": "Server received an invalid response."
    },
    503: {
        "raise_exception": freshdeskServiceUnavailableError,
        "message": "API service is currently unavailable."
    }
}