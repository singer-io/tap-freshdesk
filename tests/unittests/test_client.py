import unittest
from unittest import mock
from tap_freshdesk import client
import requests
import json

def get_response(status_code, json_resp={}, headers = None):
    """
    Returns mock response
    """
    response = requests.Response()
    response.status_code = status_code
    response._content = json.dumps(json_resp).encode()
    if headers:
        response.headers = headers
    return response


class TestAccessToken(unittest.TestCase):
    """
    Test `check_access_token` method of client class
    """

    @mock.patch("tap_freshdesk.client.FreshdeskClient.request")
    def test_access_token(self, mock_request):
        """
        Test that to check access token a request call is made.
        """
        config = {"domain": "sampleDomain"}
        _client = client.FreshdeskClient(config)
        _client.check_access_token()

        # Verify that for check access token, `request` method was called
        self.assertTrue(mock_request.called)
        mock_request.assert_called_with("https://sampleDomain.freshdesk.com/api/v2/roles", mock.ANY)
