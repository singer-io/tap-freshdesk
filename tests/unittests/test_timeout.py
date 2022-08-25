import unittest
from unittest import mock
from parameterized import parameterized
from tap_freshdesk.client import FreshdeskClient, DEFAULT_TIMEOUT


class TestTimeOut(unittest.TestCase):
    """
    Test `set_timeout` method of the client.
    """

    @parameterized.expand([
        ["integer_value", 10, 10],
        ["float_value", 100.5, 100],
        ["string_integer", "10", 10],
        ["string_float", "100.5", 100],
    ])
    def test_timeout_values(self, name, timeout_value, expected_value):
        """
        Test that for the valid value of timeout,
        No exception is raised and the expected value is set.
        """
        config = {"timeout": timeout_value}
        _client = FreshdeskClient(config)

        # Verify timeout value is expected
        self.assertEqual(_client.timeout, expected_value)

    @parameterized.expand([
        ["integer_zero", 0],
        ["float_zero", 0.0],
        ["string_zero", "0"],
        ["string_float_zero", "0.0"],
        ["string_alphabate", "abc"],
    ])
    def test_invalid_value(self, name, timeout_value):
        """
        Test that for invalid value exception is raised.
        """
        config = {"timeout": timeout_value}
        with self.assertRaises(Exception) as e:
            _client = FreshdeskClient(config)

        # Verify that the exception message is expected.
        self.assertEqual(str(e.exception), "The entered timeout is invalid, it should be a valid none-zero integer.")


    def test_none_value(self):
        """
        Test if no timeout is not passed in the config, then set it to the default value.
        """
        config = {}
        _client = FreshdeskClient(config)

        # Verify that the default timeout value is set.
        self.assertEqual(_client.timeout, DEFAULT_TIMEOUT)
