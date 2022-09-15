import unittest
from parameterized import parameterized
import tap_freshdesk.client as client_

PAGE_SIZE_INT = 50
PAGE_SIZE_STR_INT = "50"
PAGE_SIZE_STR_FLOAT = "50.0"
PAGE_SIZE_FLOAT = 50.0
PAGE_SIZE_ZERO = 0
PAGE_SIZE_STR_ZERO = "0"
PAGE_SIZE_INVALID_STRING = "abc"


class TestPageSizeValue(unittest.TestCase):

    @parameterized.expand([
        [PAGE_SIZE_INT, PAGE_SIZE_INT],
        [PAGE_SIZE_STR_INT, PAGE_SIZE_INT],
        [PAGE_SIZE_STR_FLOAT, PAGE_SIZE_INT],
        [PAGE_SIZE_FLOAT, PAGE_SIZE_INT],
    ])
    def test_page_size_for_valid_values(self, page_size_value, expected_value):
        """
        Test the various values of page_size:
            - For string, integer, float type of values, converts to float
            - For null string, zero(string), zero(integer), takes default integer value
        """
        config = {'domain': 'abc', "page_size": page_size_value}
        client = client_.FreshdeskClient(config)

        # Verify the page_size is the same as the expected value
        self.assertEqual(client.page_size, expected_value)

    @parameterized.expand([
        [PAGE_SIZE_INVALID_STRING],
        [PAGE_SIZE_STR_ZERO],
        [PAGE_SIZE_ZERO],
    ])
    def test_page_size_for_invalid_values(self, page_size_value):
        """
        Test the various values of page_size:
            - For string, integer, float type of values, converts to float
            - For null string, zero(string), zero(integer), takes default integer value
        """

        config = {'domain': 'abc', "page_size": page_size_value}
        # Verify the tap raises Exception
        with self.assertRaises(Exception) as e:
            client_.FreshdeskClient(config)

        # Verify the tap raises an error with expected error message
        self.assertEqual(str(e.exception), "The entered page size is invalid, it should be a valid integer.")

    def test_without_page_size(self):
        """
        Test if no page size is given in config, default page_size will be considered.
        """
        config = {'domain': 'abc'}
        client = client_.FreshdeskClient(config)

        # Verify the page_size is the same as the default value
        self.assertEqual(client.page_size, client_.DEFAULT_PAGE_SIZE)
