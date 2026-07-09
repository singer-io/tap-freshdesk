"""Test tap discovery mode and metadata."""
from base import FreshdeskBaseTest
from tap_tester.base_suite_tests.discovery_test import DiscoveryTest
from tap_tester import menagerie


class FreshdeskDiscoveryTest(DiscoveryTest, FreshdeskBaseTest):
    """Test tap discovery mode and metadata conforms to standards."""

    @staticmethod
    def name():
        return "tap_tester_freshdesk_discovery_test"

    def streams_to_test(self):
        return self.expected_stream_names()

    def expected_parent_tap_stream_id(self, stream):
        """Helper to get expected parent stream ID from expected_metadata"""
        return self.expected_metadata().get(stream, {}).get(self.PARENT_TAP_STREAM_ID)

    def test_replication_metadata(self):
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):
                # gather expectations
                expected_replication_keys = self.expected_replication_keys(stream)
                expected_replication_method = self.expected_replication_method(stream)
                expected_parent = self.expected_parent_tap_stream_id(stream)

                # gather results
                catalog = [
                    catalog
                    for catalog in self.found_catalogs
                    if catalog["stream_name"] == stream
                ][0]
                metadata = menagerie.get_annotated_schema(
                    self.conn_id, catalog["stream_id"]
                )["metadata"]
                stream_properties = [
                    item for item in metadata if item.get("breadcrumb") == []
                ]
                stream_metadata = stream_properties[0].get("metadata", {})

                actual_replication_method = (
                    stream_properties[0]
                    .get("metadata", {})
                    .get(self.REPLICATION_METHOD, None)
                )
                actual_replication_keys = set(
                    stream_properties[0]
                    .get("metadata", {})
                    .get(self.REPLICATION_KEYS, [])
                )

                actual_parent = stream_metadata.get(self.PARENT_TAP_STREAM_ID)

                # verify the metadata key is in properties
                self.assertIn("metadata", stream_properties[0])

                # verify the replication keys metadata key is in metadata
                self.assertIn(self.REPLICATION_METHOD, stream_metadata)
                self.assertTrue(isinstance(actual_replication_method, str))

                # verify actual replication key(s) match expected
                with self.subTest(msg="validating replication keys"):
                    self.assertSetEqual(
                        expected_replication_keys,
                        actual_replication_keys,
                        logging=f"verify {expected_replication_keys} "
                        f"is saved in metadata as a valid-replication-key",
                    )

                # verify the actual replication matches our expected replication method
                with self.subTest(msg="validating replication method"):
                    self.assertEqual(
                        expected_replication_method,
                        actual_replication_method,
                        logging=f"verify the replication method is "
                        f"{expected_replication_method}",
                    )

                # verify that if there is a replication key we are doing INCREMENTAL otherwise FULL
                # If replication keys are not specified in metadata, skip this check
                with self.subTest(msg="validating expectations consistency"):
                    if expected_replication_keys:
                        self.assertEqual(
                            actual_replication_method,
                            self.INCREMENTAL,
                            logging=f"verify the forced replication method is "
                            f"{self.INCREMENTAL} since there is a "
                            f"replication-key",
                         )

                # "verify Parent stream id" or "verify that Parent stream id is correct"
                with self.subTest(msg="validating parent-tap-stream-id"):
                    if expected_parent:
                        self.assertIn(
                            self.PARENT_TAP_STREAM_ID,
                            stream_metadata,
                            msg=f"{self.PARENT_TAP_STREAM_ID} missing for stream {stream}",
                        )
                        self.assertEqual(
                            actual_parent,
                            expected_parent,
                            msg=f"Incorrect parent stream id for {stream}",
                        )
                    else:
                        self.assertNotIn(
                            self.PARENT_TAP_STREAM_ID,
                            stream_metadata,
                            msg=f"{self.PARENT_TAP_STREAM_ID} should not exist for stream {stream}",
                        )
