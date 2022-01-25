"""Test tap discovery mode and metadata/annotated-schema."""
import re

from tap_tester import menagerie, connections, runner

from base import FreshdeskBaseTest


class DiscoveryTest(FreshdeskBaseTest):
    """Test tap discovery mode and metadata/annotated-schema conforms to standards."""

    @staticmethod
    def name():
        return "tt_freshdesk_discovery"

    def test_run(self):
        """
        Verify that discover creates the appropriate catalog, schema, metadata, etc.

        • Verify number of actual streams discovered match expected
        • Verify the stream names discovered were what we expect
        • Verify stream names follow naming convention
          streams should only have lowercase alphas and underscores
        • verify there is only 1 top level breadcrumb
        • verify replication key(s)
        • verify primary key(s)
        • verify that if there is a replication key we are doing INCREMENTAL otherwise FULL
        • verify the actual replication matches our expected replication method
        • verify that primary, replication and foreign keys
          are given the inclusion of automatic (metadata and annotated schema).
        • verify that all other fields have inclusion of available (metadata and schema)
        """
        streams_to_test = self.expected_streams()

        conn_id = connections.ensure_connection(self)

        # found_catalogs = self.run_and_verify_sync(conn_id)
        sync_job_name = runner.run_sync_mode(self, conn_id)                
