from singer import get_logger, metrics, write_record

from tap_freshdesk.streams.abstracts import (
    FullTableStream,
    TokenPaginatedMixin,
)

LOGGER = get_logger()


class CsatSurveys(TokenPaginatedMixin, FullTableStream):
    """New-style Customer Satisfaction Survey (CSAT) stream.

    ``survey_responses`` is a child stream that is synced per survey.
    This is a FULL_TABLE stream — the API does not expose ``updated_at``
    on survey objects so incremental replication is not possible.

    Ref: https://developers.freshdesk.com/api/#csat_surveys
    """

    tap_stream_id = "csat_surveys"
    key_properties = ["id"]
    # No replication_keys: FullTableStream.replication_keys is already None
    children = ["survey_responses"]
    path = "customer-satisfaction/surveys"
    data_key = "data"

    def get_url_endpoint(self, parent_obj=None):
        """Return the API endpoint URL for this stream."""
        return f"{self.client.base_url}/{self.path}"

    def sync(self, state, transformer):
        """Full-table sync that also fans out to child streams per record.

        ``FullTableStream.sync`` does not call ``child_to_sync``, so we
        override here to drive ``survey_responses`` for every survey.
        """
        self.url_endpoint = self.get_url_endpoint()
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                transformed_record = transformer.transform(
                    record, self.schema, self.metadata
                )
                write_record(self.tap_stream_id, transformed_record)
                counter.increment()

                for child in self.child_to_sync:
                    child.sync(
                        state=state,
                        transformer=transformer,
                        parent_obj=record,
                    )

            return counter.value
