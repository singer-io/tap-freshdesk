from typing import Any, Dict

from singer import (
    Transformer,
    get_logger,
    metrics,
    write_record,
    write_bookmark,
    get_currently_syncing,
    set_currently_syncing,
    write_state,
)

from tap_freshdesk.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class Tickets(IncrementalStream):
    tap_stream_id = "tickets"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    children = ["conversations", "satisfaction_ratings", "time_entries"]
    path = "tickets"

    def write_bookmark(self, state: dict, key: Any = None, value: Any = None) -> Dict:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        return write_bookmark(
            state, self.tap_stream_id, key or self.replication_keys[0], value
        )

    def update_currently_syncing(state: Dict, stream_name: str) -> None:
        if not stream_name and get_currently_syncing(state):
            del state["currently_syncing"]
        else:
            set_currently_syncing(state, stream_name)
        write_state(state)

    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """Implementation for `type: Incremental` stream."""
        current_max_bookmark_date = bookmark_date = self.get_bookmark(state)
        self.url_endpoint = self.get_url_endpoint(parent_obj)

        # Set initial parameters for API call
        self.params.update(
            {
                "order_by": self.replication_keys[0],
                "order_type": "asc",
                "include": "requester,company,stats",
            }
        )

        with metrics.record_counter(self.tap_stream_id) as counter:
            filter_values = [{}, {"filter": "spam"}, {"filter": "deleted"}]
            for value in filter_values:
                if value:
                    key = self.tap_stream_id + "_" + value["filter"]
                else:
                    key = self.tap_stream_id  # Default key when value is None or empty
                updated_since = self.get_bookmark(state, key=None)
                self.params.update({"updated_since": updated_since})
                self.params.update(**value)
                for record in self.get_records(state):
                    if "custom_fields" in record:
                        record["custom_fields"] = self.modify_object(
                            record["custom_fields"], force_str=True
                        )
                    transformed_record = transformer.transform(
                        record, self.schema, self.metadata
                    )

                    record_timestamp = transformed_record[self.replication_keys[0]]
                    if record_timestamp >= bookmark_date:
                        write_record(self.tap_stream_id, transformed_record)
                        current_max_bookmark_date = max(
                            current_max_bookmark_date, record_timestamp
                        )
                        counter.increment()

                        for child in self.child_to_sync:
                            child.sync(
                                state=state, transformer=transformer, parent_obj=record
                            )

                state = self.write_bookmark(state, key, value=current_max_bookmark_date)
            return counter.value
