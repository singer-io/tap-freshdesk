from abc import ABC, abstractmethod
from typing import Any, Dict, Tuple, List

from singer import (
    Transformer,
    get_bookmark,
    get_logger,
    metrics,
    write_bookmark,
    write_record,
    write_schema,
)
from singer.utils import strftime, strptime_to_utc

LOGGER = get_logger()


class BaseStream(ABC):
    """
    A Base Class providing structure and boilerplate for generic streams
    and required attributes for any kind of stream
    ~~~
    Provides:
     - Basic Attributes (stream_name,replication_method,key_properties)
     - Helper methods for catalog generation
     - `sync` and `get_records` method for performing sync
    """

    url_endpoint = ""
    path = ""
    page_size = 100
    next_page_key = ""
    params = {}
    headers = {'Accept': 'application/json'}

    @property
    @abstractmethod
    def tap_stream_id(self) -> str:
        """Unique identifier for the stream.

        This is allowed to be different from the name of the stream, in
        order to allow for sources that have duplicate stream names.
        """

    @property
    @abstractmethod
    def replication_method(self) -> str:
        """Defines the sync mode of a stream."""

    @property
    @abstractmethod
    def replication_keys(self) -> str:
        """Defines the replication key for incremental sync mode of a
        stream."""

    @property
    @abstractmethod
    def forced_replication_method(self) -> str:
        """Defines the sync mode of a stream."""

    @property
    @abstractmethod
    def key_properties(self) -> Tuple[str, str]:
        """List of key properties for stream."""

    @property
    def selected_by_default(self) -> bool:
        """Indicates if a node in the schema should be replicated, if a user
        has not expressed any opinion on whether or not to replicate it."""
        return False

    @abstractmethod
    def sync(
        self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer
    ) -> Dict:
        """
        Performs a replication sync for the stream.
        ~~~
        Args:
         - state (dict): represents the state file for the tap.
         - schema (dict): Schema of the stream
         - transformer (object): A Object of the singer.transformer class.

        Returns:
         - bool: The return value. True for success, False otherwise.

        Docs:
         - https://github.com/singer-io/getting-started/blob/master/docs/SYNC_MODE.md
        """

    def __init__(self, client=None) -> None:
        self.client = client

    def get_records(self, state: Dict) -> List:
        """Interacts with api client interaction and pagination."""
        extraction_url = self.url_endpoint
        page_count = 1
        if self.page_size:
            self.params.update({"per_page": self.page_size})
            self.params.update({"page": 1})

        while True:
            LOGGER.info("Calling Page %s", page_count)
            response = self.client.get(
                extraction_url, self.params, self.headers, self.path
            )
            LOGGER.info("Response......................: %s", response)
            raw_records = response
            # next_page = response.get(self.next_page_key)

            if not raw_records:
                LOGGER.warning("No records found on Page %s", page_count)
                break
            # self.params[self.next_page_key] = next_page
            # page_count += 1
            yield from raw_records

            if len(raw_records) == self.page_size:
                page_count += 1
            else:
                break
            # if not next_page:
            #     break

    def write_schema(self, schema, stream_name):
        """
        Write a schema message.
        """
        try:
            write_schema(stream_name, schema, self.key_properties)
        except OSError as err:
            LOGGER.error("OS Error while writing schema for: {}".format(stream_name))
            raise err


class IncrementalStream(BaseStream):
    """Base Class for Incremental Stream."""

    replication_method = "INCREMENTAL"
    forced_replication_method = "INCREMENTAL"
    config_start_key = "start_date"

    def get_bookmark(self, state: dict, key: Any = None) -> int:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        return get_bookmark(
            state,
            self.tap_stream_id,
            key or self.replication_keys[0],
            self.client.config.get(self.config_start_key, False),
        )

    def write_bookmark(self, state: dict, key: Any = None, value: Any = None) -> Dict:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        return write_bookmark(
            state, self.tap_stream_id, key or self.replication_keys[0], value
        )

    def sync(
        self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer
    ) -> Dict:
        bookmark_date = self.get_bookmark(state)
        current_max_bookmark_date = bookmark_date_to_utc = strptime_to_utc(
            bookmark_date
        )
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records(state):
                transformed_record = transformer.transform(
                    record, schema, stream_metadata
                )
                try:
                    record_timestamp = strptime_to_utc(
                        transformed_record[self.replication_keys[0]]
                    )
                except KeyError as _:
                    LOGGER.error(
                        "Unable to process Record, Exception occurred: %s for stream %s",
                        _,
                        self.__class__,
                    )
                    continue
                if record_timestamp >= bookmark_date_to_utc:
                    write_record(self.tap_stream_id, transformed_record)
                    current_max_bookmark_date = max(
                        current_max_bookmark_date, record_timestamp
                    )
                    counter.increment()

            state = self.write_bookmark(
                state, value=strftime(current_max_bookmark_date)
            )
            return counter.value


class FullTableStream(BaseStream):
    """Base Class for Incremental Stream."""

    replication_method = "FULL_TABLE"
    forced_replication_method = "FULL_TABLE"
    valid_replication_keys = None
    replication_keys = None

    total_records = 0

    def sync(
        self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer
    ) -> Dict:
        """Abstract implementation for `type: Fulltable` stream."""
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                transformed_record = transformer.transform(
                    record, schema, stream_metadata
                )
                write_record(self.tap_stream_id, transformed_record)
                counter.increment()
            return counter.value
